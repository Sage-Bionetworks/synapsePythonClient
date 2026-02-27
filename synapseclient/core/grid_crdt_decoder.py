"""
Decoder for json-joy indexed binary CRDT snapshots.

This module provides a minimal Python implementation of the json-joy CRDT
snapshot decoder, sufficient to extract grid data and per-row validation
results from a Synapse grid session.

The decoder handles:
- CBOR outer layer decoding
- Clock table decoding for session ID mapping
- Node type decoding: CON, VAL, OBJ, VEC, ARR
- Tombstone filtering in ARR nodes (deleted rows)

Reference implementation:
    json-joy/lib/json-crdt/codec/indexed/binary/Decoder.js

This does NOT implement full CRDT semantics (no patch application, no
conflict resolution). It is a read-only snapshot decoder.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import cbor2

from synapseclient.models.grid_query import GridRow, GridRowValidation, GridSnapshot


def _cbor_item_byte_length(data: bytes, offset: int = 0) -> int:
    """Compute the exact byte length of one CBOR item starting at offset.

    This allows us to slice exactly one CBOR item from a buffer containing
    multiple items, then decode it with cbor2.loads() without consuming
    trailing data.

    Arguments:
        data: The buffer containing CBOR data.
        offset: Starting position in the buffer.

    Returns:
        The number of bytes that the first CBOR item occupies.
    """
    if offset >= len(data):
        raise ValueError("No CBOR data at offset")

    initial_byte = data[offset]
    major_type = initial_byte >> 5
    additional_info = initial_byte & 0x1F
    pos = offset + 1

    # Decode the argument (length/value) from additional info
    if additional_info < 24:
        argument = additional_info
    elif additional_info == 24:
        argument = data[pos]
        pos += 1
    elif additional_info == 25:
        argument = int.from_bytes(data[pos : pos + 2], "big")
        pos += 2
    elif additional_info == 26:
        argument = int.from_bytes(data[pos : pos + 4], "big")
        pos += 4
    elif additional_info == 27:
        argument = int.from_bytes(data[pos : pos + 8], "big")
        pos += 8
    elif additional_info == 31:
        # Indefinite length - scan for break code (0xFF)
        if major_type in (2, 3):  # byte/text string chunks
            while data[pos] != 0xFF:
                chunk_len = _cbor_item_byte_length(data, pos)
                pos += chunk_len
            pos += 1  # skip 0xFF break
            return pos - offset
        elif major_type in (4, 5):  # array/map
            while data[pos] != 0xFF:
                pos += _cbor_item_byte_length(data, pos)
                if major_type == 5:
                    pos += _cbor_item_byte_length(data, pos)
            pos += 1  # skip 0xFF break
            return pos - offset
        else:
            # Simple break or other - just the initial byte
            return 1
    else:
        # Reserved (28-30) - treat as 0 argument
        argument = 0

    # Major types 0, 1: unsigned/negative int - no payload beyond argument
    if major_type in (0, 1):
        return pos - offset

    # Major types 2, 3: byte/text string - argument is the string length
    if major_type in (2, 3):
        return (pos - offset) + argument

    # Major type 4: array - argument is number of items
    if major_type == 4:
        for _ in range(argument):
            pos += _cbor_item_byte_length(data, pos)
        return pos - offset

    # Major type 5: map - argument is number of key-value pairs
    if major_type == 5:
        for _ in range(argument):
            pos += _cbor_item_byte_length(data, pos)  # key
            pos += _cbor_item_byte_length(data, pos)  # value
        return pos - offset

    # Major type 6: tag - argument is tag number, followed by one item
    if major_type == 6:
        pos += _cbor_item_byte_length(data, pos)
        return pos - offset

    # Major type 7: simple values and floats
    if major_type == 7:
        return pos - offset

    return pos - offset


# CRDT major type constants (upper 3 bits of node type octet)
CRDT_CON = 0  # Constant (immutable value)
CRDT_VAL = 1  # Value (LWW register)
CRDT_OBJ = 2  # Object (LWW map)
CRDT_VEC = 3  # Vector (fixed-size LWW array)
CRDT_STR = 4  # String (RGA)
CRDT_BIN = 5  # Binary (RGA)
CRDT_ARR = 6  # Array (RGA)


@dataclass
class Timestamp:
    """A logical CRDT timestamp."""

    sid: int  # Session ID
    time: int  # Logical clock time


@dataclass
class ClockEntry:
    """An entry in the clock table."""

    sid: int
    time: int


@dataclass
class ArrChunk:
    """A chunk in an RGA array, may be a tombstone."""

    id: Timestamp
    length: int
    deleted: bool
    data: Optional[List[Timestamp]] = None


@dataclass
class CrdtNode:
    """A decoded CRDT node."""

    id: Timestamp
    node_type: int
    value: Any = None
    children: Optional[Dict[str, Timestamp]] = None
    elements: Optional[List[Optional[Timestamp]]] = None
    chunks: Optional[List[ArrChunk]] = None


class BinaryReader:
    """Reader for binary data with position tracking."""

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    def u8(self) -> int:
        """Read unsigned 8-bit integer."""
        val = self.data[self.pos]
        self.pos += 1
        return val

    def vu57(self) -> int:
        """Read variable-length unsigned integer (up to 57 bits).

        Each byte contributes 7 data bits. MSB indicates continuation.
        """
        result = 0
        shift = 0
        while True:
            byte = self.u8()
            result |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        return result

    def b1vu56(self) -> Tuple[int, int]:
        """Read 1 flag bit + variable-length 56-bit integer.

        Returns (flag, value) where flag is 0 or 1 (MSB of first byte).
        """
        byte = self.u8()
        flag = 1 if (byte & 0x80) else 0
        # Lower 6 bits of first byte
        result = byte & 0x3F
        if not (byte & 0x40):
            return (flag, result)
        # Continue reading with 7-bit encoding
        shift = 6
        while True:
            byte = self.u8()
            result |= (byte & 0x7F) << shift
            if not (byte & 0x80):
                break
            shift += 7
        return (flag, result)

    def id(self) -> Tuple[int, int]:
        """Read a compact session-index + time-diff pair.

        Returns (session_index, time_diff).
        """
        byte = self.data[self.pos]
        if byte <= 0x7F:
            self.pos += 1
            return (byte >> 4, byte & 0x0F)
        # Fall back to variable-length encoding
        flag_and_val = self.b1vu56()
        time_diff = self.vu57()
        return (flag_and_val[1], time_diff)

    def reset(self, data: bytes) -> None:
        """Reset reader to new data."""
        self.data = data
        self.pos = 0

    @property
    def remaining(self) -> int:
        """Bytes remaining to read."""
        return len(self.data) - self.pos


class GridSnapshotDecoder:
    """Decoder for json-joy indexed binary CRDT snapshots.

    Decodes the CRDT model to extract grid data:
    - columnNames (vec of con strings)
    - rows (arr of obj with data vec + metadata obj)
    - rowValidation (con containing ValidationResults)

    Handles tombstones in ARR nodes (deleted rows) by filtering
    them during view generation, matching json-joy's ArrNode.view().
    """

    def __init__(self):
        self.clock_table: List[ClockEntry] = []
        self.nodes: Dict[str, CrdtNode] = {}  # Keyed by "sid_time"
        self.reader = BinaryReader(b"")
        self.root_id: Optional[Timestamp] = None

    def decode(self, cbor_data: bytes) -> GridSnapshot:
        """Decode CBOR-encoded CRDT snapshot to GridSnapshot.

        Arguments:
            cbor_data: Raw CBOR bytes of the snapshot.

        Returns:
            GridSnapshot with column names, row data, and validation.
        """
        # 1. CBOR decode → IndexedFields dict
        fields = cbor2.loads(cbor_data)

        # 2. Decode clock table
        self._decode_clock_table(fields[b"c"] if b"c" in fields else fields["c"])

        # 3. Decode root if present
        root_key = b"r" if b"r" in fields else "r"
        if root_key in fields:
            self.reader.reset(fields[root_key])
            self.root_id = self._read_ts()

        # 4. Decode all field nodes
        for key, value in fields.items():
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            if key_str in ("c", "r"):
                continue
            # Parse field name: "${relativeSidBase36}_${timeBase36}"
            node_id = self._parse_field_name(key_str)
            self.reader.reset(value)
            node = self._decode_node(node_id)
            node_key = f"{node_id.sid}_{node_id.time}"
            self.nodes[node_key] = node

        # 5. Build GridSnapshot from the CRDT tree
        return self._build_snapshot()

    def _decode_clock_table(self, data: bytes) -> None:
        """Decode the clock table from binary data."""
        self.reader.reset(data)
        length = self.reader.vu57()
        self.clock_table = []
        for _ in range(length):
            sid = self.reader.vu57()
            time = self.reader.vu57()
            self.clock_table.append(ClockEntry(sid=sid, time=time))

    def _read_ts(self) -> Timestamp:
        """Read a timestamp from the current reader position."""
        session_index, time_diff = self.reader.id()
        if session_index < len(self.clock_table):
            entry = self.clock_table[session_index]
            return Timestamp(sid=entry.sid, time=time_diff)
        return Timestamp(sid=session_index, time=time_diff)

    def _parse_field_name(self, field_name: str) -> Timestamp:
        """Parse a field name like '2_10' (base-36) to a Timestamp."""
        underscore_idx = field_name.index("_")
        relative_sid = int(field_name[:underscore_idx], 36)
        time = int(field_name[underscore_idx + 1 :], 36)
        if relative_sid < len(self.clock_table):
            entry = self.clock_table[relative_sid]
            return Timestamp(sid=entry.sid, time=time)
        return Timestamp(sid=relative_sid, time=time)

    def _read_cbor_value(self) -> Any:
        """Read exactly one CBOR value from the current reader position.

        Computes the exact byte length of the CBOR item before decoding,
        then advances the reader position by exactly that amount.

        Returns:
            The decoded CBOR value, or None if no data remains.
        """
        remaining = self.reader.data[self.reader.pos :]
        if not remaining:
            return None
        try:
            item_length = _cbor_item_byte_length(remaining)
            item_bytes = remaining[:item_length]
            value = cbor2.loads(item_bytes)
            self.reader.pos += item_length
            return value
        except Exception:
            return None

    def _decode_node(self, node_id: Timestamp) -> CrdtNode:
        """Decode a single CRDT node from the current reader."""
        octet = self.reader.u8()
        major = octet >> 5  # Upper 3 bits
        minor = octet & 0x1F  # Lower 5 bits

        if major == CRDT_CON:
            return self._decode_con(node_id, minor)
        elif major == CRDT_VAL:
            return self._decode_val(node_id)
        elif major == CRDT_OBJ:
            return self._decode_obj(node_id, minor)
        elif major == CRDT_VEC:
            return self._decode_vec(node_id, minor)
        elif major == CRDT_STR:
            return self._decode_str(node_id, minor)
        elif major == CRDT_BIN:
            return self._decode_bin(node_id, minor)
        elif major == CRDT_ARR:
            return self._decode_arr(node_id, minor)
        else:
            return CrdtNode(id=node_id, node_type=major)

    def _decode_con(self, node_id: Timestamp, length: int) -> CrdtNode:
        """Decode a CON (constant) node.

        If length == 0: value is a CBOR-encoded constant.
        If length > 0: value is a Timestamp reference.
        """
        if length == 0:
            # Read exactly one CBOR value using streaming decoder
            # to track exact byte consumption
            value = self._read_cbor_value()
        else:
            # Timestamp reference
            value = self._read_ts()

        return CrdtNode(id=node_id, node_type=CRDT_CON, value=value)

    def _decode_val(self, node_id: Timestamp) -> CrdtNode:
        """Decode a VAL (value/register) node - pointer to another node."""
        child_ts = self._read_ts()
        return CrdtNode(id=node_id, node_type=CRDT_VAL, value=child_ts)

    def _decode_obj(self, node_id: Timestamp, length: int) -> CrdtNode:
        """Decode an OBJ (object/map) node."""
        children: Dict[str, Timestamp] = {}
        for _ in range(length):
            # Read key using streaming CBOR decoder for exact byte tracking
            key = self._read_cbor_value()
            if key is None:
                break
            # Read value timestamp
            val_ts = self._read_ts()
            children[str(key)] = val_ts
        return CrdtNode(id=node_id, node_type=CRDT_OBJ, children=children)

    def _decode_vec(self, node_id: Timestamp, length: int) -> CrdtNode:
        """Decode a VEC (vector/fixed-array) node."""
        elements: List[Optional[Timestamp]] = []
        for _ in range(length):
            # Check for null/empty slot
            if self.reader.remaining > 0:
                el_ts = self._read_ts()
                elements.append(el_ts)
            else:
                elements.append(None)
        return CrdtNode(id=node_id, node_type=CRDT_VEC, elements=elements)

    def _decode_arr(self, node_id: Timestamp, length: int) -> CrdtNode:
        """Decode an ARR (RGA array) node."""
        chunks: List[ArrChunk] = []
        for _ in range(length):
            chunk = self._decode_arr_chunk()
            chunks.append(chunk)
        return CrdtNode(id=node_id, node_type=CRDT_ARR, chunks=chunks)

    def _decode_arr_chunk(self) -> ArrChunk:
        """Decode a single ARR chunk (may be a tombstone)."""
        chunk_id = self._read_ts()
        deleted_flag, chunk_length = self.reader.b1vu56()
        if deleted_flag:
            return ArrChunk(id=chunk_id, length=chunk_length, deleted=True)
        else:
            data = []
            for _ in range(chunk_length):
                data.append(self._read_ts())
            return ArrChunk(
                id=chunk_id,
                length=chunk_length,
                deleted=False,
                data=data,
            )

    def _decode_str(self, node_id: Timestamp, length: int) -> CrdtNode:
        """Decode a STR (RGA string) node - treated similarly to ARR."""
        # For our purposes, we skip string internals
        return CrdtNode(id=node_id, node_type=CRDT_STR)

    def _decode_bin(self, node_id: Timestamp, length: int) -> CrdtNode:
        """Decode a BIN (RGA binary) node."""
        return CrdtNode(id=node_id, node_type=CRDT_BIN)

    # --- Snapshot to GridSnapshot conversion ---

    def _resolve_node(self, ts: Timestamp) -> Optional[CrdtNode]:
        """Resolve a Timestamp to its CrdtNode."""
        key = f"{ts.sid}_{ts.time}"
        return self.nodes.get(key)

    def _resolve_value(self, ts: Timestamp) -> Any:
        """Resolve a Timestamp reference to its final value."""
        node = self._resolve_node(ts)
        if node is None:
            return None
        if node.node_type == CRDT_CON:
            return node.value
        if node.node_type == CRDT_VAL:
            if isinstance(node.value, Timestamp):
                return self._resolve_value(node.value)
            return node.value
        return node

    def _build_snapshot(self) -> GridSnapshot:
        """Build a GridSnapshot from the decoded CRDT tree."""
        if self.root_id is None:
            return GridSnapshot()

        root_node = self._resolve_node(self.root_id)
        if root_node is None or root_node.node_type == CRDT_VAL:
            # Root is a VAL pointing to the actual document object
            if root_node and isinstance(root_node.value, Timestamp):
                root_node = self._resolve_node(root_node.value)
        if root_node is None:
            return GridSnapshot()

        # Extract column names
        column_names = self._extract_column_names(root_node)

        # Extract rows with validation
        rows = self._extract_rows(root_node, column_names)

        return GridSnapshot(column_names=column_names, rows=rows)

    def _extract_column_names(self, doc_node: CrdtNode) -> List[str]:
        """Extract column names from the document's columnNames vec."""
        if doc_node.children is None:
            return []

        col_names_ts = doc_node.children.get("columnNames")
        if col_names_ts is None:
            return []

        col_names_node = self._resolve_node(col_names_ts)
        if col_names_node is None:
            return []

        # VEC of CON strings
        if col_names_node.node_type == CRDT_VEC and col_names_node.elements:
            names = []
            for el_ts in col_names_node.elements:
                if el_ts is not None:
                    val = self._resolve_value(el_ts)
                    names.append(str(val) if val is not None else "")
                else:
                    names.append("")
            return names

        return []

    def _extract_rows(
        self, doc_node: CrdtNode, column_names: List[str]
    ) -> List[GridRow]:
        """Extract rows from the document's rows arr."""
        if doc_node.children is None:
            return []

        rows_ts = doc_node.children.get("rows")
        if rows_ts is None:
            return []

        rows_node = self._resolve_node(rows_ts)
        if rows_node is None:
            return []

        # ARR of row objects - filter tombstones
        if rows_node.node_type != CRDT_ARR or not rows_node.chunks:
            return []

        grid_rows: List[GridRow] = []
        for chunk in rows_node.chunks:
            if chunk.deleted:
                continue  # Skip tombstones
            if chunk.data:
                for row_ts in chunk.data:
                    row = self._extract_single_row(row_ts, column_names)
                    if row is not None:
                        grid_rows.append(row)

        return grid_rows

    def _extract_single_row(
        self, row_ts: Timestamp, column_names: List[str]
    ) -> Optional[GridRow]:
        """Extract a single row's data and validation."""
        row_node = self._resolve_node(row_ts)
        if row_node is None:
            return None

        # Row is a VAL pointing to an OBJ
        if row_node.node_type == CRDT_VAL and isinstance(row_node.value, Timestamp):
            row_node = self._resolve_node(row_node.value)
            if row_node is None:
                return None

        if row_node.node_type != CRDT_OBJ or not row_node.children:
            return None

        # Extract row data from 'data' vec
        row_data: Dict[str, Any] = {}
        data_ts = row_node.children.get("data")
        if data_ts:
            data_node = self._resolve_node(data_ts)
            if data_node and data_node.node_type == CRDT_VEC and data_node.elements:
                for i, el_ts in enumerate(data_node.elements):
                    col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                    if el_ts is not None:
                        row_data[col_name] = self._resolve_value(el_ts)
                    else:
                        row_data[col_name] = None

        # Extract validation from 'metadata.rowValidation'
        validation = self._extract_row_validation(row_node)

        row_id = f"{row_ts.sid}.{row_ts.time}"

        return GridRow(
            row_id=row_id,
            data=row_data,
            validation=validation,
        )

    def _extract_row_validation(
        self, row_node: CrdtNode
    ) -> Optional[GridRowValidation]:
        """Extract validation results from a row's metadata."""
        if not row_node.children:
            return None

        metadata_ts = row_node.children.get("metadata")
        if metadata_ts is None:
            return None

        metadata_node = self._resolve_node(metadata_ts)
        if (
            metadata_node is None
            or metadata_node.node_type != CRDT_OBJ
            or not metadata_node.children
        ):
            return None

        validation_ts = metadata_node.children.get("rowValidation")
        if validation_ts is None:
            return None

        validation_value = self._resolve_value(validation_ts)
        if validation_value is None:
            return None

        # validation_value should be a dict-like object from CBOR
        if isinstance(validation_value, dict):
            return GridRowValidation(
                is_valid=validation_value.get("isValid"),
                validation_error_message=validation_value.get("validationErrorMessage"),
                all_validation_messages=validation_value.get("allValidationMessages"),
            )

        return None
