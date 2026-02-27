"""
Read-only WebSocket client for Synapse grid sessions.

Connects to a grid session via presigned WebSocket URL, receives the initial
grid state (snapshot or patches), decodes the CRDT model, and extracts grid
data including per-row validation results.

Protocol: JSON-Rx (https://jsonjoy.com/specs/json-rx)
Messages are JSON arrays with type code as first element:
  [1, reqId, method, payload?]  - Request (complete)
  [4, subId, payload]           - Response (data)
  [5, subId, payload?]          - Response (complete)
  [8, method, payload?]         - Notification
"""

import asyncio
import json
import logging
from typing import Any, List, Optional

import httpx
import websockets

from synapseclient.core.grid_crdt_decoder import GridSnapshotDecoder
from synapseclient.models.grid_query import GridSnapshot

logger = logging.getLogger(__name__)

# JSON-Rx message type codes
JSONRX_REQUEST_COMPLETE = 1
JSONRX_RESPONSE_DATA = 4
JSONRX_RESPONSE_COMPLETE = 5
JSONRX_NOTIFICATION = 8


class GridWebSocketClient:
    """Read-only WebSocket client for grid sessions.

    Connects to a grid session via presigned WebSocket URL,
    receives the initial snapshot or patches, and extracts grid data
    including per-row validation results.

    This client is designed for one-shot reads: connect, receive state,
    extract data, disconnect. It does not participate in collaborative
    editing or send patches.
    """

    def __init__(self, connect_timeout: float = 30.0):
        """
        Arguments:
            connect_timeout: Timeout in seconds for the WebSocket connection
                and initial data reception.
        """
        self.connect_timeout = connect_timeout

    async def get_snapshot(
        self,
        presigned_url: str,
        replica_id: int,
    ) -> GridSnapshot:
        """Connect to a grid session, receive its state, and return a snapshot.

        Arguments:
            presigned_url: The presigned WebSocket URL from
                ``POST /grid/session/{sessionId}/presigned/url``.
            replica_id: The replica ID for this connection.

        Returns:
            GridSnapshot with column names, row data, and per-row validation.
        """
        snapshot_url: Optional[str] = None
        patches: List[Any] = []
        sync_complete = False

        async with websockets.connect(
            presigned_url,
            close_timeout=10,
            open_timeout=self.connect_timeout,
        ) as ws:
            try:
                async with asyncio.timeout(self.connect_timeout):
                    # Wait for initial messages until sync complete
                    async for raw_message in ws:
                        message = self._parse_message(raw_message)
                        if message is None:
                            continue

                        msg_type = message[0]

                        if msg_type == JSONRX_NOTIFICATION:
                            method = message[1] if len(message) > 1 else None
                            if method == "connected":
                                logger.debug("Grid WebSocket connected")
                                # Send clock sync with empty clock
                                sync_msg = json.dumps(
                                    [JSONRX_REQUEST_COMPLETE, 1, "synchronize-clock", []]
                                )
                                await ws.send(sync_msg)
                            elif method == "ping":
                                pass  # Ignore keep-alive pings

                        elif msg_type == JSONRX_RESPONSE_DATA:
                            payload = message[2] if len(message) > 2 else None
                            if isinstance(payload, dict):
                                payload_type = payload.get("type")
                                if payload_type == "snapshot":
                                    snapshot_url = payload.get("body")
                                    logger.debug(
                                        "Received snapshot URL"
                                    )
                                elif payload_type == "patch":
                                    patches.append(payload.get("body"))
                            elif payload is not None:
                                # Raw patch data
                                patches.append(payload)

                        elif msg_type == JSONRX_RESPONSE_COMPLETE:
                            logger.debug("Grid sync complete")
                            sync_complete = True
                            break

                        # Safety: don't loop forever
                        if len(patches) > 10000:
                            logger.warning(
                                "Received >10000 patches without sync "
                                "complete, stopping"
                            )
                            break

            except TimeoutError:
                logger.warning(
                    "Grid WebSocket timed out after %.1fs waiting for "
                    "sync complete signal",
                    self.connect_timeout,
                )
            except websockets.exceptions.ConnectionClosed:
                logger.debug("WebSocket connection closed during sync")

        # Process the received data
        if snapshot_url:
            return await self._process_snapshot(snapshot_url)
        elif patches:
            logger.warning(
                "Received patches but no snapshot URL. "
                "Patch-based initialization is not yet supported. "
                "Returning empty snapshot."
            )
            return GridSnapshot()
        else:
            logger.warning(
                "No snapshot or patches received from grid session"
            )
            return GridSnapshot()

    async def _process_snapshot(self, snapshot_url: str) -> GridSnapshot:
        """Fetch and decode a CRDT snapshot from its S3 URL.

        Arguments:
            snapshot_url: Presigned S3 URL containing the CBOR snapshot.

        Returns:
            Decoded GridSnapshot.
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(snapshot_url, timeout=30.0)
            response.raise_for_status()
            cbor_data = response.content

        logger.debug(
            "Fetched snapshot: %d bytes", len(cbor_data)
        )

        decoder = GridSnapshotDecoder()
        return decoder.decode(cbor_data)

    def _parse_message(self, raw: Any) -> Optional[list]:
        """Parse a WebSocket message as JSON-Rx.

        Arguments:
            raw: Raw WebSocket message (str or bytes).

        Returns:
            Parsed JSON array, or None if parsing fails.
        """
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.debug("Failed to parse WebSocket message: %s", e)
            return None
