"""Common utility functions used during upload."""

import math
import re
from io import BytesIO, StringIO
from typing import Any, Dict, Optional, TypeVar, Union

DATA_FRAME_TYPE = TypeVar("pd.DataFrame")


def get_partial_dataframe_chunk(
    df: DATA_FRAME_TYPE,
    part_number: int,
    part_size: int,
    byte_offset: int,
    total_size_of_chunks_being_uploaded: int,
    line_start: int,
    line_end: int,
    bytes_to_prepend: Optional[bytes] = None,
    to_csv_kwargs: Optional[Dict[str, Any]] = None,
) -> bytes:
    """Read the nth chunk from the file assuming that we are not going to be reading
    or uploading the entire file. This function allows us to read a portion of the file
    and upload it to Synapse.

    Arguments:
        part_number: The part number.
        part_size: The maximum size of the part to read for the upload process.
        byte_offset: The byte offset for the file that has already been read and
            and uploaded. This offset is used to calculate the total offset to read
            the next chunk.
        path_to_file_to_split: The path to the file that we are reading off disk and
            uploading portions of to Synapse.
        total_size_of_chunks_being_uploaded: The total size of the chunks that are being
            uploaded. This is used to calculate the maximum number of bytes to read
            from the file, accounting for the last chunk that may be smaller than the
            chunk size.
        line_start: The starting line number to read from the dataframe for the entire chunk.
        line_end: The ending line number to read from the dataframe for the entire chunk.
        bytes_to_prepend: Bytes to prepend to the first chunk.
        to_csv_kwargs: Additional arguments to pass to the `to_csv` pandas method.

    Returns:
        bytes: The bytes that are read from the dataframe for the given `part_size`,
            `byte_offset` and `part_number`.
    """
    header_bytes = None
    if bytes_to_prepend and part_number == 1:
        header_bytes = bytes_to_prepend
    buffer = BytesIO()

    total_offset = byte_offset + ((part_number - 1) * part_size)

    max_bytes_to_read = min(
        (total_size_of_chunks_being_uploaded - ((part_number - 1) * part_size)),
        part_size,
    )
    # TODO: This is an area for optimization. It is possible to avoid writing the entire
    # dataframe to a buffer and then reading the buffer to get the bytes. Instead, we
    # might be able to do something like keeping markers at each 100 row increment how
    # many bytes exist in the next 100 rows. We may be able to then skip over each
    # 100 row increment until we reach the row that we need to start reading from.
    # Ticket: SYNPY-1573
    for start in range(0, len(df), 100):
        offset_start = start + line_start
        end = min(offset_start + 100, line_end)
        df.iloc[offset_start:end].to_csv(
            buffer,
            mode="a",
            header=False,
            index=False,
            float_format="%.12g",
            **(to_csv_kwargs or {}),
        )
        number_of_bytes_in_buffer = buffer.tell()
        # Drop data from the front of the buffer until total_offset is 0
        if total_offset > 0 and total_offset >= number_of_bytes_in_buffer:
            buffer.seek(0)
            buffer.truncate(0)
            total_offset -= number_of_bytes_in_buffer
            continue
        elif total_offset > 0 and total_offset < number_of_bytes_in_buffer:
            # Drop total_offset number of bytes from the front of the buffer
            buffer.seek(total_offset)
            copy_of_data = buffer.read()
            buffer.seek(0)
            buffer.truncate(0)
            buffer.write(copy_of_data)
            total_offset = 0
            number_of_bytes_in_buffer = buffer.tell()

        if number_of_bytes_in_buffer >= max_bytes_to_read:
            # Return maximum number of bytes that can be read from the buffer
            buffer.seek(0)
            if header_bytes:
                res = header_bytes + buffer.read(max_bytes_to_read)
            else:
                res = buffer.read(max_bytes_to_read)
            return res

    # Return maximum number of bytes that can be read from the buffer
    buffer.seek(0)
    if header_bytes:
        res = header_bytes + buffer.read(max_bytes_to_read)
    else:
        res = buffer.read(max_bytes_to_read)
    return res


def get_partial_file_chunk(
    bytes_to_prepend: bytes,
    part_number: int,
    part_size: int,
    byte_offset: int,
    path_to_file_to_split: str,
    total_size_of_chunks_being_uploaded: int,
) -> bytes:
    """Read the nth chunk from the file assuming that we are not going to be reading
    or uploading the entire file. This function allows us to read a portion of the file
    and upload it to Synapse.

    Arguments:
        bytes_to_prepend: Bytes to prepend to the first chunk.
        part_number: The part number.
        part_size: The maximum size of the part to read for the upload process.
        byte_offset: The byte offset for the file that has already been read and
            and uploaded. This offset is used to calculate the total offset to read
            the next chunk.
        path_to_file_to_split: The path to the file that we are reading off disk and
            uploading portions of to Synapse.
        total_size_of_chunks_being_uploaded: The total size of the chunks that are being
            uploaded. This is used to calculate the maximum number of bytes to read
            from the file, accounting for the last chunk that may be smaller than the
            chunk size.

    Returns:
        bytes: The bytes that are read from the file for the given `part_size`,
            `byte_offset` and `part_number`.
    """
    header_bytes = None
    if bytes_to_prepend and part_number == 1:
        header_bytes = bytes_to_prepend

    with open(path_to_file_to_split, "rb") as f:
        total_offset = byte_offset + ((part_number - 1) * part_size)

        max_bytes_to_read = min(
            (total_size_of_chunks_being_uploaded - ((part_number - 1) * part_size)),
            part_size,
        )
        f.seek(total_offset - 1)

        if header_bytes:
            res = header_bytes + f.read(max_bytes_to_read)
            return res
        else:
            res = f.read(max_bytes_to_read)
            return res


def get_file_chunk(
    file_path: Union[str, StringIO], part_number: int, chunk_size: int
) -> bytes:
    """Read the nth chunk from the file.

    Arguments:
        file_path: The path to the file.
        part_number: The part number.
        chunk_size: The size of the chunk.

    Returns:
        bytes: The bytes that are read from the file for the given `part_number` and
            `chunk_size`.
    """
    with open(file_path, "rb") as f:
        f.seek((part_number - 1) * chunk_size)
        return f.read(chunk_size)


def get_data_chunk(data: bytes, part_number: int, chunk_size: int) -> bytes:
    """Return the nth chunk of a buffer.

    Arguments:
        data: The data in bytes.
        part_number: The part number.
        chunk_size: The size of the chunk.

    Returns:
        bytes: The bytes that are read from the data for the given `part_number` and
            `chunk_size`.
    """
    return data[((part_number - 1) * chunk_size) : part_number * chunk_size]


def get_part_size(
    part_size: int, file_size: int, minimum_part_size: int, max_number_of_parts: int
) -> int:
    """Calculate the part size for a multipart upload.

    Arguments:
        part_size: The part size.
        file_size: The size of the file.
        minimum_part_size: The minimum part size.
        max_number_of_parts: The maximum number of parts.

    Returns:
        int: The part size (number of bytes) that should be read for the upload process.
    """
    # can't exceed the maximum allowed num parts
    part_size = max(
        part_size, minimum_part_size, int(math.ceil(file_size / max_number_of_parts))
    )
    return part_size


def copy_part_request_body_provider_fn(_) -> None:
    """For an upload copy there are no bytes"""
    return None


def copy_md5_fn(_, response) -> str:
    """
    For a multipart copy we use the md5 returned by the UploadPartCopy command
    when we add the part to the Synapse upload

    we extract the md5 from the <ETag> element in the response.
    use lookahead and lookbehind to find the opening and closing ETag elements but
    do not include those in the match, thus the entire matched string (group 0) will be
    what was between those elements.
    """

    md5_hex = re.search(
        "(?<=<ETag>).*?(?=<\\/ETag>)", (response.content.decode("utf-8"))
    ).group(0)

    # remove quotes found in the ETag to get at the normalized ETag
    return md5_hex.replace("&quot;", "").replace('"', "")
