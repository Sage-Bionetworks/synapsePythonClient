"""Common utility functions used during upload."""

import math
import re
from io import StringIO
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from synapseclient import Synapse


def get_in_memory_csv_chunk(
    bytes_to_prepend: bytes,
    part_number: int,
    chunk_size: int,
    byte_offset: int,
    path_to_original_file: str,
    file_size: int,
    client: "Synapse",
) -> bytes:
    """Read the nth chunk from the file.

    Arguments:
        file_path: The path to the file.
        part_number: The part number.
        chunk_size: The size of the chunk.
    """
    header_bytes = None
    if bytes_to_prepend and part_number == 1:
        header_bytes = bytes_to_prepend

    with open(path_to_original_file, "rb") as f:
        total_offset = byte_offset + ((part_number - 1) * chunk_size)
        client.logger.info(f"Part number: {part_number}, total_offset: {total_offset}")

        max_bytes_to_read = min((file_size - total_offset), chunk_size)
        client.logger.info(f"max_bytes_to_read: {max_bytes_to_read}")
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
