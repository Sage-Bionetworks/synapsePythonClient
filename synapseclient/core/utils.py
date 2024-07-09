"""
Utility functions useful in the implementation and testing of the Synapse client.
"""

import base64
import cgi
import collections.abc
import datetime
import errno
import gc
import hashlib
import importlib
import inspect
import numbers
import os
import platform
import random
import re
import sys
import tempfile
import threading
import typing
import urllib.parse as urllib_parse
import uuid
import warnings
import zipfile
from dataclasses import asdict, is_dataclass
from typing import TYPE_CHECKING, Callable, TypeVar

import requests
from opentelemetry import context, trace
from opentelemetry.context import Context

if TYPE_CHECKING:
    from synapseclient.models import File, Folder, Project

R = TypeVar("R")

UNIX_EPOCH = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
ISO_FORMAT_MICROS = "%Y-%m-%dT%H:%M:%S.%fZ"
GB = 2**30
MB = 2**20
KB = 2**10
BUFFER_SIZE = 8 * KB

tracer = trace.get_tracer("synapseclient")

SLASH_PREFIX_REGEX = re.compile(r"\/[A-Za-z]:")


def md5_for_file(
    filename: str, block_size: int = 2 * MB, callback: typing.Callable = None
):
    """
    Calculates the MD5 of the given file.
    See source <http://stackoverflow.com/questions/1131220/get-md5-hash-of-a-files-without-open-it-in-python>.

    Arguments:
        filename: The file to read in
        block_size: How much of the file to read in at once (bytes).
                    Defaults to 2 MB
        callback: The callback function that help us show loading spinner on terminal.
                    Defaults to None

    Returns:
        The MD5 Checksum
    """
    loop_iteration = 0
    md5 = hashlib.new("md5", usedforsecurity=False)  # nosec
    with open(filename, "rb") as f:
        while True:
            loop_iteration += 1
            if callback:
                callback()
            data = f.read(block_size)
            if not data:
                break
            md5.update(data)
            del data
            # Garbage collect every 100 iterations
            if loop_iteration % 100 == 0:
                gc.collect()
    return md5


def md5_for_file_hex(
    filename: str, block_size: int = 2 * MB, callback: typing.Callable = None
) -> str:
    """
    Calculates the MD5 of the given file.
    See source <http://stackoverflow.com/questions/1131220/get-md5-hash-of-a-files-without-open-it-in-python>.

    Arguments:
        filename: The file to read in
        block_size: How much of the file to read in at once (bytes).
                    Defaults to 2 MB
        callback: The callback function that help us show loading spinner on terminal.
                    Defaults to None

    Returns:
        The MD5 Checksum
    """

    return md5_for_file(filename, block_size, callback).hexdigest()


@tracer.start_as_current_span("Utils::md5_fn")
def md5_fn(part, _) -> str:
    """Calculate the MD5 of a file-like object.

    Arguments:
        part: A file-like object to read from.

    Returns:
        The MD5 Checksum
    """
    md5 = hashlib.new("md5", usedforsecurity=False)  # nosec
    md5.update(part)
    return md5.hexdigest()


def download_file(url: str, localFilepath: str = None) -> str:
    """
    Downloads a remote file.

    Arguments:
        localFilePath: May be None, in which case a temporary file is created

    Returns:
        localFilePath: The path to the downloaded file
    """

    f = None
    try:
        if localFilepath:
            dir = os.path.dirname(localFilepath)
            if not os.path.exists(dir):
                os.makedirs(dir)
            f = open(localFilepath, "wb")
        else:
            f = tempfile.NamedTemporaryFile(delete=False)
            localFilepath = f.name

        r = requests.get(url, stream=True)
        toBeTransferred = float(r.headers["content-length"])
        for nChunks, chunk in enumerate(r.iter_content(chunk_size=1024 * 10)):
            if chunk:
                f.write(chunk)
                printTransferProgress(nChunks * 1024 * 10, toBeTransferred)
    finally:
        if f:
            f.close()
            printTransferProgress(toBeTransferred, toBeTransferred)

    return localFilepath


def extract_filename(content_disposition_header, default_filename=None):
    """
    Extract a filename from an HTTP content-disposition header field.

    See [this memo](http://tools.ietf.org/html/rfc6266) and
    [this package](http://pypi.python.org/pypi/rfc6266)
    for cryptic details.
    """

    if not content_disposition_header:
        return default_filename
    value, params = cgi.parse_header(content_disposition_header)
    return params.get("filename", default_filename)


def extract_user_name(profile):
    """
    Extract a displayable user name from a user's profile
    """
    if "userName" in profile and profile["userName"]:
        return profile["userName"]
    elif "displayName" in profile and profile["displayName"]:
        return profile["displayName"]
    else:
        if (
            "firstName" in profile
            and profile["firstName"]
            and "lastName" in profile
            and profile["lastName"]
        ):
            return profile["firstName"] + " " + profile["lastName"]
        elif "lastName" in profile and profile["lastName"]:
            return profile["lastName"]
        elif "firstName" in profile and profile["firstName"]:
            return profile["firstName"]
        else:
            return str(profile.get("id", "Unknown-user"))


def _get_from_members_items_or_properties(obj, key):
    if hasattr(obj, key):
        return getattr(obj, key)
    try:
        if hasattr(obj, "properties") and key in obj.properties:
            return obj.properties[key]
        if key in obj:
            return obj[key]
        else:
            return obj["properties"][key]
    except (KeyError, TypeError):
        # We cannot get the key from this obj. So this case will be treated as key not found.
        pass
    return None


# TODO: what does this do on an unsaved Synapse Entity object?
def id_of(obj: typing.Union[str, collections.abc.Mapping, numbers.Number]) -> str:
    """
    Try to figure out the Synapse ID of the given object.

    Arguments:
        obj: May be a string, Entity object, or dictionary

    Returns:
        The ID

    Raises:
        ValueError: if the object doesn't have an ID
    """
    if isinstance(obj, str):
        return str(obj)
    if isinstance(obj, numbers.Number):
        return str(obj)

    id_attr_names = [
        "id",
        "ownerId",
        "tableId",
    ]  # possible attribute names for a synapse Id
    for attribute_name in id_attr_names:
        syn_id = _get_from_members_items_or_properties(obj, attribute_name)
        if syn_id is not None:
            return str(syn_id)

    raise ValueError("Invalid parameters: couldn't find id of " + str(obj))


def concrete_type_of(obj: collections.abc.Mapping):
    """
    Return the concrete type of an object representing a Synapse entity.
    This is meant to operate either against an actual Entity object, or the lighter
    weight dictionary returned by Synapse#getChildren, both of which are Mappings.
    """
    concrete_type = None
    if isinstance(obj, collections.abc.Mapping):
        for key in ("concreteType", "type"):
            concrete_type = obj.get(key)
            if concrete_type:
                break

    if not isinstance(concrete_type, str) or not concrete_type.startswith(
        "org.sagebionetworks.repo.model"
    ):
        raise ValueError("Unable to determine concreteType")

    return concrete_type


def is_in_path(id: str, path: collections.abc.Mapping) -> bool:
    """Determines whether id is in the path as returned from /entity/{id}/path

    Arguments:
        id: synapse id string
        path: object as returned from '/entity/{id}/path'

    Returns:
        True or False
    """
    return id in [item["id"] for item in path["path"]]


def get_properties(entity):
    """Returns the dictionary of properties of the given Entity."""

    return entity.properties if hasattr(entity, "properties") else entity


def is_url(s):
    """Return True if the string appears to be a valid URL."""
    if isinstance(s, str):
        try:
            url_parts = urllib_parse.urlsplit(s)
            # looks like a Windows drive letter?
            if len(url_parts.scheme) == 1 and url_parts.scheme.isalpha():
                return False
            if url_parts.scheme == "file" and bool(url_parts.path):
                return True
            return bool(url_parts.scheme) and bool(url_parts.netloc)
        except Exception:
            return False
    return False


def as_url(s):
    """Tries to convert the input into a proper URL."""
    url_parts = urllib_parse.urlsplit(s)
    # Windows drive letter?
    if len(url_parts.scheme) == 1 and url_parts.scheme.isalpha():
        return "file:///%s" % str(s).replace("\\", "/")
    if url_parts.scheme:
        return url_parts.geturl()
    else:
        return "file://%s" % str(s)


def guess_file_name(string):
    """Tries to derive a filename from an arbitrary string."""
    path = normalize_path(urllib_parse.urlparse(string).path)
    tokens = [x for x in path.split("/") if x != ""]
    if len(tokens) > 0:
        return tokens[-1]

    # Try scrubbing the path of illegal characters
    if len(path) > 0:
        path = re.sub(r"[^a-zA-Z0-9_.+() -]", "", path)
    if len(path) > 0:
        return path
    raise ValueError("Could not derive a name from %s" % string)


def normalize_path(path):
    """Transforms a path into an absolute path with forward slashes only."""
    if path is None:
        return None
    return re.sub(r"\\", "/", os.path.normcase(os.path.abspath(path)))


def equal_paths(path1, path2):
    """
    Compare file paths in a platform neutral way
    """
    return normalize_path(path1) == normalize_path(path2)


def file_url_to_path(url: str, verify_exists: bool = False) -> typing.Union[str, None]:
    """
    Convert a file URL to a path, handling some odd cases around Windows paths.

    Arguments:
        url: a file URL
        verify_exists: If true, return an populated dict only if the resulting file
                        path exists on the local file system.

    Returns:
        a path or None if the URL is not a file URL.
    """
    parts = urllib_parse.urlsplit(url)
    if parts.scheme == "file" or parts.scheme == "":
        path = parts.path
        # A windows file URL, for example file:///c:/WINDOWS/asdf.txt
        # will get back a path of: /c:/WINDOWS/asdf.txt, which we need to fix by
        # lopping off the leading slash character. Apparently, the Python developers
        # think this is not a bug: http://bugs.python.org/issue7965
        if SLASH_PREFIX_REGEX.match(path):
            path = path[1:]
        if os.path.exists(path) or not verify_exists:
            return path
    return None


def is_same_base_url(url1: str, url2: str) -> bool:
    """Compares two urls to see if they are the same excluding up to the base path

    Arguments:
        url1: a URL
        url2: a second URL

    Returns:
        A Boolean
    """
    url1 = urllib_parse.urlsplit(url1)
    url2 = urllib_parse.urlsplit(url2)
    return url1.scheme == url2.scheme and url1.hostname == url2.hostname


def is_synapse_id_str(obj: str) -> typing.Union[str, None]:
    """If the input is a Synapse ID return it, otherwise return None"""
    if isinstance(obj, str):
        m = re.match(r"(syn\d+(\.\d+)?$)", obj)
        if m:
            return m.group(1)
    return None


def get_synid_and_version(
    obj: typing.Union[str, collections.abc.Mapping]
) -> typing.Tuple[str, typing.Union[int, None]]:
    """Extract the Synapse ID and version number from input entity

    Arguments:
            obj: May be a string, Entity object, or dictionary.

    Returns:
        A tuple containing the synapse ID and version number,
            where the version number may be an integer or None if
            the input object does not contain a versonNumber or
            .version notation (if string).

    Example: Get synID and version from string object
        Extract the synID and version number of the entity string ID

            from synapseclient.core import utils
            utils.get_synid_and_version("syn123.4")

        The call above will return the following tuple:

            ('syn123', 4)
    """

    if isinstance(obj, str):
        synapse_id_and_version = is_synapse_id_str(obj)
        if not synapse_id_and_version:
            raise ValueError("The input string was not determined to be a syn ID.")
        m = re.match(r"(syn\d+)(?:\.(\d+))?", synapse_id_and_version)
        id = m.group(1)
        version = int(m.group(2)) if m.group(2) is not None else m.group(2)

        return id, version

    id = id_of(obj)
    version = None
    if "versionNumber" in obj:
        version = obj["versionNumber"]

    return id, version


def bool_or_none(input_value: str) -> typing.Union[bool, None]:
    """
    Attempts to convert a string to a bool. Returns None if it fails.

    Arguments:
        input_value: The string to convert to a bool

    Returns:
        The bool or None if the conversion fails
    """
    if input_value is None or input_value == "":
        return None

    return_value = None

    if input_value.lower() == "true":
        return_value = True
    elif input_value.lower() == "false":
        return_value = False

    return return_value


def datetime_or_none(datetime_str: str) -> typing.Union[datetime.datetime, None]:
    """Attempts to convert a string to a datetime object. Returns None if it fails.

    Some of the expected formats of datetime_str are:

    - 2023-12-04T07:00:00Z
    - 2001-01-01 15:00:00+07:00
    - 2001-01-01 15:00:00-07:00
    - 2023-12-04 07:00:00+00:00
    - 2019-01-01

    Arguments:
        datetime_str: The string to convert to a datetime object

    Returns:
        The datetime object or None if the conversion fails
    """
    try:
        return datetime.datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))
    except Exception:
        return None


def is_date(dt):
    """Objects of class datetime.date and datetime.datetime will be recognized as dates"""
    return isinstance(dt, datetime.date) or isinstance(dt, datetime.datetime)


def to_list(value):
    """Convert the value (an iterable or a scalar value) to a list."""
    if isinstance(value, collections.abc.Iterable) and not isinstance(value, str):
        values = []
        for val in value:
            possible_datetime = None
            if isinstance(val, str):
                possible_datetime = datetime_or_none(value)
            values.append(val if possible_datetime is None else possible_datetime)
        return values
    else:
        possible_datetime = None
        if isinstance(value, str):
            possible_datetime = datetime_or_none(value)
        return [value if possible_datetime is None else possible_datetime]


def _to_iterable(value):
    """Convert the value (an iterable or a scalar value) to an iterable."""
    if isinstance(value, collections.abc.Iterable):
        return value
    return (value,)


def make_bogus_uuid_file() -> str:
    """
    Makes a bogus test file with a uuid4 string for testing. It is the caller's
    responsibility to clean up the file when finished.

    Returns:
        The name of the file
    """

    data = uuid.uuid4()

    f = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    try:
        f.write(str(data))
        f.write("\n")
    finally:
        f.close()

    return normalize_path(f.name)


def make_bogus_data_file(n: int = 100, seed: int = None) -> str:
    """
    Makes a bogus data file for testing. It is the caller's responsibility
    to clean up the file when finished.

    Arguments:
        n: How many random floating point numbers to be written into the file,
            separated by commas
        seed: Random seed for the random numbers

    Returns:
        The name of the file
    """

    if seed is not None:
        random.seed(seed)
    data = [random.gauss(mu=0.0, sigma=1.0) for i in range(n)]

    f = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    try:
        f.write(", ".join(str(n) for n in data))
        f.write("\n")
    finally:
        f.close()

    return normalize_path(f.name)


def make_bogus_binary_file(
    n: int = 1 * KB, filepath: str = None, printprogress: bool = False
) -> str:
    """
    Makes a bogus binary data file for testing. It is the caller's responsibility
    to clean up the file when finished.

    Arguments:
        n: How many bytes to write
        filepath: Where to write the data
        printprogress: Toggle printing of progress

    Returns:
        The name of the file
    """

    with (
        open(filepath, "wb")
        if filepath
        else tempfile.NamedTemporaryFile(mode="wb", suffix=".dat", delete=False)
    ) as f:
        if not filepath:
            filepath = f.name
        progress = 0
        remaining = n
        while remaining > 0:
            buff_size = int(min(remaining, 1 * KB))
            f.write(os.urandom(buff_size))
            remaining -= buff_size
            if printprogress:
                progress += buff_size
                printTransferProgress(progress, n, "Generated ", filepath)
        return normalize_path(filepath)


def to_unix_epoch_time(dt: typing.Union[datetime.date, datetime.datetime, str]) -> int:
    """
    Convert either [datetime.date or datetime.datetime objects](http://docs.python.org/2/library/datetime.html) to UNIX time.
    """
    if type(dt) == str:
        dt = datetime.datetime.fromisoformat(dt.replace("Z", "+00:00"))
    if type(dt) == datetime.date:
        current_timezone = datetime.datetime.now().astimezone().tzinfo
        datetime_utc = datetime.datetime.combine(dt, datetime.time(0, 0, 0, 0)).replace(
            tzinfo=current_timezone
        )
    else:
        # If the datetime is not timezone aware, assume it is in the local timezone.
        # This is required in order for windows to work with the `astimezone` method.
        if dt.tzinfo is None:
            current_timezone = datetime.datetime.now().astimezone().tzinfo
            dt = dt.replace(tzinfo=current_timezone)
        datetime_utc = dt.astimezone(datetime.timezone.utc)
    return int((datetime_utc - UNIX_EPOCH).total_seconds() * 1000)


def to_unix_epoch_time_secs(
    dt: typing.Union[datetime.date, datetime.datetime]
) -> float:
    """
    Convert either [datetime.date or datetime.datetime objects](http://docs.python.org/2/library/datetime.html) to UNIX time.
    """
    if type(dt) == datetime.date:
        current_timezone = datetime.datetime.now().astimezone().tzinfo
        datetime_utc = datetime.datetime.combine(dt, datetime.time(0, 0, 0, 0)).replace(
            tzinfo=current_timezone
        )
    else:
        # If the datetime is not timezone aware, assume it is in the local timezone.
        # This is required in order for windows to work with the `astimezone` method.
        if dt.tzinfo is None:
            current_timezone = datetime.datetime.now().astimezone().tzinfo
            dt = dt.replace(tzinfo=current_timezone)
        datetime_utc = dt.astimezone(datetime.timezone.utc)
    return (datetime_utc - UNIX_EPOCH).total_seconds()


def from_unix_epoch_time_secs(secs):
    """Returns a Datetime object given milliseconds since midnight Jan 1, 1970."""
    if isinstance(secs, str):
        secs = float(secs)

    # utcfromtimestamp() fails for negative values (dates before 1970-1-1) on Windows
    # so, here's a hack that enables ancient events, such as Chris's birthday to be
    # converted from milliseconds since the UNIX epoch to higher level Datetime objects. Ha!
    if platform.system() == "Windows" and secs < 0:
        mirror_date = datetime.datetime.utcfromtimestamp(abs(secs)).replace(
            tzinfo=datetime.timezone.utc
        )

        result = (UNIX_EPOCH - (mirror_date - UNIX_EPOCH)).replace(
            tzinfo=datetime.timezone.utc
        )

        return result
    datetime_instance = datetime.datetime.utcfromtimestamp(secs).replace(
        tzinfo=datetime.timezone.utc
    )

    return datetime_instance


def from_unix_epoch_time(ms) -> datetime.datetime:
    """Returns a Datetime object given milliseconds since midnight Jan 1, 1970."""

    if isinstance(ms, str):
        ms = float(ms)
    return from_unix_epoch_time_secs(ms / 1000.0)


def datetime_to_iso(
    dt: datetime.datetime, sep: str = "T", include_milliseconds_if_zero: bool = True
) -> str:
    """
    Round microseconds to milliseconds (as expected by older clients) and add back
    the "Z" at the end.
    See: http://stackoverflow.com/questions/30266188/how-to-convert-date-string-to-iso8601-standard

    Arguments:
        dt: The datetime to convert
        sep: Seperator character to use.
        include_milliseconds_if_zero: Whether or not to include millseconds in this result
                                        if the number of millseconds is 0.

    Returns:
        The formatted string.
    """
    fmt = (
        "{time.year:04}-{time.month:02}-{time.day:02}"
        "{sep}{time.hour:02}:{time.minute:02}:{time.second:02}.{millisecond:03}{tz}"
    )
    fmt_no_mills = (
        "{time.year:04}-{time.month:02}-{time.day:02}"
        "{sep}{time.hour:02}:{time.minute:02}:{time.second:02}{tz}"
    )
    if dt.microsecond >= 999500:
        dt -= datetime.timedelta(microseconds=dt.microsecond)
        dt += datetime.timedelta(seconds=1)
    rounded_microseconds = int(round(dt.microsecond / 1000.0))
    if include_milliseconds_if_zero or rounded_microseconds:
        return fmt.format(time=dt, millisecond=rounded_microseconds, tz="Z", sep=sep)
    else:
        return fmt_no_mills.format(
            time=dt, millisecond=rounded_microseconds, tz="Z", sep=sep
        )


def iso_to_datetime(iso_time):
    return datetime.datetime.strptime(iso_time, ISO_FORMAT_MICROS)


def format_time_interval(seconds):
    """
    Format a time interval given in seconds to a readable value,
    e.g. \"5 minutes, 37 seconds\".
    """

    periods = (
        ("year", 60 * 60 * 24 * 365),
        ("month", 60 * 60 * 24 * 30),
        ("day", 60 * 60 * 24),
        ("hour", 60 * 60),
        ("minute", 60),
        ("second", 1),
    )

    result = []
    for period_name, period_seconds in periods:
        if seconds > period_seconds or period_name == "second":
            period_value, seconds = divmod(seconds, period_seconds)
            if period_value > 0 or period_name == "second":
                if period_value == 1:
                    result.append("%d %s" % (period_value, period_name))
                else:
                    result.append("%d %ss" % (period_value, period_name))
    return ", ".join(result)


def _find_used(activity, predicate):
    """Finds a particular used resource in an activity that matches a predicate."""

    for resource in activity["used"]:
        if predicate(resource):
            return resource
    return None


def itersubclasses(cls, _seen=None):
    """
    <http://code.activestate.com/recipes/576949/> (r3)

    itersubclasses(cls)

    Generator over all subclasses of a given class, in depth first order.

        >>> list(itersubclasses(int)) == [bool]
        True
        >>> class A(object): pass
        >>> class B(A): pass
        >>> class C(A): pass
        >>> class D(B,C): pass
        >>> class E(D): pass
        >>>
        >>> for cls in itersubclasses(A):
        ...     print(cls.__name__)
        B
        D
        E
        C
        >>> # get ALL (new-style) classes currently defined
        >>> [cls.__name__ for cls in itersubclasses(object)] #doctest: +ELLIPSIS
        ['type', ...'tuple', ...]
    """

    if not isinstance(cls, type):
        raise TypeError(
            "itersubclasses must be called with " "new-style classes, not %.100r" % cls
        )
    if _seen is None:
        _seen = set()
    try:
        subs = cls.__subclasses__()
    except TypeError:  # fails only when cls is type
        subs = cls.__subclasses__(cls)
    for sub in subs:
        if sub not in _seen:
            _seen.add(sub)
            yield sub
            for inner_sub in itersubclasses(sub, _seen):
                yield inner_sub


def normalize_whitespace(s):
    """
    Strips the string and replace all whitespace sequences and other
    non-printable characters with a single space.
    """
    assert isinstance(s, str)
    return re.sub(r"[\x00-\x20\s]+", " ", s.strip())


def normalize_lines(s):
    assert isinstance(s, str)
    s2 = re.sub(r"[\t ]*\n[\t ]*", "\n", s.strip())
    return re.sub(r"[\t ]+", " ", s2)


def _synapse_error_msg(ex):
    """
    Format a human readable error message
    """
    if isinstance(ex, str):
        return ex

    # one line for the root exception and then an additional line per chained cause
    error_str = ""
    depth = 0
    while ex:
        error_str += (
            "\n"
            + ("  " * depth)
            + ("caused by " if depth > 0 else "")
            + ex.__class__.__name__
            + ": "
            + str(ex)
        )

        ex = ex.__cause__
        if ex:
            depth += 1
        else:
            break

    return error_str + "\n\n"


def _limit_and_offset(uri, limit=None, offset=None):
    """
    Set limit and/or offset query parameters of the given URI.
    """
    parts = urllib_parse.urlparse(uri)
    query = urllib_parse.parse_qs(parts.query)
    if limit is None:
        query.pop("limit", None)
    else:
        query["limit"] = limit
    if offset is None:
        query.pop("offset", None)
    else:
        query["offset"] = offset

    new_query_string = urllib_parse.urlencode(query, doseq=True)
    return urllib_parse.urlunparse(
        urllib_parse.ParseResult(
            scheme=parts.scheme,
            netloc=parts.netloc,
            path=parts.path,
            params=parts.params,
            query=new_query_string,
            fragment=parts.fragment,
        )
    )


def query_limit_and_offset(
    query: str, hard_limit: int = 1000
) -> typing.Tuple[str, int, int]:
    """
    Extract limit and offset from the end of a query string.

    Returns:
        A tuple containing the query with limit and offset removed,
        the limit at most equal to the hard_limit,
        and the offset which defaults to 1
    """
    # Regex a lower-case string to simplify matching
    tempQueryStr = query.lower()
    regex = r"\A(.*\s)(offset|limit)\s*(\d*\s*)\Z"

    # Continue to strip off and save the last limit/offset
    match = re.search(regex, tempQueryStr)
    options = {}
    while match is not None:
        options[match.group(2)] = int(match.group(3))
        tempQueryStr = match.group(1)
        match = re.search(regex, tempQueryStr)

    # Get a truncated version of the original query string (not in lower-case)
    query = query[: len(tempQueryStr)].strip()

    # Continue querying until the entire query has been fetched (or crash out)
    limit = min(options.get("limit", hard_limit), hard_limit)
    offset = options.get("offset", 1)

    return query, limit, offset


def extract_synapse_id_from_query(query):
    """
    An unfortunate hack to pull the synapse ID out of a table query of the form
    "select column1, column2 from syn12345 where...."
    needed to build URLs for table services.
    """
    m = re.search(r"from\s+(syn\d+)", query, re.IGNORECASE)
    if m:
        return m.group(1)
    else:
        raise ValueError('Couldn\'t extract synapse ID from query: "%s"' % query)


def printTransferProgress(
    transferred: int,
    toBeTransferred: int,
    prefix: str = "",
    postfix: str = "",
    isBytes: bool = True,
    dt: float = None,
    previouslyTransferred: int = 0,
):
    """Prints a progress bar

    Arguments:
        transferred: a number of items/bytes completed
        toBeTransferred: total number of items/bytes when completed
        prefix: String printed before progress bar
        postfix: String printed after progress bar
        isBytes: A boolean indicating whether to convert bytes to kB, MB, GB etc.
        dt: The time in seconds that has passed since transfer started is used to calculate rate
        previouslyTransferred: the number of bytes that were already transferred before this
                                transfer began (e.g. someone ctrl+c'd out of an upload and
                                restarted it later)
    """
    if not sys.stdout.isatty():
        return
    barLength = 20  # Modify this to change the length of the progress bar
    status = ""
    rate = ""
    if dt is not None and dt != 0:
        rate = (transferred - previouslyTransferred) / float(dt)
        rate = "(%s/s)" % humanizeBytes(rate) if isBytes else rate
    if toBeTransferred < 0:
        defaultToBeTransferred = barLength * 1 * MB
        if transferred > defaultToBeTransferred:
            progress = (
                float(transferred % defaultToBeTransferred) / defaultToBeTransferred
            )
        else:
            progress = float(transferred) / defaultToBeTransferred
    elif toBeTransferred == 0:  # There is nothing to be transferred
        progress = 1
        status = "Done...\n"
    else:
        progress = float(transferred) / toBeTransferred
        if progress >= 1:
            progress = 1
            status = "Done...\n"
    block = int(round(barLength * progress))
    nbytes = humanizeBytes(transferred) if isBytes else transferred
    if toBeTransferred > 0:
        outOf = "/%s" % (humanizeBytes(toBeTransferred) if isBytes else toBeTransferred)
        percentage = "%4.2f%%" % (progress * 100)
    else:
        outOf = ""
        percentage = ""
    text = "\r%s [%s]%s   %s%s %s %s %s    \n" % (
        prefix,
        "#" * block + "-" * (barLength - block),
        percentage,
        nbytes,
        outOf,
        rate,
        postfix,
        status,
    )
    sys.stdout.write(text)
    sys.stdout.flush()


def humanizeBytes(num_bytes):
    if num_bytes is None:
        raise ValueError("bytes must be a number")

    num_bytes = float(num_bytes)
    units = ["bytes", "kB", "MB", "GB", "TB", "PB", "EB"]
    for i, unit in enumerate(units):
        if num_bytes < 1024:
            return "%3.1f%s" % (num_bytes, units[i])
        else:
            num_bytes /= 1024
    return "Oops larger than Exabytes"


def touch(path, times=None):
    """
    Make sure a file exists. Update its access and modified times.
    """
    basedir = os.path.dirname(path)
    if not os.path.exists(basedir):
        try:
            os.makedirs(basedir)
        except OSError as err:
            # alternate processes might be creating these at the same time
            if err.errno != errno.EEXIST:
                raise

    with open(path, "a"):
        os.utime(path, times)
    return path


def is_json(content_type):
    """detect if a content-type is JSON"""
    # The value of Content-Type defined here:
    # http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.7
    return (
        content_type.lower().strip().startswith("application/json")
        if content_type
        else False
    )


def find_data_file_handle(bundle):
    """Return the fileHandle whose ID matches the dataFileHandleId in an entity bundle"""
    for fileHandle in bundle["fileHandles"]:
        if fileHandle["id"] == bundle["entity"]["dataFileHandleId"]:
            return fileHandle
    return None


def unique_filename(path):
    """Returns a unique path by appending (n) for some number n to the end of the filename."""

    base, ext = os.path.splitext(path)
    counter = 0
    while os.path.exists(path):
        counter += 1
        path = base + ("(%d)" % counter) + ext

    return path


class threadsafe_iter:
    """Takes an iterator/generator and makes it thread-safe by serializing call to the
    `next` method of given iterator/generator.
    See: <http://anandology.com/blog/using-iterators-and-generators/>
    """

    def __init__(self, it):
        self.it = it
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.it)


def threadsafe_generator(f):
    """A decorator that takes a generator function and makes it thread-safe.
    See: <http://anandology.com/blog/using-iterators-and-generators/>
    """

    def g(*a, **kw):
        return threadsafe_iter(f(*a, **kw))

    return g


def extract_prefix(keys):
    """
    Takes a list of strings and extracts a common prefix delimited by a dot,
    for example::

        extract_prefix(["entity.bang", "entity.bar", "entity.bat"])
        # returns "entity"

    """
    prefixes = set()
    for key in keys:
        parts = key.split(".")
        if len(parts) > 1:
            prefixes.add(parts[0])
        else:
            return ""
    if len(prefixes) == 1:
        return prefixes.pop() + "."
    return ""


def temp_download_filename(destination, file_handle_id):
    suffix = "synapse_download_" + (
        str(file_handle_id) if file_handle_id else str(uuid.uuid4())
    )
    return (
        os.path.join(destination, suffix)
        if os.path.isdir(destination)
        else destination + "." + suffix
    )


def extract_zip_file_to_directory(
    zip_file: zipfile.ZipFile, zip_entry_name: str, target_dir: str
) -> str:
    """
    Extracts a specified file in a zip to the specified directory

    Arguments:
        zip_file: an opened zip file. e.g. "with zipfile.ZipFile(zipfilepath) as zip_file:"
        zip_entry_name: the name of the file to be extracted from the zip
                        e.g. folderInsideZipIfAny/fileName.txt
        target_dir: the directory to which the file will be extracted

    Returns:
        full path to the extracted file
    """
    file_base_name = os.path.basename(zip_entry_name)  # base name of the file
    filepath = os.path.join(
        target_dir, file_base_name
    )  # file path to the cached file to write

    # Create the cache directory if it does not exist
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    # write the file from the zip into the cache
    with open(filepath, "wb") as cache_file:
        cache_file.write(zip_file.read(zip_entry_name))

    return filepath


def is_integer(x):
    try:
        return float.is_integer(x)
    except TypeError:
        try:
            int(x)
            return True
        except (ValueError, TypeError):
            # anything that's not an integer, for example: empty string, None, 'NaN' or float('Nan')
            return False


def topolgical_sort(graph: typing.Dict[str, typing.List[str]]) -> list:
    """Given a graph in the form of a dictionary returns a sorted list
    Adapted from:
    <http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/>

    Arguments:
        graph: a dictionary with values containing lists of keys
        referencing back into the dictionary

    Returns:
        A sorted list of items
    """
    graph_unsorted = graph.copy()
    graph_sorted = []
    # Convert the unsorted graph into a hash table. This gives us
    # constant-time lookup for checking if edges are unresolved

    # Run until the unsorted graph is empty.
    while graph_unsorted:
        # Go through each of the node/edges pairs in the unsorted
        # graph. If a set of edges doesn't contain any nodes that
        # haven't been resolved, that is, that are still in the
        # unsorted graph, remove the pair from the unsorted graph,
        # and append it to the sorted graph. Note here that by using
        # using the items() method for iterating, a copy of the
        # unsorted graph is used, allowing us to modify the unsorted
        # graph as we move through it. We also keep a flag for
        # checking that that graph is acyclic, which is true if any
        # nodes are resolved during each pass through the graph. If
        # not, we need to bail out as the graph therefore can't be
        # sorted.
        acyclic = False
        for node, edges in list(graph_unsorted.items()):
            for edge in edges:
                if edge in graph_unsorted:
                    break
            else:
                acyclic = True
                del graph_unsorted[node]
                graph_sorted.append((node, edges))

        if not acyclic:
            # We've passed through all the unsorted nodes and
            # weren't able to resolve any of them, which means there
            # are nodes with cyclic edges that will never be resolved,
            # so we bail out with an error.
            raise RuntimeError(
                "A cyclic dependency occurred."
                " Some files in provenance reference each other circularly."
            )
    return graph_sorted


def caller_module_name(current_frame):
    """
    Returns the name of the module in which the calling function resides.

    Arguments:
        current_frame: use inspect.currentframe().

    Returns:
        the name of the module calling the function, foo(),
        in which this calling_module() is invoked.
        Ignores callers that belong in the same module as foo()
    """

    current_frame_filename = (
        current_frame.f_code.co_filename
    )  # filename in which foo() resides

    # go back a frame takes us to the frame calling foo()
    caller_frame = current_frame.f_back
    caller_filename = caller_frame.f_code.co_filename

    # find the first frame that does not have the same filename.
    # this ensures that we don't consider functions within
    # the same module as foo() that use foo() as a helper function
    while caller_filename == current_frame_filename:
        caller_frame = caller_frame.f_back
        caller_filename = caller_frame.f_code.co_filename

    return inspect.getmodulename(caller_filename)


def attempt_import(module_name: str, fail_message: str):
    """
    Attempt to import a module by name and return the imported module if successful.

    Arguments:
        module_name: The name of the module to import.
        fail_message: The error message to display if the import fails.

    Returns:
        The imported module.

    Raises:
        ImportError: If the module cannot be imported.

    """
    try:
        return importlib.import_module(module_name)
    except ImportError:
        sys.stderr.write(
            (
                fail_message
                + "To install this library on Mac or Linux distributions:\n"
                "    (sudo) pip install %s\n\n"
                "On Windows, right click the Command Prompt(cmd.exe) and select 'Run as administrator' then:\n"
                "    pip install %s\n\n"
                "\n\n\n" % (module_name, module_name)
            )
        )
        raise


def require_param(param, name):
    if param is None:
        raise ValueError("%s parameter is required." % name)


def snake_case(string):
    """Convert the given string from CamelCase to snake_case"""
    # https://stackoverflow.com/a/1176023
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()


def is_base64_encoded(input_string):
    """Return whether the given input string appears to be base64 encoded"""
    if not input_string:
        # None, empty string are not considered encoded
        return False
    try:
        # see if we can decode it and then reencode it back to the input
        byte_string = (
            input_string
            if isinstance(input_string, bytes)
            else str.encode(input_string)
        )
        return base64.b64encode(base64.b64decode(byte_string)) == byte_string
    except Exception:
        return False


class deprecated_keyword_param:
    """A decorator to use to warn when a keyword parameter from a function has been deprecated
    and is intended for future removal. Will emit a warning such a keyword is passed."""

    def __init__(self, keywords, version, reason):
        self.keywords = set(keywords)
        self.version = version
        self.reason = reason

    def __call__(self, fn):
        def wrapper(*args, **kwargs):
            found = self.keywords.intersection(kwargs)
            if found:
                warnings.warn(
                    "Parameter(s) {} deprecated since version {}; {}".format(
                        sorted(list(found)), self.version, self.reason
                    ),
                    category=DeprecationWarning,
                    stacklevel=2,
                )

            return fn(*args, **kwargs)

        return wrapper


class Spinner:
    def __init__(self, msg=""):
        self._tick = 0
        self.msg = msg

    def print_tick(self):
        spinner = ["|", "/", "-", "\\"][self._tick % 4]
        if sys.stdin.isatty():
            sys.stdout.write(f"\r {spinner} {self.msg}")
            sys.stdout.flush()
        self._tick += 1


def run_and_attach_otel_context(
    callable_function: Callable[..., R], current_context: Context
) -> R:
    """
    This is a generic function that will run a callable function and attach the passed in
    OpenTelemetry context to the thread or context that the function is running on.

    This is a hack to get around AsyncIO `run_in_executor` not propagating the context
    to the code it's executing. When we are directly calling async functions after
    SYNPY-1411 we will be able to remove this function.

    Example: Adding this to a `run_in_executor` call
        Note the 2 lambdas that are required:

            import asyncio
            from opentelemetry import context
            from synapseclient import Synapse

            loop = asyncio.get_event_loop()
            current_context = context.get_current()
            await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(synapse_client=synapse_client).delete(
                        obj="syn123",
                    ),
                    current_context,
                ),
            )
    """
    context.attach(current_context)
    return callable_function()


def delete_none_keys(incoming_object: typing.Dict) -> None:
    """Clean up the incoming object by removing any keys with None values."""
    if incoming_object:
        for key in list(incoming_object.keys()):
            if incoming_object[key] is None:
                del incoming_object[key]


def merge_dataclass_entities(
    source: typing.Union["Project", "Folder", "File"],
    destination: typing.Union["Project", "Folder", "File"],
    fields_to_ignore: typing.List[str] = None,
) -> typing.Union["Project", "Folder", "File"]:
    """
    Utility function to merge two dataclass entities together. This is used when we are
    upserting an entity from the Synapse service with the requested changes.

    Arguments:
        source: The source entity to merge from.
        destination: The destination entity to merge into.
        fields_to_ignore: A list of fields to ignore when merging.

    Returns:
        The destination entity with the merged values.
    """
    # Convert dataclasses to dictionaries
    destination_dict = asdict(destination)
    source_dict = asdict(source)
    modified_items = {}

    # Update destination_dict with source_dict, keeping destination's values in case of conflicts
    for key, value in source_dict.items():
        if fields_to_ignore is not None and key in fields_to_ignore:
            continue
        if is_dataclass(getattr(source, key)):
            if hasattr(destination, key):
                setattr(destination, key, getattr(source, key))
            else:
                modified_items[key] = merge_dataclass_entities(
                    getattr(source, key), destination=getattr(destination, key)
                )
        elif key not in destination_dict or destination_dict[key] is None:
            modified_items[key] = value
        elif key == "annotations":
            modified_items[key] = {
                **(value or {}),
                **destination_dict[key],
            }

    # Update destination's fields with the merged dictionary
    for key, value in modified_items.items():
        setattr(destination, key, value)

    return destination
