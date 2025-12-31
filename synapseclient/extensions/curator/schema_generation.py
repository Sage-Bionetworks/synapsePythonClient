import json
import multiprocessing
import os
import pathlib
import re
import time
import urllib.request
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from enum import Enum
from inspect import isfunction
from itertools import product
from logging import Logger
from string import whitespace
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Iterable,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from deprecated import deprecated

from synapseclient.core.utils import test_import_pandas

try:
    from dataclasses_json import config, dataclass_json
except ImportError:
    # dataclasses_json is an optional dependency only available with curator extra
    # Provide dummy implementations to avoid import errors
    def dataclass_json(cls):
        """Dummy decorator when dataclasses_json is not installed"""
        return cls

    def config(**kwargs):
        """Dummy config function when dataclasses_json is not installed"""
        return None


try:
    from inflection import camelize
except ImportError:
    # inflection is an optional dependency only available with curator extra
    def camelize(string, uppercase_first_letter=True):
        """Dummy camelize function when inflection is not installed"""
        return None


try:
    from rdflib import Namespace
except ImportError:
    # rdflib is an optional dependency
    Namespace = None  # type: ignore

from synapseclient import Synapse
from synapseclient.core.typing_utils import DataFrame as DATA_FRAME_TYPE
from synapseclient.core.typing_utils import np, nx

if TYPE_CHECKING:
    NUMPY_INT_64 = np.int64
    MULTI_GRAPH_TYPE = nx.MultiDiGraph
    GRAPH_TYPE = nx.Graph
    DI_GRAPH_TYPE = nx.DiGraph
else:
    NUMPY_INT_64 = object
    MULTI_GRAPH_TYPE = object
    GRAPH_TYPE = object
    DI_GRAPH_TYPE = object

X = TypeVar("X")

BLACKLISTED_CHARS = ["(", ")", ".", " ", "-"]
COMPONENT_NAME_DELIMITER = "#"
COMPONENT_RULES_DELIMITER = "^^"
RULE_DELIMITER = "::"


# Characters display names of nodes that are not allowed
BLACKLISTED_CHARACTERS_NODE_NAMES = ["(", ")", ".", "-"]
# Names of nodes that are used internally
RESERVED_NODE_NAMES = {"entityId"}

DisplayLabelType = Literal["class_label", "display_label"]
EntryType = Literal["class", "property"]

Items = dict[str, Union[str, float, list[str]]]
Property = dict[str, Union[str, float, list, dict]]
TypeDict = dict[str, Union[str, Items]]
AllOf = dict[str, Any]


class ColumnType(Enum):
    """Names of values allowed in the columnType column in datamodel csvs."""


class AtomicColumnType(ColumnType):
    """Column Types that are not lists"""

    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"


class ListColumnType(ColumnType):
    """Column Types that are lists"""

    STRING_LIST = "string_list"
    INTEGER_LIST = "integer_list"
    BOOLEAN_LIST = "boolean_list"


ALL_COLUMN_TYPE_VALUES = [member.value for member in AtomicColumnType] + [
    member.value for member in ListColumnType
]


# Translates list types to their atomic type
LIST_TYPE_DICT = {
    ListColumnType.STRING_LIST: AtomicColumnType.STRING,
    ListColumnType.INTEGER_LIST: AtomicColumnType.INTEGER,
    ListColumnType.BOOLEAN_LIST: AtomicColumnType.BOOLEAN,
}


class JSONSchemaFormat(Enum):
    """
    Allowed formats by the JSON Schema validator used by Synapse: https://github.com/everit-org/json-schema#format-validators
    For descriptions see: https://json-schema.org/understanding-json-schema/reference/type#format
    """

    DATE_TIME = "date-time"
    EMAIL = "email"
    HOSTNAME = "hostname"
    IPV4 = "ipv4"
    IPV6 = "ipv6"
    URI = "uri"
    URI_REFERENCE = "uri-reference"
    URI_TEMPLATE = "uri-template"
    JSON_POINTER = "json-pointer"
    DATE = "date"
    TIME = "time"
    REGEX = "regex"
    RELATIVE_JSON_POINTER = "relative-json-pointer"


# TODO: remove: https://sagebionetworks.jira.com/browse/SYNPY-1724
class ValidationRuleName(Enum):
    """Names of validation rules that are used to create JSON Schema"""

    LIST = "list"
    DATE = "date"
    URL = "url"
    REGEX = "regex"
    IN_RANGE = "inRange"


# TODO: remove: https://sagebionetworks.jira.com/browse/SYNPY-1724
class RegexModule(Enum):
    """This enum are allowed modules for the regex validation rule"""

    SEARCH = "search"
    MATCH = "match"


# TODO: remove: https://sagebionetworks.jira.com/browse/SYNPY-1724
@dataclass
class ValidationRule:
    """
    This class represents a Schematic validation rule to be used for creating JSON Schemas

    Attributes:
        name: The name of the validation rule
        incompatible_rules: Other validation rules this rule can not be paired with
        parameters: Parameters for the validation rule that need to be collected for the JSON Schema
    """

    name: ValidationRuleName
    incompatible_rules: list[ValidationRuleName]
    parameters: Optional[list[str]] = None


# TODO: remove: https://sagebionetworks.jira.com/browse/SYNPY-1724
_VALIDATION_RULES = {
    "list": ValidationRule(
        name=ValidationRuleName.LIST,
        incompatible_rules=[],
    ),
    "date": ValidationRule(
        name=ValidationRuleName.DATE,
        incompatible_rules=[
            ValidationRuleName.IN_RANGE,
            ValidationRuleName.URL,
        ],
    ),
    "url": ValidationRule(
        name=ValidationRuleName.URL,
        incompatible_rules=[
            ValidationRuleName.IN_RANGE,
            ValidationRuleName.DATE,
        ],
    ),
    "regex": ValidationRule(
        name=ValidationRuleName.REGEX,
        incompatible_rules=[
            ValidationRuleName.IN_RANGE,
        ],
        parameters=["module", "pattern"],
    ),
    "inRange": ValidationRule(
        name=ValidationRuleName.IN_RANGE,
        incompatible_rules=[
            ValidationRuleName.URL,
            ValidationRuleName.DATE,
            ValidationRuleName.REGEX,
        ],
        parameters=["minimum", "maximum"],
    ),
}


@dataclass
class Node:
    """A node in graph from the data model."""

    name: Any
    """Name of the node."""

    fields: dict
    """Fields of the node"""

    def __post_init__(self) -> None:
        if "displayName" not in self.fields:
            raise ValueError(f"Node: {str(self.name)} missing displayName field")
        self.display_name = str(self.fields["displayName"])


def load_json(file_path: str) -> Any:
    """Load json document from file path or url

    :arg str file_path: The path of the url doc, could be url or file path
    """
    # TODO: Can I swap this to HTTPX instead??
    if file_path.startswith("http"):
        with urllib.request.urlopen(file_path, timeout=200) as url:
            data = json.loads(url.read().decode())
            return data
    # handle file path
    else:
        with open(file_path, encoding="utf8") as fle:
            data = json.load(fle)
            return data


def attr_dict_template(key_name: str) -> dict[str, dict[str, dict]]:
    """Create a single empty attribute_dict template.

    Args:
        key_name (str): Attribute/node to use as the key in the dict.

    Returns:
        dict[str, dict[str, dict]]: template single empty attribute_relationships dictionary
    """
    return {key_name: {"Relationships": {}}}


def check_allowed_values(
    dmr: "DataModelRelationships", entry_id: str, value: Any, relationship: str
) -> None:
    """Checks that the entry is in the allowed values if they exist for the relationship

    Args:
        dmr: DataModelRelationships, the data model relationships object
        entry_id: The id of the entry
        value: The value to check
        relationship (str): The name of the relationship to check for allowed values

    Raises:
        ValueError: If the value isn't in the list of allowed values
    """
    allowed_values = dmr.get_allowed_values(relationship)
    if allowed_values and value not in allowed_values:
        msg = f"For entry: '{entry_id}', '{value}' not in allowed values: {allowed_values}"
        raise ValueError(msg)


def trim_commas_df(
    dataframe: DATA_FRAME_TYPE,
    allow_na_values: Optional[bool] = False,
) -> DATA_FRAME_TYPE:
    """Removes empty (trailing) columns and empty rows from pandas dataframe (manifest data).

    Args:
        dataframe: pandas dataframe with data from manifest file.
        allow_na_values (bool, optional): If true, allow pd.NA values in the dataframe

    Returns:
        df: cleaned-up pandas dataframe.
    """
    # remove all columns which have substring "Unnamed" in them
    dataframe = dataframe.loc[:, ~dataframe.columns.str.contains("^Unnamed")]

    # remove all completely empty rows
    dataframe = dataframe.dropna(how="all", axis=0)

    if allow_na_values is False:
        # Fill in nan cells with empty strings
        dataframe.fillna("", inplace=True)
    return dataframe


def find_and_convert_ints(
    dataframe: DATA_FRAME_TYPE,
) -> tuple[DATA_FRAME_TYPE, DATA_FRAME_TYPE]:
    """
    Find strings that represent integers and convert to type int
    Args:
        dataframe: dataframe with nulls masked as empty strings
    Returns:
        ints: dataframe with values that were converted to type int
        is_int: dataframe with boolean values indicating which cells were converted to type int

    """
    test_import_pandas()
    from pandarallel import pandarallel
    from pandas import DataFrame
    from pandas.api.types import is_integer

    large_manifest_cutoff_size = 1000
    # Find integers stored as strings and replace with entries of type np.int64
    if (
        dataframe.size < large_manifest_cutoff_size
    ):  # If small manifest, iterate as normal for improved performance
        ints = dataframe.map(  # type:ignore
            lambda cell: convert_ints(cell), na_action="ignore"
        ).fillna(False)

    else:  # parallelize iterations for large manifests
        pandarallel.initialize(verbose=1)
        ints = dataframe.parallel_applymap(  # type:ignore
            lambda cell: convert_ints(cell), na_action="ignore"
        ).fillna(False)

    # Identify cells converted to integers
    is_int = ints.map(is_integer)  # type:ignore

    assert isinstance(ints, DataFrame)
    assert isinstance(is_int, DataFrame)

    return ints, is_int


def convert_ints(string: str) -> Union[NUMPY_INT_64, bool]:
    """
    Lambda function to convert a string to an integer if possible, otherwise returns False
    Args:
        string: string to attempt conversion to int
    Returns:
        string converted to type int if possible, otherwise False
    """
    from numpy import int64

    if isinstance(string, str) and str.isdigit(string):
        return int64(string)
    return False


def convert_floats(dataframe: DATA_FRAME_TYPE) -> DATA_FRAME_TYPE:
    """
    Convert strings that represent floats to type float
    Args:
        dataframe: dataframe with nulls masked as empty strings
    Returns:
        float_df: dataframe with values that were converted to type float. Columns are type object
    """
    test_import_pandas()
    from pandas import to_numeric

    # create a separate copy of the manifest
    # before beginning conversions to store float values
    float_df = deepcopy(dataframe)

    # convert strings to numerical dtype (float) if possible, preserve non-numerical strings
    for col in dataframe.columns:
        float_df[col] = to_numeric(float_df[col], errors="coerce").astype("object")

        # replace values that couldn't be converted to float with the original str values
        float_df[col].fillna(dataframe[col][float_df[col].isna()], inplace=True)

    return float_df


def get_str_pandas_na_values() -> List[str]:
    test_import_pandas()
    from pandas._libs.parsers import STR_NA_VALUES  # type: ignore

    STR_NA_VALUES_FILTERED = deepcopy(STR_NA_VALUES)

    try:
        STR_NA_VALUES_FILTERED.remove("None")
    except KeyError:
        pass

    return STR_NA_VALUES_FILTERED


def read_csv(
    path_or_buffer: str,
    keep_default_na: bool = False,
    encoding: str = "utf8",
    **load_args: Any,
) -> DATA_FRAME_TYPE:
    """
    A wrapper around pd.read_csv that filters out "None" from the na_values list.

    Args:
        path_or_buffer: The path to the file or a buffer containing the file.
        keep_default_na: Whether to keep the default na_values list.
        encoding: The encoding of the file.
        **load_args: Additional arguments to pass to pd.read_csv.

    Returns:
        pd.DataFrame: The dataframe created from the CSV file or buffer.
    """
    test_import_pandas()
    from pandas import read_csv as pandas_read_csv

    STR_NA_VALUES_FILTERED = get_str_pandas_na_values()

    na_values = load_args.pop(
        "na_values", STR_NA_VALUES_FILTERED if not keep_default_na else None
    )

    return pandas_read_csv(  # type: ignore
        path_or_buffer,
        na_values=na_values,
        keep_default_na=keep_default_na,
        encoding=encoding,
        **load_args,
    )


def load_df(
    file_path: str,
    preserve_raw_input: bool = True,
    data_model: bool = False,
    allow_na_values: bool = False,
    **load_args: Any,
) -> DATA_FRAME_TYPE:
    """
    Universal function to load CSVs and return DataFrames
    Parses string entries to convert as appropriate to type int, float, and pandas timestamp
    Pandarallel is used for type inference for large manifests to improve performance

    Args:
        file_path (str): path of csv to open
        preserve_raw_input (bool, optional): If false, convert cell datatypes to an inferred type
        data_model (bool, optional): bool, indicates if importing a data model
        allow_na_values (bool, optional): If true, allow pd.NA values in the dataframe
        **load_args(dict): dict of key value pairs to be passed to the pd.read_csv function

    Raises:
        ValueError: When pd.read_csv on the file path doesn't return as dataframe

    Returns:
        pd.DataFrame: a processed dataframe for manifests or unprocessed df for data models and
      where indicated
    """
    test_import_pandas()
    from pandas import DataFrame

    # Read CSV to df as type specified in kwargs
    org_df = read_csv(file_path, encoding="utf8", **load_args)  # type: ignore
    if not isinstance(org_df, DataFrame):
        raise ValueError(
            (
                "Pandas did not return a dataframe. "
                "Pandas will return a TextFileReader if chunksize parameter is used."
            )
        )

    # only trim if not data model csv
    if not data_model:
        org_df = trim_commas_df(org_df, allow_na_values=allow_na_values)

    if preserve_raw_input:
        return org_df

    ints, is_int = find_and_convert_ints(org_df)

    float_df = convert_floats(org_df)

    # Store values that were converted to type int in the final dataframe
    processed_df = float_df.mask(is_int, other=ints)

    return processed_df


class DataModelParser:
    """
    This class takes in a path to a data model and will convert it to an
    attributes:relationship dictionarythat can then be further converted into a graph data model.
    Other data model types may be added in the future.
    """

    def __init__(self, path_to_data_model: str, logger: Logger) -> None:
        """
        Args:
            path_to_data_model, str: path to data model.
        """

        self.path_to_data_model = path_to_data_model
        self.model_type = self.get_model_type()
        self.logger = logger

    def get_model_type(self) -> str:
        """
        Parses the path to the data model to extract the extension and determine the
          data model type.

        Args:
            path_to_data_model, str: path to data model
        Returns:
            str: uppercase, data model file extension.
        Note: Consider moving this to Utils.
        """
        return pathlib.Path(self.path_to_data_model).suffix.replace(".", "").upper()

    def parse_model(self) -> dict[str, dict[str, Any]]:
        """Given a data model type, instantiate and call the appropriate data model parser.
        Returns:
            model_dict, dict:
                {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
        Raises:
            Value Error if an incorrect model type is passed.
        Note: in future will add base model parsing in this step too and extend new model
          off base model.
        """
        # Call appropriate data model parser and return parsed model.
        if self.model_type == "CSV":
            csv_parser = DataModelCSVParser(logger=self.logger)
            model_dict = csv_parser.parse_csv_model(self.path_to_data_model)
        elif self.model_type == "JSONLD":
            jsonld_parser = DataModelJSONLDParser()
            model_dict = jsonld_parser.parse_jsonld_model(self.path_to_data_model)
        else:
            raise ValueError(
                (
                    "Only data models of type CSV or JSONLD are accepted, "
                    "you provided a model type "
                    f"{self.model_type}, please resubmit in the proper format."
                )
            )
        return model_dict


class DataModelCSVParser:
    """DataModelCSVParser"""

    def __init__(self, logger: Optional[Any] = None) -> None:
        self.logger = logger
        # Instantiate DataModelRelationships
        self.dmr = DataModelRelationships()
        # Load relationships dictionary.
        self.rel_dict = self.dmr.define_data_model_relationships()
        # Get edge relationships
        self.edge_relationships_dictionary = self.dmr.retrieve_rel_headers_dict(
            edge=True
        )
        # Load required csv headers
        self.required_headers = self.dmr.define_required_csv_headers()
        # Get the type for each value that needs to be submitted.
        # using csv_headers as keys to match required_headers/relationship_types
        self.rel_val_types = {
            value["csv_header"]: value["type"]
            for value in self.rel_dict.values()
            if "type" in value
        }

    def check_schema_definition(self, model_df: DATA_FRAME_TYPE) -> None:
        """Checks if a schema definition data frame contains the right required headers.
        Args:
            model_df: a pandas dataframe containing schema definition; see example here:
              https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0
        Raises: Exception if model_df does not have the required headers.
        """
        if "Requires" in list(model_df.columns) or "Requires Component" in list(
            model_df.columns
        ):
            raise ValueError(
                "The input CSV schema file contains the 'Requires' and/or the 'Requires "
                "Component' column headers. These columns were renamed to 'DependsOn' and "
                "'DependsOn Component', respectively. Switch to the new column names."
            )
        if not set(self.required_headers).issubset(set(list(model_df.columns))):
            raise ValueError(
                f"Schema extension headers: {set(list(model_df.columns))} "
                f"do not match required schema headers: {self.required_headers}"
            )
        if set(self.required_headers).issubset(set(list(model_df.columns))):
            self.logger.debug("Schema definition csv ready for processing!")

    def parse_entry(self, attr: dict, relationship: str) -> Any:
        """Parse attr entry baed on type
        Args:
            attr, dict: single row of a csv model in dict form, where only the required
              headers are keys. Values are the entries under each header.
            relationship, str: one of the header relationships to parse the entry of.
        Returns:
            parsed_rel_entry, any: parsed entry for downstream processing based on the entry type.
        """

        rel_val_type = self.rel_val_types[relationship]
        # Parse entry based on type:
        # If the entry should be preserved as a bool dont convert to str.
        if rel_val_type is bool and isinstance(attr[relationship], bool):
            parsed_rel_entry = attr[relationship]
        # Move strings to list if they are comma separated. Schema order is preserved,
        # remove any empty strings added by trailing commas
        elif rel_val_type is list:
            parsed_rel_entry = attr[relationship].strip().split(",")
            parsed_rel_entry = [r.strip() for r in parsed_rel_entry if r]
        # Convert value string if dictated by rel_val_type, strip whitespace.
        elif rel_val_type is str:
            parsed_rel_entry = str(attr[relationship]).strip()
        else:
            raise ValueError(
                (
                    "The value type recorded for this relationship, is not currently "
                    "supported for CSV parsing. Please check with your DCC."
                )
            )
        return parsed_rel_entry

    def gather_csv_attributes_relationships(
        self, model_df: DATA_FRAME_TYPE
    ) -> dict[str, dict[str, Any]]:
        """Parse csv into a attributes:relationshps dictionary to be used in downstream efforts.
        Args:
            model_df: pd.DataFrame, data model that has been loaded into pandas DataFrame.
        Returns:
            attr_rel_dictionary: dict,
                {Attribute Display Name: {
                    Relationships: {
                                    CSV Header: Value}}}
        """
        test_import_pandas()
        from pandas import isnull

        # Check csv schema follows expectations.
        self.check_schema_definition(model_df)

        # get attributes from Attribute column
        attributes = model_df.to_dict("records")

        # Check for presence of optional columns
        model_includes_column_type = "columnType" in model_df.columns
        model_includes_format = "Format" in model_df.columns
        model_includes_pattern = "Pattern" in model_df.columns

        # Build attribute/relationship dictionary
        relationship_types = self.required_headers
        attr_rel_dictionary = {}

        for attr in attributes:
            attribute_name = attr["Attribute"]
            # Add attribute to dictionary
            attr_rel_dictionary.update(attr_dict_template(attribute_name))
            # Fill in relationship info for each attribute.
            for relationship in relationship_types:
                if not isnull(attr[relationship]):
                    parsed_rel_entry = self.parse_entry(
                        attr=attr, relationship=relationship
                    )
                    attr_rel_dictionary[attribute_name]["Relationships"].update(
                        {relationship: parsed_rel_entry}
                    )
            if model_includes_column_type:
                column_type_dict = self.parse_column_type(attr)
                attr_rel_dictionary[attribute_name]["Relationships"].update(
                    column_type_dict
                )
            if model_includes_format:
                format_dict = self.parse_format(attr)
                attr_rel_dictionary[attribute_name]["Relationships"].update(format_dict)

            if "Minimum" in model_df.columns:
                minimum_dict = self.parse_minimum_maximum(attr, "Minimum")
                attr_rel_dictionary[attribute_name]["Relationships"].update(
                    minimum_dict
                )

            if "Maximum" in model_df.columns:
                maximum_dict = self.parse_minimum_maximum(attr, "Maximum")
                attr_rel_dictionary[attribute_name]["Relationships"].update(
                    maximum_dict
                )

            if model_includes_pattern:
                pattern_dict = self.parse_pattern(attr)
                attr_rel_dictionary[attribute_name]["Relationships"].update(
                    pattern_dict
                )
        return attr_rel_dictionary

    def parse_column_type(self, attr: dict) -> dict:
        """Parse the attribute type for a given attribute.

        Args:
            attr (dict): The attribute dictionary.

        Returns:
            dict: A dictionary containing the parsed column type information if present
            else an empty dict
        """
        test_import_pandas()
        from pandas import isna

        column_type = attr.get("columnType")

        # If no column type specified, we don't want to add any entry to the dictionary
        if isna(column_type):
            return {}

        # column types should be case agnostic and valid
        column_type = str(column_type).strip().lower()

        check_allowed_values(
            self.dmr,
            entry_id=attr["Source"],
            value=column_type,
            relationship="columnType",
        )

        return {"ColumnType": column_type}

    def parse_minimum_maximum(
        self, attr: dict, relationship: str
    ) -> dict[str, Union[float, int]]:
        """Parse minimum/maximum value for a given attribute.

        Args:
            attr: single row of a csv model in dict form, where only the required
              headers are keys. Values are the entries under each header.
            relationship: either "Minimum" or "Maximum"
        Returns:
            dict[str, Union[float, int]]: A dictionary containing the parsed minimum/maximum value
            if present else an empty dict
        """
        from numbers import Number

        from pandas import isna

        value = attr.get(relationship)

        # If maximum and minimum are not specified, we don't want to add any entry to the dictionary
        if isna(value):
            return {}

        # Validate that the value is numeric
        if not isinstance(value, Number) or isinstance(value, bool):
            raise ValueError(
                f"The {relationship} value: {attr[relationship]} is not numeric, "
                "please correct this value in the data model."
            )

        # if both maximum and minimum are present, check if maximum > minimum
        if attr.get("Minimum") and attr.get("Maximum"):
            maximum = attr.get("Maximum")
            minimum = attr.get("Minimum")
            if maximum < minimum:
                raise ValueError(
                    f"The Maximum value: {maximum} must be greater than the Minimum value: {minimum}"
                )

        return {relationship: value}

    def parse_format(self, attribute_dict: dict) -> dict[str, str]:
        """Finds the format value if it exists and returns it as a dictionary.

        Args:
            attribute_dict: The attribute dictionary.

        Returns:
            A dictionary containing the format value if it exists
            else an empty dict
        """
        test_import_pandas()
        from pandas import isna

        format_value = attribute_dict.get("Format")

        if isna(format_value):
            return {}

        format_string = str(format_value).strip().lower()

        check_allowed_values(
            self.dmr,
            entry_id=attribute_dict["Format"],
            value=format_string,
            relationship="format",
        )

        return {"Format": format_string}

    def parse_pattern(self, attribute_dict: dict) -> dict[str, str]:
        """Finds the pattern value if it exists and returns it as a dictionary.

        Args:
            attribute_dict: The attribute dictionary.
        Returns:
            A dictionary containing the pattern value if it exists
            else an empty dict
        """
        from pandas import isna

        pattern_value = attribute_dict.get("Pattern")

        if isna(pattern_value):
            return {}

        pattern_string = str(pattern_value).strip()

        return {"Pattern": pattern_string}

    def parse_csv_model(
        self,
        path_to_data_model: str,
    ) -> dict[str, dict[str, Any]]:
        """Load csv data model and parse into an attributes:relationships dictionary
        Args:
            path_to_data_model, str: path to data model
        Returns:
            model_dict, dict:{Attribute Display Name: {
                                                Relationships: {
                                                        CSV Header: Value}}}
        """
        # Load the csv data model to DF
        model_df = load_df(file_path=path_to_data_model, data_model=True)

        # Gather info from the model
        model_dict = self.gather_csv_attributes_relationships(model_df)

        return model_dict


class DataModelJSONLDParser:
    """DataModelJSONLDParser"""

    def __init__(
        self,
    ) -> None:
        # Instantiate DataModelRelationships
        self.dmr = DataModelRelationships()
        # Load relationships dictionary.
        self.rel_dict = self.dmr.define_data_model_relationships()

    def parse_jsonld_dicts(
        self, rel_entry: dict[str, str]
    ) -> Union[str, dict[str, str]]:
        """Parse incoming JSONLD dictionaries, only supported dictionaries are non-edge
            dictionaries.
        Note:
            The only two dictionaries we expect are a single entry dictionary containing
            id information and dictionaries where the key is the attribute label
            (and it is expected to stay as the label). The individual rules per component are not
            attached to nodes but rather parsed later in validation rule parsing.
            So the keys do not need to be converted to display names.
        Args:
            rel_entry, Any: Given a single entry and relationship in a JSONLD data model,
                the recorded value
        Returns:
            str, the JSONLD entry ID
            dict, JSONLD dictionary entry returned.
        """

        # Retrieve ID from a dictionary recording the ID
        if set(rel_entry.keys()) == {"@id"}:
            parsed_rel_entry: Union[str, dict[str, str]] = rel_entry["@id"]
        # Parse any remaining dictionaries
        else:
            parsed_rel_entry = rel_entry
        return parsed_rel_entry

    def parse_entry(
        self,
        rel_entry: Any,
        id_jsonld_key: str,
        model_jsonld: list[dict],
    ) -> Any:
        """Parse an input entry based on certain attributes

        Args:
            rel_entry: Given a single entry and relationship in a JSONLD data model,
                the recorded value
            id_jsonld_key: str, the jsonld key for id
            model_jsonld: list[dict], list of dictionaries, each dictionary is an entry
                in the jsonld data model
        Returns:
            Any: n entry that has been parsed base on its input type and
              characteristics.
        """
        # Parse dictionary entries
        if isinstance(rel_entry, dict):
            parsed_rel_entry: Any = self.parse_jsonld_dicts(rel_entry)

        # Parse list of dictionaries to make a list of entries with context stripped (will update
        # this section when contexts added.)
        elif isinstance(rel_entry, list) and isinstance(rel_entry[0], dict):
            parsed_rel_entry = self.convert_entry_to_dn_label(
                [r[id_jsonld_key].split(":")[1] for r in rel_entry], model_jsonld
            )
        # Strip context from string and convert true/false to bool
        elif isinstance(rel_entry, str):
            # Remove contexts and treat strings as appropriate.
            if ":" in rel_entry and "http:" not in rel_entry:
                parsed_rel_entry = rel_entry.split(":")[1]
                # Convert true/false strings to boolean
                if parsed_rel_entry.lower() == "true":
                    parsed_rel_entry = True
                elif parsed_rel_entry.lower == "false":
                    parsed_rel_entry = False
            else:
                parsed_rel_entry = self.convert_entry_to_dn_label(
                    rel_entry, model_jsonld
                )

        # For anything else get that
        else:
            parsed_rel_entry = self.convert_entry_to_dn_label(rel_entry, model_jsonld)

        return parsed_rel_entry

    def label_to_dn_dict(self, model_jsonld: list[dict]) -> dict:
        """
        Generate a dictionary of labels to display name, so can easily look up
          display names using the label.
        Args:
            model_jsonld: list of dictionaries, each dictionary is an entry in the jsonld data model
        Returns:
            dn_label_dict: dict of model labels to display names
        """
        jsonld_keys_to_extract = ["label", "displayName"]
        label_jsonld_key, dn_jsonld_key = [
            self.rel_dict[key]["jsonld_key"] for key in jsonld_keys_to_extract
        ]
        dn_label_dict = {}
        for entry in model_jsonld:
            dn_label_dict[entry[label_jsonld_key]] = entry[dn_jsonld_key]
        return dn_label_dict

    def convert_entry_to_dn_label(
        self, parsed_rel_entry: Union[str, list], model_jsonld: list[dict]
    ) -> Union[str, list, None]:
        """Convert a parsed entry to display name, taking into account the entry type
        Args:
            parsed_rel_entry: an entry that has been parsed base on its input type
              and characteristics.
            model_jsonld: list of dictionaries, each dictionary is an entry in the jsonld data model
        Returns:
            parsed_rel_entry: an entry that has been parsed based on its input type and
              characteristics, and converted to display names.
        """
        # Get a dictionary of display_names mapped to labels
        dn_label_dict = self.label_to_dn_dict(model_jsonld=model_jsonld)
        dn_label = None

        # Handle if using the display name as the label
        if isinstance(parsed_rel_entry, list):
            dn_label: Union[str, list, None] = [
                dn_label_dict.get(entry) if dn_label_dict.get(entry) else entry
                for entry in parsed_rel_entry
            ]
        elif isinstance(parsed_rel_entry, str):
            converted_label = dn_label_dict.get(parsed_rel_entry)
            if converted_label:
                dn_label = dn_label_dict.get(parsed_rel_entry)
            else:
                dn_label = parsed_rel_entry
        return dn_label

    def gather_jsonld_attributes_relationships(self, model_jsonld: list[dict]) -> dict:
        """
        Args:
            model_jsonld: list of dictionaries, each dictionary is an entry in the jsonld data model
        Returns:
            attr_rel_dictionary: dict,
                {Node Display Name:
                    {Relationships: {
                                     CSV Header: Value}}}
        Notes:
            - Unlike a CSV the JSONLD might already have a base schema attached to it.
              So the attributes:relationship dictionary for importing a CSV vs JSONLD may not match.
            - It is also just about impossible to extract attributes explicitly. Using a dictionary
              should avoid duplications.
            - This is a promiscuous capture and will create an attribute for each model entry.
            - Currently only designed to capture the same information that would be encoded in CSV,
                can be updated in the future.
        TODO:
            - Find a way to delete non-attribute keys, is there a way to reliable distinguish
              after the fact?
            - Right now, here we are stripping contexts, will need to track them in the future.
        """

        # Retrieve relevant JSONLD keys.
        jsonld_keys_to_extract = ["label", "subClassOf", "id", "displayName"]
        label_jsonld_key, _, id_jsonld_key, dn_jsonld_key = [
            self.rel_dict[key]["jsonld_key"] for key in jsonld_keys_to_extract
        ]

        # Build the attr_rel_dictionary
        attr_rel_dictionary = {}
        # Move through each entry in the jsonld model
        for entry in model_jsonld:
            # Get the attr key for the dictionary
            if dn_jsonld_key in entry:
                # The attr_key is the entry display name if one was recorded
                attr_key = entry[dn_jsonld_key]
            else:
                # If not we wil use the get the label.
                attr_key = entry[label_jsonld_key]

            # If the entry has not already been added to the dictionary, add it.
            if attr_key not in attr_rel_dictionary:
                attr_rel_dictionary.update(attr_dict_template(attr_key))

            # Add relationships for each entry
            # Go through each defined relationship type (rel_key) and its attributes (rel_vals)
            for rel_key, rel_vals in self.rel_dict.items():
                # Determine if current entry in the for loop, can be described by the current
                # relationship that is being cycled through.
                # used to also check "csv_header" in rel_vals.keys() which allows all JSONLD
                # values through even if it does not have a CSV counterpart, will allow other
                # values thorough in the else statement now
                if rel_vals["jsonld_key"] in entry.keys() and rel_vals["csv_header"]:
                    # Retrieve entry value associated with the given relationship
                    rel_entry = entry[rel_vals["jsonld_key"]]
                    # If there is an entry parse it by type and add to the attr:relationships
                    # dictionary.
                    if rel_entry:
                        parsed_rel_entry = self.parse_entry(
                            rel_entry=rel_entry,
                            id_jsonld_key=id_jsonld_key,
                            model_jsonld=model_jsonld,
                        )
                        if "@id" not in entry:
                            raise ValueError(
                                "Datatype in JSON-LD missing `@id`: ", entry
                            )
                        check_allowed_values(
                            self.dmr,
                            entry_id=entry["@id"],
                            value=rel_entry,
                            relationship=rel_key,
                        )
                        rel_csv_header = rel_vals["csv_header"]
                        if rel_key == "domainIncludes":
                            # In the JSONLD the domain includes field contains the ids of
                            # attributes that the current attribute is the property/parent of.
                            # Because of this we need to handle these values differently.
                            # We will get the values in the field (parsed_val), then add the
                            # current attribute as to the property key in the
                            # attr_rel_dictionary[p_attr_key].
                            for parsed_val in parsed_rel_entry:
                                attr_in_dict = False
                                # Get propert/parent key (displayName)
                                p_attr_key: Any = ""
                                # Check if the parsed value is already a part of the
                                # attr_rel_dictionary
                                for attr_dn in attr_rel_dictionary:
                                    if parsed_val == attr_dn:
                                        p_attr_key = attr_dn
                                        attr_in_dict = True
                                # If it is part of the dictionary update add current
                                # attribute as a property of the parsed value
                                if attr_in_dict:
                                    if (
                                        rel_csv_header
                                        not in attr_rel_dictionary[p_attr_key][
                                            "Relationships"
                                        ]
                                    ):
                                        attr_rel_dictionary[p_attr_key][
                                            "Relationships"
                                        ].update(
                                            {rel_csv_header: [entry[dn_jsonld_key]]}
                                        )
                                    else:
                                        attr_rel_dictionary[p_attr_key][
                                            "Relationships"
                                        ][rel_csv_header].extend([entry[dn_jsonld_key]])
                                # If the parsed_val is not already recorded in the dictionary,
                                # add it
                                elif not attr_in_dict:
                                    # Get the display name for the parsed value
                                    p_attr_key = self.convert_entry_to_dn_label(
                                        parsed_val, model_jsonld
                                    )

                                    attr_rel_dictionary.update(
                                        attr_dict_template(p_attr_key)
                                    )
                                    attr_rel_dictionary[p_attr_key][
                                        "Relationships"
                                    ].update(
                                        {rel_csv_header: [entry[label_jsonld_key]]}
                                    )

                        else:
                            attr_rel_dictionary[attr_key]["Relationships"].update(
                                {rel_csv_header: parsed_rel_entry}
                            )

                elif (
                    rel_vals["jsonld_key"] in entry.keys()
                    and not rel_vals["csv_header"]
                ):
                    # Retrieve entry value associated with the given relationship
                    rel_entry = entry[rel_vals["jsonld_key"]]
                    # If there is an entry parset it by type and add to the
                    # attr:relationships dictionary.
                    if rel_entry:
                        parsed_rel_entry = self.parse_entry(
                            rel_entry=rel_entry,
                            id_jsonld_key=id_jsonld_key,
                            model_jsonld=model_jsonld,
                        )
                        # Add relationships for each attribute and relationship to the dictionary
                        attr_rel_dictionary[attr_key]["Relationships"].update(
                            {rel_key: parsed_rel_entry}
                        )
        return attr_rel_dictionary

    def parse_jsonld_model(
        self,
        path_to_data_model: str,
    ) -> dict:
        """Convert raw JSONLD data model to attributes relationship dictionary.
        Args:
            path_to_data_model: str, path to JSONLD data model
        Returns:
            model_dict: dict,
                {Node Display Name:
                    {Relationships: {
                                     CSV Header: Value}}}
        """
        # Load the json_ld model to df
        json_load = load_json(path_to_data_model)
        # Convert dataframe to attributes relationship dictionary.
        model_dict = self.gather_jsonld_attributes_relationships(json_load["@graph"])
        return model_dict


def unlist(seq: Sequence[X]) -> Union[Sequence[X], X]:
    """Returns the first item of a sequence

    Args:
        seq (Sequence[X]): A Sequence of any type

    Returns:
        Union[Sequence[X], X]:
          if sequence is length one, return the first item
          otherwise return the sequence
    """
    if len(seq) == 1:
        return seq[0]
    return seq


def rule_in_rule_list(rule: str, rule_list: list[str]) -> Optional[re.Match[str]]:
    """
    Function to standardize
    checking to see if a rule is contained in a list of rules.
    Uses regex to avoid issues arising from validation rules with arguments
    or rules that have arguments updated.
    """
    # separate rule type if arguments are specified
    rule_type = rule.split(" ")[0]

    # Process string and list of strings for regex comparison
    rule_type = rule_type + "[^\|]*"
    rule_list_str = "|".join(rule_list)
    return re.search(rule_type, rule_list_str, flags=re.IGNORECASE)


def extract_component_validation_rules(
    manifest_component: str, validation_rules_dict: dict[str, Union[list, str]]
) -> list[Union[str, list]]:
    """
    Parse a component validation rule dictionary to pull out the rule (if any) for a given manifest

    Args:
        manifest_component, str: Component label, pulled from the manifest directly
        validation_rules_dict, dict[str, list[Union[list,str]]: Validation rules dictionary,
          where keys are the manifest component label, and the value is a parsed set of
          validation rules.
    Returns:
        validation_rules, list[str]: rule for the provided manifest component if one is available,
            if a validation rule is not specified for a given component but "all_other_components"
            is specified (as a key), then pull that one, otherwise return an empty list.
    """
    manifest_component_rule = validation_rules_dict.get(manifest_component)
    all_component_rules = validation_rules_dict.get("all_other_components")
    validation_rules_list = []

    # Capture situation where manifest_component rule is an empty string
    if manifest_component_rule is not None:
        if isinstance(manifest_component_rule, str):
            if manifest_component_rule == "":
                validation_rules_list: list[Union[str, list]] = []
            else:
                validation_rules_list = [manifest_component_rule]
        elif isinstance(manifest_component_rule, list):
            validation_rules_list = manifest_component_rule
    elif all_component_rules:
        if isinstance(all_component_rules, str):
            validation_rules_list = [all_component_rules]
        elif isinstance(all_component_rules, list):
            validation_rules_list = all_component_rules
    else:
        validation_rules_list = []
    return validation_rules_list


class DataModelGraphExplorer:
    """DataModelGraphExplorer"""

    def __init__(
        self,
        graph: MULTI_GRAPH_TYPE,
        logger: Logger,
    ):
        """Load data model graph as a singleton.
        Args:
            G: nx.MultiDiGraph, networkx graph representation of the data model
            logger: Logger instance for logging
        """
        self.logger = logger
        self.graph = graph  # At this point the graph is expected to be fully formed.
        self.dmr = DataModelRelationships()

    def find_properties(self) -> set[str]:
        """
        Identify all properties, as defined by the first node in a pair, connected with
        'domainIncludes' edge type

        Returns:
            properties, set: All properties defined in the data model, each property name
              is defined by its label.
        """
        properties_list: list[str] = []
        for node_1, _, rel in self.graph.edges:
            if rel == self.dmr.get_relationship_value("domainIncludes", "edge_key"):
                properties_list.append(node_1)
        properties_set = set(properties_list)
        return properties_set

    def find_classes(self) -> AbstractSet[str]:
        """
        Identify all classes, as defined but all nodes, minus all properties
        (which are explicitly defined)
        Returns:
            classes, set:  All classes defined in the data model, each class
              name is defined by its label.
        """
        nodes = self.graph.nodes
        properties = self.find_properties()
        classes = nodes - properties
        return classes

    def find_node_range(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> list:
        """Get valid values for the given node (attribute)
        Args:
            node_label, str, Optional[str]: label of the node for which to retrieve valid values
            node_display_name, str, Optional[str]: Display Name of the node for which to
              retrieve valid values
        Returns:
            valid_values, list: List of valid values associated with the provided node.
        """
        node_label = self._get_node_label(node_label, node_display_name)

        valid_values = []
        for node_1, node_2, rel in self.graph.edges:
            if node_1 == node_label and rel == self.dmr.get_relationship_value(
                "rangeIncludes", "edge_key"
            ):
                valid_values.append(node_2)
        valid_values = list(set(valid_values))
        return valid_values

    def get_adjacent_nodes_by_relationship(
        self, node_label: str, relationship: str
    ) -> list[str]:
        """Get a list of nodes that is / are adjacent to a given node, based on a relationship type.

        Args:
            node_label: label of the the node whose edges we need to look at.
            relationship: the type of link(s) that the above node and its immediate neighbors share.

        Returns:
            List of nodes that are adjacent to the given node.
        #checked
        """
        nodes = set()
        for _, node_2, key, _ in self.graph.out_edges(node_label, data=True, keys=True):
            if key == relationship:
                nodes.add(node_2)

        return list(nodes)

    def get_component_node_required(
        self,
        manifest_component: str,
        node_validation_rules: Optional[list[str]] = None,
        node_label: Optional[str] = None,
        node_display_name: Optional[str] = None,
    ) -> bool:
        """Check if a node is required taking into account the manifest component it is defined in
        (requirements can be set in validation rule as well as required column)
        Args:
            manifest_component: str, manifest component display name that the node belongs to.
            node_validation_rules: list[str], validation rules for a given node and component.
            node_label: str, Label of the node you would want to get the comment for.
            node_display_name: str, node display name for the node being queried.
        Returns:
            True, if node is required, False if not
        """
        node_required = False

        if not node_validation_rules:
            # Get node validation rules for a given component
            node_validation_rules = self.get_component_node_validation_rules(
                manifest_component=manifest_component,
                node_label=node_label,
                node_display_name=node_display_name,
            )

        # Check if the validation rule specifies that the node is required for this particular
        # component.
        if rule_in_rule_list("required", node_validation_rules):
            node_required = True
            # To prevent any unintended errors, ensure the Required field for this node is False
            if self.get_node_required(
                node_label=node_label, node_display_name=node_display_name
            ):
                if not node_display_name:
                    assert node_label is not None
                    node_display_name = self.graph.nodes[node_label][
                        self.dmr.get_relationship_value("displayName", "node_label")
                    ]
                error_str = " ".join(
                    [
                        f"For component: {manifest_component} and attribute: {node_display_name}",
                        "requirements are being specified in both the Required field and in the",
                        "Validation Rules. If you desire to use validation rules to set component",
                        "specific requirements for this attribute",
                        "then the Required field needs to be set to False, or the validation may",
                        "not work as intended, for other components where the attribute",
                        "that should not be required.",
                    ]
                )

                self.logger.error(error_str)
        else:
            # If requirements are not being set in the validation rule, then just pull the
            # standard node requirements from the model
            node_required = self.get_node_required(
                node_label=node_label, node_display_name=node_display_name
            )
        return node_required

    def get_component_node_validation_rules(
        self,
        manifest_component: str,
        node_label: Optional[str] = None,
        node_display_name: Optional[str] = None,
    ) -> list:
        """Get validation rules for a given node and component.
        Args:
            manifest_component: str, manifest component display name that the node belongs to.
            node_label: str, Label of the node you would want to get the comment for.
            node_display_name: str, node display name for the node being queried.
        Returns:
            validation_rules: list, validation rules list for a given node and component.
        """
        # get any additional validation rules associated with this node (e.g. can this node
        # be mapped to a list of other nodes)
        node_validation_rules = self.get_node_validation_rules(
            node_label=node_label, node_display_name=node_display_name
        )

        # Parse the validation rules per component if applicable
        if node_validation_rules and isinstance(node_validation_rules, dict):
            node_validation_rules_list = extract_component_validation_rules(
                manifest_component=manifest_component,
                validation_rules_dict=node_validation_rules,  # type: ignore
            )
        else:
            assert isinstance(node_validation_rules, list)
            node_validation_rules_list = node_validation_rules
        return node_validation_rules_list

    def get_digraph_by_edge_type(self, edge_type: str) -> DI_GRAPH_TYPE:
        """Get a networkx digraph of the nodes connected via a given edge_type.
        Args:
            edge_type:
                Edge type to search for, possible types are defined by 'edge_key'
                  in relationship class
        Returns:
        """
        from networkx import DiGraph

        digraph: DiGraph = DiGraph()
        for node_1, node_2, key, _ in self.graph.edges(data=True, keys=True):
            if key == edge_type:
                digraph.add_edge(node_1, node_2)
        return digraph

    def get_ordered_entry(self, key: str, source_node_label: str) -> list[str]:
        """
        Order the values associated with a particular node and edge_key to
          match original ordering in schema.

        Args:
            key (str): a key representing and edge relationship in
              DataModelRelationships.relationships_dictionary
            source_node_label (str): node to look for edges of and order

        Raises:
            KeyError: cannot find source node in graph

        Returns:
            list[str]:
              list of sorted nodes, that share the specified relationship with the source node
              For the example data model, for key='rangeIncludes', source_node_label='CancerType'
                the return would be ['Breast, 'Colorectal', 'Lung', 'Prostate', 'Skin'] in that
                exact order.
        """
        # Check if node is in the graph, if not throw an error.
        if not self.is_class_in_schema(node_label=source_node_label):
            raise KeyError(
                f"Cannot find node: {source_node_label} in the graph, please check entry."
            )

        edge_key = self.dmr.get_relationship_value(key, "edge_key")

        # Handle out edges
        if self.dmr.get_relationship_value(key, "jsonld_direction") == "out":
            # use out edges

            original_edge_weights_dict = {
                attached_node: self.graph[source_node][attached_node][edge_key][
                    "weight"
                ]
                for source_node, attached_node in self.graph.out_edges(
                    source_node_label
                )
                if edge_key in self.graph[source_node][attached_node]
            }
        # Handle in edges
        else:
            # use in edges
            original_edge_weights_dict = {
                attached_node: self.graph[attached_node][source_node][edge_key][
                    "weight"
                ]
                for attached_node, source_node in self.graph.in_edges(source_node_label)
                if edge_key in self.graph[attached_node][source_node]
            }

        sorted_nodes = list(
            dict(
                sorted(original_edge_weights_dict.items(), key=lambda item: item[1])
            ).keys()
        )

        return sorted_nodes

    def get_node_comment(
        self, node_display_name: Optional[str] = None, node_label: Optional[str] = None
    ) -> str:
        """Get the node definition, i.e., the "comment" associated with a given node display name.

        Args:
            node_display_name, str: Display name of the node which you want to get the comment for.
            node_label, str: Label of the node you would want to get the comment for.
        Returns:
            Comment associated with node, as a string.
        """
        node_label = self._get_node_label(node_label, node_display_name)

        if not node_label:
            return ""

        node_definition = self.graph.nodes[node_label][
            self.dmr.get_relationship_value("comment", "node_label")
        ]
        return node_definition

    def get_node_dependencies(
        self,
        source_node: str,
        display_names: bool = True,
        schema_ordered: bool = True,
    ) -> list[str]:
        """Get the immediate dependencies that are related to a given source node.

        Args:
            source_node: The node whose dependencies we need to compute.
            display_names: if True, return list of display names of each of the dependencies.
                           if False, return list of node labels of each of the dependencies.
            schema_ordered:
              if True, return the dependencies of the node following the order of the schema
                (slower).
              if False, return dependencies from graph without guaranteeing schema order (faster)

        Returns:
            List of nodes that are dependent on the source node.
        """

        if schema_ordered:
            # get dependencies in the same order in which they are defined in the schema
            required_dependencies = self.get_ordered_entry(
                key=self.dmr.get_relationship_value("requiresDependency", "edge_key"),
                source_node_label=source_node,
            )
        else:
            required_dependencies = self.get_adjacent_nodes_by_relationship(
                node_label=source_node,
                relationship=self.dmr.get_relationship_value(
                    "requiresDependency", "edge_key"
                ),
            )

        if display_names:
            # get display names of dependencies
            dependencies_display_names = []

            for req in required_dependencies:
                dependencies_display_names.append(
                    self.graph.nodes[req][
                        self.dmr.get_relationship_value("displayName", "node_label")
                    ]
                )

            return dependencies_display_names

        return required_dependencies

    def get_nodes_descendants(self, node_label: str) -> list[str]:
        """Return a list of nodes reachable from source in graph
        Args:
            node_label, str: any given node
        Return:
            all_descendants, list: nodes reachable from source in graph
        """
        from networkx import descendants

        all_descendants = list(descendants(self.graph, node_label))

        return all_descendants

    def get_nodes_display_names(
        self,
        node_list: list[str],
    ) -> list[str]:
        """Get display names associated with the given list of nodes.

        Args:
            node_list: List of nodes whose display names we need to retrieve.

        Returns:
            List of display names.
        """
        node_list_display_names = [
            self.graph.nodes[node][
                self.dmr.get_relationship_value("displayName", "node_label")
            ]
            for node in node_list
        ]

        return node_list_display_names

    def get_node_label(self, node_display_name: str) -> str:
        """Get the node label for a given display name.

        Args:
            node_display_name: Display name of the node which you want to get the label for.
        Returns:
            Node label associated with given node.
            If display name not part of schema, return an empty string.
        """

        node_class_label = get_class_label_from_display_name(
            display_name=node_display_name
        )
        node_property_label = get_property_label_from_display_name(
            display_name=node_display_name
        )

        if node_class_label in self.graph.nodes:
            node_label = node_class_label
        elif node_property_label in self.graph.nodes:
            node_label = node_property_label
        else:
            node_label = ""

        return node_label

    def get_node_range(
        self,
        node_label: Optional[str] = None,
        node_display_name: Optional[str] = None,
        display_names: bool = False,
    ) -> list[str]:
        """
        Get the range, i.e., all the valid values that are associated with a node label.


        Args:
            node_label (Optional[str], optional): Node for which you need to retrieve the range.
              Defaults to None.
            node_display_name (Optional[str], optional): _description_. Defaults to None.
            display_names (bool, optional): _description_. Defaults to False.

        Raises:
            ValueError: If the node cannot be found in the graph.

        Returns:
            list[str]:
              If display_names=False, a list of valid values (labels) associated with a given node.
              If display_names=True, a list of valid values (display names) associated
                with a given node
        """
        node_label = self._get_node_label(node_label, node_display_name)
        try:
            # get node range in the order defined in schema for given node
            required_range = self.find_node_range(node_label=node_label)
        except KeyError as exc:
            raise ValueError(
                f"The source node {node_label} does not exist in the graph. "
                "Please use a different node."
            ) from exc

        if display_names:
            # get the display name(s) of all dependencies
            dependencies_display_names = []

            for req in required_range:
                dependencies_display_names.append(self.graph.nodes[req]["displayName"])

            return dependencies_display_names

        return required_range

    def get_node_required(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> bool:
        """Check if a given node is required or not.

        Note: The possible options that a node can be associated with -- "required" / "optional".

        Args:
            node_label: Label of the node for which you need to look up.
            node_display_name: Display name of the node for which you want look up.
        Returns:
            True: If the given node is a "required" node.
            False: If the given node is not a "required" (i.e., an "optional") node.
        """
        node_label = self._get_node_label(node_label, node_display_name)
        rel_node_label = self.dmr.get_relationship_value("required", "node_label")
        node_required = self.graph.nodes[node_label][rel_node_label]
        return node_required

    def get_node_validation_rules(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> Union[list, dict[str, str]]:
        """Get validation rules associated with a node,

        Args:
            node_label: Label of the node for which you need to look up.
            node_display_name: Display name of the node which you want to get the label for.
        Returns:
            A set of validation rules associated with node, as a list or a dictionary.
        """
        node_label = self._get_node_label(node_label, node_display_name)

        if not node_label:
            return []

        try:
            node_validation_rules = self.graph.nodes[node_label]["validationRules"]
        except KeyError as key_error:
            raise ValueError(
                f"{node_label} is not in the graph, please provide a proper node label"
            ) from key_error

        return node_validation_rules

    def get_subgraph_by_edge_type(self, relationship: str) -> DI_GRAPH_TYPE:
        """Get a subgraph containing all edges of a given type (aka relationship).

        Args:
            relationship: edge / link relationship type with possible values same as in above docs.

        Returns:
            Directed graph on edges of a particular type (aka relationship)
        """
        from networkx import DiGraph

        # prune the metadata model graph so as to include only those edges that
        # match the relationship type
        rel_edges = []
        for node_1, node_2, key, _ in self.graph.out_edges(data=True, keys=True):
            if key == relationship:
                rel_edges.append((node_1, node_2))

        relationship_subgraph: DiGraph = DiGraph()
        relationship_subgraph.add_edges_from(rel_edges)

        return relationship_subgraph

    def find_adjacent_child_classes(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> list[str]:
        """Find child classes of a given node.
        Args:
            node_display_name: Display name of the node to look up.
            node_label: Label of the node to look up.
        Returns:
            List of nodes that are adjacent to the given node, by SubclassOf relationship.
        """
        node_label = self._get_node_label(node_label, node_display_name)
        return self.get_adjacent_nodes_by_relationship(
            node_label=node_label,
            relationship=self.dmr.get_relationship_value("subClassOf", "edge_key"),
        )

    def find_child_classes(self, schema_class: str) -> list:
        """Find schema classes that inherit from the given class
        Args:
            schema_class: node label for the class to from which to look for children.
        Returns:
            list of children to the schema_class.
        """
        child_classes = unlist(list(self.graph.successors(schema_class)))
        assert isinstance(child_classes, list)
        return child_classes

    def find_class_specific_properties(self, schema_class: str) -> list[str]:
        """Find properties specifically associated with a given class
        Args:
            schema_class, str: node/class label, to identify properties for.
        Returns:
            properties, list: List of properties associate with a given schema class.
        Raises:
            KeyError: Key error is raised if the provided schema_class is not in the graph
        """

        if not self.is_class_in_schema(schema_class):
            raise KeyError(
                (
                    f"Schema_class provided: {schema_class} is not in the data model, please check "
                    "that you are providing the proper class/node label"
                )
            )

        properties = []
        for node1, node2 in self.graph.edges():
            if (
                node2 == schema_class
                and "domainValue" in self.graph[node1][schema_class]
            ):
                properties.append(node1)
        return properties

    def find_parent_classes(self, node_label: str) -> list[list[str]]:
        """Find all parents of the provided node
        Args:
            node_label: label of the node to find parents of
        Returns:
            List of list of Parents to the given node.
        """
        from networkx import all_simple_paths, topological_sort

        # Get digraph of nodes with parents
        digraph = self.get_digraph_by_edge_type("parentOf")

        # Get root node
        root_node = list(topological_sort(digraph))[0]

        # Get paths between root_node and the target node.
        paths = all_simple_paths(self.graph, source=root_node, target=node_label)

        return [_path[:-1] for _path in paths]

    def is_class_in_schema(self, node_label: str) -> bool:
        """Determine if provided node_label is in the schema graph/data model.
        Args:
            node_label: label of node to search for in the
        Returns:
            True, if node is in the graph schema
            False, if node is not in graph schema
        """
        return node_label in self.graph.nodes()

    def get_node_column_type(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> Optional[ColumnType]:
        """Gets the column type of the node

        Args:
            node_label: The label of the node to get the type from
            node_display_name: The display name of the node to get the type from

        Raises:
            ValueError: If the value from the node is not allowed

        Returns:
            The column type of the node if it has one, otherwise None
        """
        node_label = self._get_node_label(node_label, node_display_name)
        rel_node_label = self.dmr.get_relationship_value("columnType", "node_label")
        column_type_value = self.graph.nodes[node_label][rel_node_label]
        if column_type_value is None:
            return column_type_value
        column_type_string = str(column_type_value).lower()
        try:
            column_type = AtomicColumnType(column_type_string)
        except ValueError:
            try:
                column_type = ListColumnType(column_type_string)
            except ValueError:
                msg = (
                    f"Node: '{node_label}' had illegal column type value: '{column_type_string}'. "
                    f"Allowed values are: [{ALL_COLUMN_TYPE_VALUES}]"
                )
                raise ValueError(msg)
        return column_type

    def get_node_column_pattern(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> Optional[str]:
        """Gets the regex pattern of the node

        Args:
            node_label: The label of the node to get the type from
            node_display_name: The display name of the node to get the type from

        Raises:
            ValueError: If the value from the node is not allowed

        Returns:
            The column pattern of the node if it has one, otherwise None
        """
        node_label = self._get_node_label(node_label, node_display_name)
        rel_node_label = self.dmr.get_relationship_value("pattern", "node_label")
        pattern = self.graph.nodes[node_label][rel_node_label]

        return pattern

    def get_node_format(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> Optional[JSONSchemaFormat]:
        """Gets the format of the node

        Args:
            node_label: The label of the node to get the format from
            node_display_name: The display name of the node to get the format from

        Returns:
            The format of the node if it has one, otherwise None
        """
        node_label = self._get_node_label(node_label, node_display_name)
        rel_node_label = self.dmr.get_relationship_value("format", "node_label")
        format_value = self.graph.nodes[node_label][rel_node_label]
        if format_value is None:
            return format_value
        format_string = str(format_value).lower()
        column_type = JSONSchemaFormat(format_string)
        return column_type

    def get_node_maximum_minimum_value(
        self,
        relationship_value: str,
        node_label: Optional[str] = None,
        node_display_name: Optional[str] = None,
    ) -> Union[int, float]:
        """Gets the maximum and minimum value of the node

        Args:
            relationship_value: The relationship value (either maximum or minimum) to get the maximum or minimum from
            node_label: The label of the node to get the format from
            node_display_name: The display name of the node to get the format from

        Returns:
            The maximum or minimum value of the node
        """

        node_label = self._get_node_label(node_label, node_display_name)
        rel_node_label = self.dmr.get_relationship_value(
            relationship_value, "node_label"
        )
        value = self.graph.nodes[node_label][rel_node_label]
        return value

    def _get_node_label(
        self, node_label: Optional[str] = None, node_display_name: Optional[str] = None
    ) -> str:
        """Returns the node label if given otherwise gets the node label from the display name

        Args:
            node_label: The label of the node to get the type from
            node_display_name: The display name of the node to get the type from

        Raises:
            ValueError: If neither node_label or node_display_name is provided

        Returns:
            The node label
        """
        if node_label is not None:
            return node_label
        if node_display_name is not None:
            return self.get_node_label(node_display_name)
        raise ValueError("Either 'node_label' or 'node_display_name' must be provided.")


@dataclass_json
@dataclass
class BaseTemplate:
    """Base Template"""

    magic_context: dict[str, str] = field(
        default_factory=lambda: {
            "bts": "http://schema.biothings.io/",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "schema": "http://schema.org/",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
        },
        metadata=config(field_name="@context"),
    )
    magic_graph: list = field(
        default_factory=list, metadata=config(field_name="@graph")
    )
    magic_id: str = field(
        default="http://schema.biothings.io/#0.1", metadata=config(field_name="@id")
    )


@dataclass_json
@dataclass
class PropertyTemplate:
    """Property Template"""

    magic_id: str = field(default="", metadata=config(field_name="@id"))
    magic_type: str = field(default="rdf:Property", metadata=config(field_name="@type"))
    magic_comment: str = field(default="", metadata=config(field_name="rdfs:comment"))
    magic_label: str = field(default="", metadata=config(field_name="rdfs:label"))
    magic_domain_includes: list = field(
        default_factory=list, metadata=config(field_name="schema:domainIncludes")
    )
    magic_range_includes: list = field(
        default_factory=list, metadata=config(field_name="schema:rangeIncludes")
    )
    magic_isPartOf: dict = field(
        default_factory=dict, metadata=config(field_name="schema:isPartOf")
    )
    magic_displayName: str = field(
        default="", metadata=config(field_name="sms:displayName")
    )
    magic_required: str = field(
        default="sms:false", metadata=config(field_name="sms:required")
    )
    magic_validationRules: list = field(
        default_factory=list, metadata=config(field_name="sms:validationRules")
    )
    magic_pattern: list = field(
        default_factory=list, metadata=config(field_name="sms:pattern")
    )


@dataclass_json
@dataclass
class ClassTemplate:
    "Class Template"
    magic_id: str = field(default="", metadata=config(field_name="@id"))
    magic_type: str = field(default="rdfs:Class", metadata=config(field_name="@type"))
    magic_comment: str = field(default="", metadata=config(field_name="rdfs:comment"))
    magic_label: str = field(default="", metadata=config(field_name="rdfs:label"))
    magic_subClassOf: list = field(
        default_factory=list, metadata=config(field_name="rdfs:subClassOf")
    )
    magic_range_includes: list = field(
        default_factory=list, metadata=config(field_name="schema:rangeIncludes")
    )
    magic_isPartOf: dict = field(
        default_factory=dict, metadata=config(field_name="schema:isPartOf")
    )
    magic_displayName: str = field(
        default="", metadata=config(field_name="sms:displayName")
    )
    magic_required: str = field(
        default="sms:false", metadata=config(field_name="sms:required")
    )
    magic_requiresDependency: list = field(
        default_factory=list, metadata=config(field_name="sms:requiresDependency")
    )
    magic_requiresComponent: list = field(
        default_factory=list, metadata=config(field_name="sms:requiresComponent")
    )
    magic_validationRules: list = field(
        default_factory=list, metadata=config(field_name="sms:validationRules")
    )


class DataModelJsonLD:
    """
    #Interface to JSONLD_object
    """

    def __init__(self, graph: MULTI_GRAPH_TYPE, logger: Logger, output_path: str = ""):
        # Setup
        self.logger = logger
        self.graph = graph  # Graph would be fully made at this point.
        self.dmr = DataModelRelationships()
        self.rel_dict = self.dmr.relationships_dictionary
        self.dmge = DataModelGraphExplorer(self.graph, logger)
        self.output_path = output_path

        # Gather the templates
        base_template = BaseTemplate()
        self.base_jsonld_template = json.loads(base_template.to_json())

        property_template = PropertyTemplate()
        self.property_template = json.loads(property_template.to_json())

        class_template = ClassTemplate()
        self.class_template = json.loads(class_template.to_json())
        self.logger = logger

    def get_edges_associated_with_node(
        self, node: str
    ) -> list[tuple[str, str, dict[str, int]]]:
        """Retrieve all edges traveling in and out of a node.
        Args:
            node, str: Label of node in the graph to look for assiciated edges
        Returns:
            node_edges, list: List of Tuples of edges associated with the given node,
              tuple contains the two nodes, plus the weight dict associated with
              the edge connection.
        """
        node_edges = list(self.graph.in_edges(node, data=True))
        node_edges.extend(list(self.graph.out_edges(node, data=True)))
        return node_edges

    def get_edges_associated_with_property_nodes(
        self, node: str
    ) -> list[tuple[str, str, dict[str, int]]]:
        """Get edges associated with property nodes to make sure we add that relationship.
        Args:
            node, str: Label of node property in the graph to look for assiciated edges
        Returns:
            node_edges, list: List of Tuples of edges associated with the given node,
              tuple contains the two nodes, plus the weight dict associated with the
              edge connection.
        """
        # Get edge keys for domainIncludes and subclassOf
        domain_includes_edge_key = self.rel_dict["domainIncludes"]["edge_key"]
        node_edges = []
        # Get dict of edges for the current property node
        node_edges_dict = self.graph[node]
        for node_2, edge_dict in node_edges_dict.items():
            # Look through relationships in the edge dictionary
            for edge_key in edge_dict:
                # If the edge is a property or subclass then add the edges to the list
                if edge_key in [domain_includes_edge_key]:
                    node_edges.append((node, node_2, edge_dict[edge_key]))
        return node_edges

    def add_edge_rels_to_template(  # pylint:disable=too-many-branches
        self, template: dict, rel_vals: dict, node: str
    ) -> dict:
        """
        Args:
            template, dict: single class or property JSONLD template that is in the process of being
             filled.
            rel_vals, dict: sub relationship dict for a given relationship (contains informtion
              like 'edge_rel', 'jsonld_key' etc..)
            node, str: node whose edge information is presently being added to the JSONLD
        Returns:
        """
        # Get all edges associated with the current node
        node_edges = self.get_edges_associated_with_node(node=node)

        # For properties look for reverse relationships too
        if node in self.dmge.find_properties():
            property_node_edges = self.get_edges_associated_with_property_nodes(
                node=node
            )
            node_edges.extend(property_node_edges)

        # Get node pairs and weights for each edge
        for node_1, node_2, _ in node_edges:  # pylint:disable=too-many-nested-blocks
            # Retrieve the relationship(s) and related info between the two nodes
            node_edge_relationships = self.graph[node_1][node_2]

            # Get the relationship edge key
            edge_key = rel_vals["edge_key"]

            # Check if edge_key is even one of the relationships for this node pair.
            if edge_key in node_edge_relationships:
                # for each relationship between the given nodes
                for relationship in node_edge_relationships.keys():
                    # If the relationship defined and edge_key
                    if relationship == edge_key:
                        # TODO: rewrite to use edge_dir
                        domain_includes_edge_key = self.rel_dict["domainIncludes"][
                            "edge_key"
                        ]
                        subclass_of_edge_key = self.rel_dict["subClassOf"]["edge_key"]
                        if edge_key in [subclass_of_edge_key]:
                            if node_2 == node:
                                # Make sure the key is in the template
                                # (differs between properties and classes)
                                if rel_vals["jsonld_key"] in template.keys():
                                    node_1_id = {"@id": "bts:" + node_1}
                                    if (
                                        isinstance(
                                            template[rel_vals["jsonld_key"]], list
                                        )
                                        and node_1_id
                                        not in template[rel_vals["jsonld_key"]]
                                    ):
                                        template[rel_vals["jsonld_key"]].append(
                                            node_1_id
                                        )
                        elif edge_key in [domain_includes_edge_key]:
                            if node_1 == node:
                                # Make sure the key is in the template
                                # (differs between properties and classes)
                                if rel_vals["jsonld_key"] in template.keys():
                                    node_2_id = {"@id": "bts:" + node_2}
                                    # TODO Move this to a helper function to clear up.
                                    if (
                                        isinstance(
                                            template[rel_vals["jsonld_key"]], list
                                        )
                                        and node_2_id
                                        not in template[rel_vals["jsonld_key"]]
                                    ):
                                        template[rel_vals["jsonld_key"]].append(
                                            node_2_id
                                        )
                        else:
                            if node_1 == node:
                                # Make sure the key is in the template
                                # (differs between properties and classes)
                                if rel_vals["jsonld_key"] in template.keys():
                                    node_2_id = {"@id": "bts:" + node_2}
                                    if (
                                        isinstance(
                                            template[rel_vals["jsonld_key"]], list
                                        )
                                        and node_2_id
                                        not in template[rel_vals["jsonld_key"]]
                                    ):
                                        template[rel_vals["jsonld_key"]].append(
                                            node_2_id
                                        )
        return template

    def add_node_info_to_template(
        self, template: dict, rel_vals: dict, node: str
    ) -> dict:
        """For a given node and relationship, add relevant value to template
        Args:
            template, dict: single class or property JSONLD template that is in the process
              of being filled.
            rel_vals, dict: sub relationship dict for a given relationship
              (contains informtion like, 'edge_rel', 'jsonld_key' etc..)
            node, str: node whose information is presently being added to the JSONLD
        Returns:
            template, dict: single class or property JSONLD template that is in the
              process of being filled, and now has had additional node information added.
        """
        from networkx import get_node_attributes

        # Get label for relationship used in the graph
        node_label = rel_vals["node_label"]

        # Get recorded info for current node, and the attribute type
        node_info = get_node_attributes(self.graph, node_label)[node]

        # Add this information to the template
        template[rel_vals["jsonld_key"]] = node_info
        return template

    def fill_entry_template(self, template: dict, node: str) -> dict:
        """
        Fill in a blank JSONLD template with information for each node.
        All relationships are filled from the graph, based on the type of information
          (node or edge)

        Args:
            template, dict: empty class or property template to be filled with
              information for the given node.
            node, str: target node to fill the template out for.
        Returns:
            template, dict: filled class or property template, that has been
              processed and cleaned up.
        """
        data_model_relationships = self.dmr.relationships_dictionary

        # For each field in template fill out with information from the graph
        for rel_vals in data_model_relationships.values():
            # Fill in the JSONLD template for this node, with data from the graph by looking
            # up the nodes edge relationships, and the value information attached to the node.

            # Fill edge information (done per edge type)
            if rel_vals["edge_rel"]:
                template = self.add_edge_rels_to_template(
                    template=template, rel_vals=rel_vals, node=node
                )

            # Fill in node value information
            else:
                template = self.add_node_info_to_template(
                    template=template, rel_vals=rel_vals, node=node
                )

        # Clean up template
        template = self.clean_template(
            template=template,
            data_model_relationships=data_model_relationships,
        )

        # Reorder lists based on weights:
        template = self.reorder_template_entries(
            template=template,
        )

        # Add contexts to certain values
        template = self.add_contexts_to_entries(
            template=template,
        )

        return template

    def add_contexts_to_entries(self, template: dict) -> dict:
        """
        Args:
            template, dict: JSONLD template that has been filled up to
              the current node, with information
        Returns:
            template, dict: JSONLD template where contexts have been added back to certain values.
        Note: This will likely need to be modified when Contexts are truly added to the model
        """
        # pylint:disable=comparison-with-callable
        for jsonld_key in template.keys():
            # Retrieve the relationships key using the jsonld_key
            rel_key = []

            for rel, rel_vals in self.rel_dict.items():
                if "jsonld_key" in rel_vals and jsonld_key == rel_vals["jsonld_key"]:
                    rel_key.append(rel)

            if rel_key:
                rel_key = rel_key[0]
                # If the current relationship can be defined with a 'node_attr_dict'
                if "node_attr_dict" in self.rel_dict[rel_key].keys():
                    try:
                        # if possible pull standard function to get node information
                        rel_func = self.rel_dict[rel_key]["node_attr_dict"]["standard"]
                    except Exception:  # pylint:disable=bare-except
                        # if not pull default function to get node information
                        rel_func = self.rel_dict[rel_key]["node_attr_dict"]["default"]

                    # Add appropritae contexts that have been removed in previous steps
                    # (for JSONLD) or did not exist to begin with (csv)
                    if (
                        rel_key == "id"
                        and rel_func == get_label_from_display_name
                        and "bts" not in str(template[jsonld_key]).lower()
                    ):
                        template[jsonld_key] = "bts:" + template[jsonld_key]
                    elif (
                        rel_key == "required"
                        and rel_func == convert_bool_to_str
                        and "sms" not in str(template[jsonld_key]).lower()
                    ):
                        template[jsonld_key] = (
                            "sms:" + str(template[jsonld_key]).lower()
                        )

        return template

    def clean_template(self, template: dict, data_model_relationships: dict) -> dict:
        """
        Get rid of empty k:v pairs. Fill with a default if specified in the
          relationships dictionary.

        Args:
            template, dict: JSONLD template for a single entry, keys specified in property
              and class templates.
            data_model_relationships, dict: dictionary containing information for each
              relationship type supported.
        Returns:
            template: JSONLD template where unfilled entries have been removed,
              or filled with default depending on specifications in the relationships dictionary.
        """
        for rels in data_model_relationships.values():
            # Get the current relationships, jsonld key
            relationship_jsonld_key = rels["jsonld_key"]
            # Check if the relationship_relationship_key is part of the template,
            # and if it is, look to see if it has an entry
            if (
                relationship_jsonld_key in template.keys()
                and not template[rels["jsonld_key"]]
            ):
                # If there is no value recorded, fill out the template with the
                # default relationship value (if recorded.)
                if "jsonld_default" in rels.keys():
                    template[relationship_jsonld_key] = rels["jsonld_default"]
                else:
                    # If there is no default specified in the relationships dictionary,
                    # delete the empty value from the template.
                    del template[relationship_jsonld_key]
        return template

    def reorder_template_entries(self, template: dict) -> dict:
        """
        In JSONLD some classes or property keys have list values.
        We want to make sure these lists are ordered according to the order supplied by the user.
        This will look specically in lists and reorder those.

        Args:
            template, dict: JSONLD template for a single entry, keys specified in
              property and class templates.
        Returns:
            template, dict: list entries re-ordered to match user supplied order.
        Note:
            User order only matters for nodes that are also attributes
        """
        template_label = template["rdfs:label"]

        for jsonld_key, entry in template.items():
            # Make sure dealing with an edge relationship:
            is_edge = [
                "True"
                for rel_vals in self.rel_dict.values()
                if rel_vals["jsonld_key"] == jsonld_key
                if rel_vals["edge_rel"]
            ]

            # if the entry is of type list and theres more than one value in the
            # list attempt to reorder
            if is_edge and isinstance(entry, list) and len(entry) > 1:
                # Get edge key from data_model_relationships using the jsonld_key:
                key, _ = [
                    (rel_key, rel_vals["edge_key"])
                    for rel_key, rel_vals in self.rel_dict.items()
                    if jsonld_key == rel_vals["jsonld_key"]
                ][0]

                # Order edges
                sorted_edges = self.dmge.get_ordered_entry(
                    key=key, source_node_label=template_label
                )
                if not len(entry) == len(sorted_edges):
                    self.logger.error(
                        (
                            "There is an error with sorting values in the JSONLD, "
                            "please issue a bug report."
                        )
                    )

                edge_weights_dict = {edge: i for i, edge in enumerate(sorted_edges)}
                ordered_edges: list[Union[int, dict]] = [0] * len(
                    edge_weights_dict.keys()
                )
                for edge, normalized_weight in edge_weights_dict.items():
                    ordered_edges[normalized_weight] = {"@id": "bts:" + edge}

                # Throw an error if ordered_edges does not get fully filled as expected.
                if 0 in ordered_edges:
                    self.logger.error(
                        (
                            "There was an issue getting values to match order specified in "
                            "the data model, please submit a help request."
                        )
                    )
                template[jsonld_key] = ordered_edges
        return template

    def generate_jsonld_object(self) -> dict:
        """Create the JSONLD object.
        Returns:
            jsonld_object, dict: JSONLD object containing all nodes and related information
        """
        # Get properties.
        properties = self.dmge.find_properties()

        # Get JSONLD Template
        json_ld_template = self.base_jsonld_template

        # Iterativly add graph nodes to json_ld_template as properties or classes
        for node in self.graph.nodes:
            if node in properties:
                # Get property template
                property_template = deepcopy(self.property_template)
                obj = self.fill_entry_template(template=property_template, node=node)
            else:
                # Get class template
                class_template = deepcopy(self.class_template)
                obj = self.fill_entry_template(template=class_template, node=node)
            json_ld_template["@graph"].append(obj)
        return json_ld_template


def convert_graph_to_jsonld(graph: MULTI_GRAPH_TYPE, logger: Logger) -> dict:
    """convert graph to jsonld"""
    # Make the JSONLD object
    data_model_jsonld_converter = DataModelJsonLD(graph=graph, logger=logger)
    jsonld_dm = data_model_jsonld_converter.generate_jsonld_object()
    return jsonld_dm


def get_attribute_display_name_from_label(
    node_name: str, attr_relationships: dict
) -> str:
    """
    Get attribute display name for a node, using the node label, requires the attr_relationships
      dictionary from the data model parser

    Args:
        node_name, str: node label
        attr_relationships, dict: dictionary defining attributes and relationships,
          generated in data model parser.
    Returns:
        display_name, str: node display name, recorded in attr_relationships.
    """
    if "Attribute" in attr_relationships.keys():
        display_name = attr_relationships["Attribute"]
    else:
        display_name = node_name
    return display_name


def check_if_display_name_is_valid_label(
    display_name: str,
    blacklisted_chars: Optional[list[str]] = None,
) -> bool:
    """Check if the display name can be used as a display label

    Args:
        display_name (str): node display name
        blacklisted_chars (Optional[list[str]], optional):
          characters that are not permitted for synapse annotations uploads.
          Defaults to None.

    Returns:
        bool: True, if the display name can be used as a label, False, if it cannot.
    """
    if blacklisted_chars is None:
        blacklisted_chars = BLACKLISTED_CHARS
    valid_label = not any(char in display_name for char in blacklisted_chars)
    return valid_label


def get_property_label_from_display_name(
    display_name: str, strict_camel_case: bool = False
) -> str:
    """Convert a given display name string into a proper property label string
    Args:
        display_name, str: node display name
        strict_camel_case, bool: Default, False; defines whether or not to use
          strict camel case or not for conversion.
    Returns:
        label, str: property label of display name
    """
    # This is the newer more strict method
    if strict_camel_case:
        display_name = display_name.strip().translate({ord(c): "_" for c in whitespace})
        label = camelize(display_name, uppercase_first_letter=False)

    # This method remains for backwards compatibility
    else:
        display_name = display_name.translate({ord(c): None for c in whitespace})
        label = camelize(display_name.strip(), uppercase_first_letter=False)

    return label


def get_stripped_label(
    display_name: str,
    entry_type: EntryType,
    logger: Logger,
    blacklisted_chars: Optional[list[str]] = None,
) -> str:
    """
    Args:
        display_name, str: node display name
        entry_type, EntryType: 'class' or 'property', defines what type the entry is.
        logger: Logger instance for logging warnings
        blacklisted_chars, list[str]: characters that are not permitted for
          synapse annotations uploads.
    Returns:
        stripped_label, str: class or property label that has been stripped
          of blacklisted characters.
    """
    if blacklisted_chars is None:
        blacklisted_chars = BLACKLISTED_CHARS
    stripped_label = None
    if entry_type.lower() == "class":
        stripped_label = [
            get_class_label_from_display_name(str(display_name)).translate(
                {ord(x): "" for x in blacklisted_chars}
            )
        ][0]

    elif entry_type.lower() == "property":
        stripped_label = [
            get_property_label_from_display_name(str(display_name)).translate(
                {ord(x): "" for x in blacklisted_chars}
            )
        ][0]

    logger.warning(
        (
            f"Cannot use display name {display_name} as the data model label, "
            "because it is not formatted properly. Please remove all spaces and "
            f"blacklisted characters: {str(blacklisted_chars)}. "
            f"The following label was assigned instead: {stripped_label}"
        )
    )
    return stripped_label


def get_class_label_from_display_name(
    display_name: str, strict_camel_case: bool = False
) -> str:
    """Convert a given display name string into a proper class label string
    Args:
        display_name, str: node display name
        strict_camel_case, bool: Default, False; defines whether or not to
         use strict camel case or not for conversion.
    Returns:
        label, str: class label of display name
    """
    # This is the newer more strict method
    if strict_camel_case:
        display_name = display_name.strip().translate({ord(c): "_" for c in whitespace})
        label = camelize(display_name, uppercase_first_letter=True)

    # This method remains for backwards compatibility
    else:
        display_name = display_name.translate({ord(c): None for c in whitespace})
        label = camelize(display_name.strip(), uppercase_first_letter=True)

    return label


def get_label_from_display_name(
    display_name: str,
    entry_type: EntryType,
    logger: Logger,
    strict_camel_case: bool = False,
    data_model_labels: DisplayLabelType = "class_label",
) -> str:
    """Get node label from provided display name, based on whether the node is a class or property
    Args:
        display_name, str: node display name
        entry_type, EntryType: 'class' or 'property', defines what type the entry is.
        logger: Logger instance for logging warnings and errors
        strict_camel_case, bool: Default, False; defines whether or not to use strict camel
          case or not for conversion.
    Returns:
        label, str: label to be used for the provided display name.
    """
    if data_model_labels == "display_label":
        # Check that display name can be used as a label.
        valid_display_name = check_if_display_name_is_valid_label(
            display_name=display_name
        )
        # If the display name is valid, set the label to be the display name
        if valid_display_name:
            label = display_name
        # If not, set get a stripped class or property label (as indicated by the entry type)
        else:
            label = get_stripped_label(
                display_name=display_name, entry_type=entry_type, logger=logger
            )

    else:
        label = get_schema_label(
            display_name=display_name,
            entry_type=entry_type,
            strict_camel_case=strict_camel_case,
            logger=logger,
        )

    return label


def get_schema_label(
    display_name: str, entry_type: EntryType, strict_camel_case: bool, logger: Logger
) -> str:
    """Get the class or property label for a given display name
    Args:
        display_name, str: node display name
        entry_type, EntryType: 'class' or 'property', defines what type the entry is.
        strict_camel_case, bool: Default, False; defines whether or not to use strict
          camel case or not for conversion.
        logger: Logger instance for logging errors
    Returns:
        label, str: class label of display name
    Raises:
        Error Logged if entry_type.lower(), is not either 'class' or 'property'
    """
    label = None
    if entry_type.lower() == "class":
        label = get_class_label_from_display_name(
            display_name=display_name, strict_camel_case=strict_camel_case
        )

    elif entry_type.lower() == "property":
        label = get_property_label_from_display_name(
            display_name=display_name, strict_camel_case=strict_camel_case
        )
    else:
        logger.error(
            (
                f"The entry type submitted: {entry_type}, is not one of the "
                "permitted types: 'class' or 'property'"
            )
        )
    return label


def convert_bool_to_str(provided_bool: bool) -> str:
    """Convert bool to string.
    Args:
        provided_bool, str: true or false bool
    Returns:
        Boolean converted to 'true' or 'false' str as appropriate.
    """
    return str(provided_bool)


def get_individual_rules(rule: str, validation_rules: list, logger: Logger) -> list:
    """Extract individual rules from a string and add to a list of rules
    Args:
        rule, str: validation rule that has been parsed from a component rule.
        validation_rules, list: list of rules being collected,
            if this is the first time the list is being added to, it will be empty
        logger: Logger instance for logging errors
    Returns:
        validation_rules, list: list of rules being collected.
    """
    # Separate multiple rules (defined by addition of the rule delimiter)
    if RULE_DELIMITER in rule:
        validation_rules.append(parse_single_set_validation_rules(rule, logger))
    # Get single rule
    else:
        validation_rules.append(rule)
    return validation_rules


def get_component_name_rules(
    component_names: list[str], component_rule: str, logger: Logger
) -> tuple[list[str], str]:
    """
    Get component name and rule from an string that was initially split by the
      COMPONENT_RULES_DELIMITER

    Args:
        component_names, list[str]: list of components, will be empty
          if being added to for the first time.
        component_rule, str: component rule string that has only been split by
          the COMPONENT_RULES_DELIMITER
        logger: Logger instance for logging errors
    Returns:
        Tuple[list,str]: list with the a new component name or 'all_other_components' appended,
            rule with the component name stripped off.
    Raises:
        Error Logged if it looks like a component name should have been added to the
          list, but was not.
    """
    # If a component name is not attached to the rule, have it apply to all other components
    if COMPONENT_NAME_DELIMITER != component_rule[0]:
        component_names.append("all_other_components")
    # Get the component name if available
    else:
        component_names.append(
            component_rule.split(" ")[0].replace(COMPONENT_NAME_DELIMITER, "")
        )
        if component_names[-1] == " ":
            logger.error(
                f"There was an error capturing at least one of the component names "
                f"in the following rule: {component_rule}, "
                f"please ensure there is not extra whitespace or non-allowed characters."
            )

        component_rule = component_rule.replace(component_rule.split(" ")[0], "")
        component_rule = component_rule.strip()
    return component_names, component_rule


def check_for_duplicate_components(
    component_names: list[str], validation_rule_string: str, logger: Logger
) -> None:
    """
    Check if component names are repeated in a validation rule
    Error Logged if a component name is duplicated.

    Args:
        component_names (list[str]): list of components identified in the validation rule
        validation_rule_string (str): validation rule, used if error needs to be raised.
        logger: Logger instance for logging errors
    """
    duplicated_entries = [cn for cn in component_names if component_names.count(cn) > 1]
    if duplicated_entries:
        logger.error(
            f"Oops, it looks like the following rule {validation_rule_string}, "
            "contains the same component name more than once. An attribute can "
            "only have a single rule applied per manifest/component."
        )


def parse_component_validation_rules(
    validation_rule_string: str,
    logger: Logger,
) -> dict[str, list[str]]:
    """
    If a validation rule is identified to be formatted as a component validation rule,
      parse to a dictionary of components:rules

    Args:
        validation_rule_string (str):  validation rule provided by user.
        logger: Logger instance for logging errors

    Returns:
        dict[str, list[str]]: validation rules parsed to a dictionary where the key
          is the component name (or 'all_other_components') and the value is the parsed
          validation rule for the given component.
    """
    component_names: list[str] = []
    validation_rules: list[list[str]] = []

    component_rules = validation_rule_string.split(COMPONENT_RULES_DELIMITER)
    # Extract component rules, per component
    for component_rule in component_rules:
        component_rule = component_rule.strip()
        if component_rule:
            # Get component name attached to rule
            component_names, component_rule = get_component_name_rules(
                component_names=component_names,
                component_rule=component_rule,
                logger=logger,
            )

            # Get rules
            validation_rules = get_individual_rules(
                rule=component_rule, validation_rules=validation_rules, logger=logger
            )

    # Ensure we collected the component names and validation rules like expected
    if len(component_names) != len(validation_rules):
        logger.error(
            f"The number of components names and validation rules does not match "
            f"for validation rule: {validation_rule_string}."
        )

    # If a component name is repeated throw an error.
    check_for_duplicate_components(component_names, validation_rule_string, logger)

    validation_rules_dict = dict(zip(component_names, validation_rules))

    return validation_rules_dict


def parse_single_set_validation_rules(
    validation_rule_string: str, logger: Logger
) -> list[str]:
    """Parse a single set of validation rules.

    Args:
        validation_rule_string (str): validation rule provided by user.
        logger: Logger instance for logging errors

    Returns:
        list: the validation rule string split by the rule delimiter
    """
    # Try to catch an improperly formatted rule
    if COMPONENT_NAME_DELIMITER == validation_rule_string[0]:
        logger.error(
            f"The provided validation rule {validation_rule_string}, looks to be formatted as a "
            "component based rule, but is missing the necessary formatting, "
            "please refer to the SchemaHub documentation for more details."
        )

    return validation_rule_string.split(RULE_DELIMITER)


def parse_validation_rules(
    validation_rules: Union[list, dict], logger: Logger
) -> Union[list, dict]:
    """Split multiple validation rules based on :: delimiter

    Args:
        validation_rules (Union[list, dict]): List or Dictionary of validation rules,
            if list:, contains a string validation rule
            if dict:, key is the component the rule (value) is applied to
        logger: Logger instance for logging errors

    Returns:
        Union[list, dict]: Parsed validation rules, component rules are output
          as a dictionary, single sets are a list.
    """
    if isinstance(validation_rules, dict):
        # Rules pulled in as a dict can be used directly
        return validation_rules

    # If rules are already parsed from the JSONLD
    if len(validation_rules) > 1 and isinstance(validation_rules[-1], str):
        return validation_rules
    # Parse rules set for a subset of components/manifests
    if COMPONENT_RULES_DELIMITER in validation_rules[0]:
        return parse_component_validation_rules(
            validation_rule_string=validation_rules[0], logger=logger
        )
    # Parse rules that are set across *all* components/manifests
    return parse_single_set_validation_rules(
        validation_rule_string=validation_rules[0], logger=logger
    )


class DataModelRelationships:
    """Data Model Relationships"""

    def __init__(self) -> None:
        self.relationships_dictionary = self.define_data_model_relationships()

    def define_data_model_relationships(self) -> dict:
        """Define the relationships and their attributes so they can be accessed
          through other classes.
        The key is how it the relationship will be referenced throughout Schematic.
        Note: Though we could use other keys to determine which keys define nodes and edges,
            edge_rel is used as an explicit definition, for easier code readability.
        key:
            jsonld_key: Name for relationship in the JSONLD.
                        Include in all sub-dictionaries.
            csv_header: Str, name for this relationship in the CSV data model.
                        Enter None if not part of the CSV data model.
            node_label: Name for relationship in the graph representation of the data model.
                        Do not include this key for edge relationships.
            type: type, type of expected to be read into graph creation.
            edge_rel: True, if this relationship defines an edge
                      False, if is a value relationship
                      Include in all sub-dictionaries.
            required_header: True, if relationship header is required for the csv
            jsonld_default:
                Defines default values to fill for JSONLD generation.
                Used during func DataModelJsonLD.clean_template(), to fill value with a default,
                  if not supplied in the data model.
            node_attr_dict: This is used to add information to nodes in the model.
                Only include for nodes not edges.
                set default values for this relationship
                key is the node relationship name, value is the default value.
                If want to set default as a function create a nested dictionary.
                    {'default': default_function,
                     'standard': alternative function to call if relationship is present for a node
                    }
                If adding new functions to node_dict will
                    need to modify data_model_nodes.generate_node_dict in
            allowed_values: A list of values the entry must be  one of
            edge_dir: str, 'in'/'out' is the edge an in or out edge. Define for edge relationships
            jsonld_dir: str, 'in'/out is the direction in or out in the JSONLD.
            pattern: regex pattern that the entry must match
        """
        map_data_model_relationships = {
            "displayName": {
                "jsonld_key": "sms:displayName",
                "csv_header": "Attribute",
                "node_label": "displayName",
                "type": str,
                "edge_rel": False,
                "required_header": True,
                "node_attr_dict": {
                    "default": get_attribute_display_name_from_label,
                    "standard": get_attribute_display_name_from_label,
                },
            },
            "label": {
                "jsonld_key": "rdfs:label",
                "csv_header": None,
                "node_label": "label",
                "type": str,
                "edge_rel": False,
                "required_header": False,
                "node_attr_dict": {
                    "default": get_label_from_display_name,
                    "standard": get_label_from_display_name,
                },
            },
            "comment": {
                "jsonld_key": "rdfs:comment",
                "csv_header": "Description",
                "node_label": "comment",
                "type": str,
                "edge_rel": False,
                "required_header": True,
                "node_attr_dict": {"default": "TBD"},
            },
            "rangeIncludes": {
                "jsonld_key": "schema:rangeIncludes",
                "csv_header": "Valid Values",
                "edge_key": "rangeValue",
                "jsonld_direction": "out",
                "edge_dir": "out",
                "type": list,
                "edge_rel": True,
                "required_header": True,
            },
            "requiresDependency": {
                "jsonld_key": "sms:requiresDependency",
                "csv_header": "DependsOn",
                "edge_key": "requiresDependency",
                "jsonld_direction": "out",
                "edge_dir": "out",
                "type": list,
                "edge_rel": True,
                "required_header": True,
            },
            "requiresComponent": {
                "jsonld_key": "sms:requiresComponent",
                "csv_header": "DependsOn Component",
                "edge_key": "requiresComponent",
                "jsonld_direction": "out",
                "edge_dir": "out",
                "type": list,
                "edge_rel": True,
                "required_header": False,
            },
            "required": {
                "jsonld_key": "sms:required",
                "csv_header": "Required",
                "node_label": "required",
                "type": bool,
                "jsonld_default": "sms:false",
                "edge_rel": False,
                "required_header": True,
                "node_attr_dict": {
                    "default": False,
                    "standard": convert_bool_to_str,
                },
            },
            "subClassOf": {
                "jsonld_key": "rdfs:subClassOf",
                "csv_header": "Parent",
                "edge_key": "parentOf",
                "jsonld_direction": "in",
                "edge_dir": "out",
                "jsonld_default": [{"@id": "bts:Thing"}],
                "type": list,
                "edge_rel": True,
                "required_header": True,
            },
            "validationRules": {
                "jsonld_key": "sms:validationRules",
                "csv_header": "Validation Rules",
                "node_label": "validationRules",
                "jsonld_direction": "out",
                "edge_dir": "out",
                "jsonld_default": [],
                "type": list,
                "edge_rel": False,
                "required_header": True,
                "node_attr_dict": {
                    "default": [],
                    "standard": parse_validation_rules,
                },
            },
            "domainIncludes": {
                "jsonld_key": "schema:domainIncludes",
                "csv_header": "Properties",
                "edge_key": "domainValue",
                "jsonld_direction": "out",
                "edge_dir": "in",
                "type": list,
                "edge_rel": True,
                "required_header": False,
            },
            "isPartOf": {
                "jsonld_key": "schema:isPartOf",
                "csv_header": None,
                "node_label": "isPartOf",
                "type": dict,
                "edge_rel": False,
                "required_header": False,
                "node_attr_dict": {
                    "default": {"@id": "http://schema.biothings.io"},
                },
            },
            "id": {
                "jsonld_key": "@id",
                "csv_header": "Source",
                "node_label": "uri",
                "type": str,
                "edge_rel": False,
                "required_header": False,
                "node_attr_dict": {
                    "default": get_label_from_display_name,
                    "standard": get_label_from_display_name,
                },
            },
            "columnType": {
                "jsonld_key": "sms:columnType",
                "csv_header": "ColumnType",
                "node_label": "columnType",
                "type": str,
                "required_header": False,
                "edge_rel": False,
                "node_attr_dict": {"default": None},
                "allowed_values": ALL_COLUMN_TYPE_VALUES,
            },
            "format": {
                "jsonld_key": "sms:format",
                "csv_header": "Format",
                "node_label": "format",
                "type": str,
                "required_header": False,
                "edge_rel": False,
                "node_attr_dict": {"default": None},
                "allowed_values": [member.value for member in JSONSchemaFormat],
            },
            "maximum": {
                "jsonld_key": "sms:maximum",
                "csv_header": "Maximum",
                "node_label": "maximum",
                "type": Union[float, int],
                "required_header": False,
                "edge_rel": False,
                "node_attr_dict": {"default": None},
            },
            "minimum": {
                "jsonld_key": "sms:minimum",
                "csv_header": "Minimum",
                "node_label": "minimum",
                "type": Union[float, int],
                "required_header": False,
                "edge_rel": False,
                "node_attr_dict": {"default": None},
            },
            "pattern": {
                "jsonld_key": "sms:pattern",
                "csv_header": "Pattern",
                "node_label": "pattern",
                "type": str,
                "required_header": False,
                "edge_rel": False,
                "node_attr_dict": {"default": None},
            },
        }

        return map_data_model_relationships

    def define_required_csv_headers(self) -> list:
        """
        Helper function to retrieve required CSV headers, alert if required header was
          not provided.
        Returns:
            required_headers: lst, Required CSV headers.
        """
        required_headers = []
        for key, value in self.relationships_dictionary.items():
            try:
                if value["required_header"]:
                    required_headers.append(value["csv_header"])
            except KeyError:
                print(
                    (
                        "Did not provide a 'required_header' key, value pair for the "
                        f"nested dictionary {key} : {value}"
                    )
                )

        return required_headers

    def retrieve_rel_headers_dict(self, edge: bool) -> dict[str, str]:
        """
        Helper method to retrieve CSV headers for edge and non-edge relationships
          defined by edge_type.

        Args:
            edge, bool: True if looking for edge relationships
        Returns:
            rel_headers_dict: dict, key: csv_header if the key represents an edge relationship.
        """
        rel_headers_dict = {}
        for rel, rel_dict in self.relationships_dictionary.items():
            if "edge_rel" in rel_dict:
                if rel_dict["edge_rel"] and edge:
                    rel_headers_dict.update({rel: rel_dict["csv_header"]})
                elif not rel_dict["edge_rel"] and not edge:
                    rel_headers_dict.update({rel: rel_dict["csv_header"]})
            else:
                raise ValueError(f"Did not provide a 'edge_rel' for relationship {rel}")

        return rel_headers_dict

    def get_relationship_value(
        self, relationship: str, value: str, none_if_missing: bool = False
    ) -> Any:
        """Returns a value from the relationship dictionary

        Args:
            relationship: The name of the relationship, the key in the top level relationship dict
            value: The name of the value to get, the key dict of the relationship itself
            none_if_missing: Determines the behavior when the specified value is not found
              in the relationship dictionary:
                If True returns None
                If False an exception is raised

        Raises:
            ValueError: If the relationship doesn't exist
            ValueError: If the value isn't in the relationship and none_if_missing is False

        Returns:
            The value
        """
        if relationship.strip() not in self.relationships_dictionary:
            msg = (
                f"Relationship: '{relationship}' not in dictionary: "
                f"{list(self.relationships_dictionary.keys())}"
            )
            raise ValueError(msg)
        if value.strip().lower() not in self.relationships_dictionary[relationship]:
            if not none_if_missing:
                msg = (
                    f"Value: '{value}' not in relationship dictionary: "
                    f"{list(self.relationships_dictionary[relationship].keys())}"
                )
                raise ValueError(msg)
            return None
        return self.relationships_dictionary[relationship][value]

    def get_allowed_values(self, relationship: str) -> Optional[list[Any]]:
        """Gets the allowed values for the relationship

        Arguments:
            relationship: The name of the relationship

        Returns:
             A list of allowed values if they exist, otherwise None
        """
        allowed_values = self.get_relationship_value(
            relationship, "allowed_values", none_if_missing=True
        )
        assert isinstance(allowed_values, list) or allowed_values is None
        return allowed_values


class DataModelGraphMeta:  # pylint: disable=too-few-public-methods
    """DataModelGraphMeta"""

    _instances: dict = {}

    def __call__(  # pylint: disable=no-self-argument
        cls, *args: Any, **kwargs: Any
    ) -> Any:
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)  # type: ignore # pylint: disable=no-member
            cls._instances[cls] = instance
        return cls._instances[cls]


class DataModelNodes:
    """Data model Nodes"""

    def __init__(self, attribute_relationships_dict: dict, logger: Logger):
        self.logger = logger
        self.namespaces = {
            "rdf": Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
        }
        self.data_model_relationships = DataModelRelationships()
        self.value_relationships = (
            self.data_model_relationships.retrieve_rel_headers_dict(edge=False)
        )
        self.edge_relationships_dictionary = (
            self.data_model_relationships.retrieve_rel_headers_dict(edge=True)
        )
        self.properties = self.get_data_model_properties(
            attr_rel_dict=attribute_relationships_dict
        )
        # retrieve a list of relationship types that will produce nodes.
        self.node_relationships = list(self.edge_relationships_dictionary.values())

    def gather_nodes(self, attr_info: tuple) -> list:
        """
        Take in a tuple containing attriute name and relationship dictionary,
          and find all nodes defined in attribute information.

        Args:
            attr_info, tuple: (Display Name, Relationships Dictionary portion of
              attribute_relationships dictionary)

        Returns:
            nodes, list: nodes related to the given node (specified in attr_info).
        Note:
            Extracting nodes in this fashion ensures order is preserved.
        """

        # Extract attribute and relationship dictionary
        attribute, relationship = attr_info
        relationships = relationship["Relationships"]

        nodes = []
        if attribute not in nodes:
            nodes.append(attribute)
        for rel in self.node_relationships:
            if rel in relationships.keys():
                nodes.extend([node for node in relationships[rel] if node is not None])
        return nodes

    def gather_all_nodes_in_model(self, attr_rel_dict: dict) -> list:
        """Gather all nodes in the data model, in order.
        Args:
            attr_rel_dict, dict: generated in data_model_parser
                {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
        Returns:
            all_nodes, list: List of all node display names in the data model
              preserving order entered.
        Note:
            Gathering nodes in this fashion ensures order is preserved.
        """
        all_nodes = []
        for attr_info in attr_rel_dict.items():
            nodes = self.gather_nodes(attr_info=attr_info)
            all_nodes.extend(nodes)
        # Remove any duplicates preserving order
        all_nodes = list(dict.fromkeys(all_nodes).keys())
        return all_nodes

    def get_rel_node_dict_info(self, relationship: str) -> Optional[tuple[str, dict]]:
        """For each display name get defaults for nodes.
        Args:
            relationship, str: relationship key to match.
        Returns:
            rel_key, str: relationship node label
            rel_node_dict, dict: node_attr_dict, from relationships dictionary for a
              given relationship
        TODO: Move to data_model_relationships.
        """
        for (
            key,
            value,
        ) in self.data_model_relationships.relationships_dictionary.items():
            if key == relationship:
                if "node_attr_dict" in value:
                    rel_key = value["node_label"]
                    rel_node_dict = value["node_attr_dict"]
                    return rel_key, rel_node_dict
        return None

    def get_data_model_properties(self, attr_rel_dict: dict) -> list:
        """Identify all properties defined in the data model.
        Args:
            attr_rel_dict, dict:
                {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
        Returns:
            properties,list: properties defined in the data model
        """
        properties = []
        for relationships in attr_rel_dict.values():
            if "Properties" in relationships["Relationships"].keys():
                properties.extend(relationships["Relationships"]["Properties"])
        properties = list(set(properties))
        return properties

    def get_entry_type(self, node_display_name: str) -> EntryType:
        """Get the entry type of the node, property or class.

        Args:
            node_display_name (str): display name of target node.

        Returns:
            EntryType: returns 'property' or 'class' based on data model specifications.
        """
        if node_display_name in self.properties:
            entry_type: EntryType = "property"
        else:
            entry_type = "class"
        return entry_type

    def run_rel_functions(
        self,
        rel_func: Callable,
        node_display_name: str = "",
        key: str = "",
        attr_relationships: Optional[dict] = None,
        csv_header: str = "",
        entry_type: EntryType = "class",
        data_model_labels: DisplayLabelType = "class_label",
    ) -> Any:
        """
        This function exists to centralzie handling of functions for filling out node information,
          makes sure all the proper parameters are passed to each function.

        Args:
            rel_func, callable: Function to call to get information to attach to the node
            node_display_name, str: node display name
            key, str: relationship key
            attr_relationships, dict: relationships portion of attributes_relationships dictionary
            csv_header, str: csv header
            entry_type, str: 'class' or 'property' defines how

        Returns:
            Outputs of specified rel_func (relationship function)

        For legacy:
        elif key == 'id' and rel_func == get_label_from_display_name:
            func_output = get_label_from_display_name(
                display_name =node_display_name, entry_type=entry_type
            )
        """
        # pylint: disable=too-many-arguments
        # pylint: disable=too-many-return-statements
        # pylint: disable=comparison-with-callable
        if attr_relationships is None:
            attr_relationships = {}

        if rel_func == get_attribute_display_name_from_label:
            return get_attribute_display_name_from_label(
                node_display_name, attr_relationships
            )

        if rel_func == parse_validation_rules:
            rules = attr_relationships[csv_header]
            if isinstance(rules, (dict, list)):
                return parse_validation_rules(rules, self.logger)

        if rel_func == get_label_from_display_name:
            return get_label_from_display_name(
                display_name=node_display_name,
                entry_type=entry_type,
                logger=self.logger,
                data_model_labels=data_model_labels,
            )

        if rel_func == convert_bool_to_str:
            if isinstance(attr_relationships[csv_header], str):
                if attr_relationships[csv_header].lower() == "true":
                    return True
                if attr_relationships[csv_header].lower() == "false":
                    return False
                return None

            if isinstance(attr_relationships[csv_header], bool):
                return attr_relationships[csv_header]

            return None

        # Raise Error if the rel_func provided is not captured.
        raise ValueError(
            (
                f"The function provided ({rel_func}) to define the relationship {key} "
                "is not captured in the function run_rel_functions, please update."
            )
        )

    def generate_node_dict(
        self,
        node_display_name: str,
        attr_rel_dict: dict,
        data_model_labels: DisplayLabelType = "class_label",
    ) -> dict:
        """Gather information to be attached to each node.

        Note:
            If the default calls function, call that function for the default or alternate
              implementation.
            May need to update this logic for varying function calls. (for example the current
              function takes in the node display name would need to update if new function took
              in something else.)

        Args:
            node_display_name (str): display name for current node
            attr_rel_dict (dict): generated in data_model_parser
              {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
            data_model_labels (DisplayLabelType, optional):str, display_label or class_label.
                display_label, use the display name as a label, if it is valid (contains no
                  blacklisted characters) otherwise will default to schema_label.
                class_label, default, use standard class or property label.

        Returns:
            dict: dictionary of relationship information about the current node
                {'displayName': '', 'label': '', 'comment': 'TBD', 'required': None,
                 'validationRules': [], 'isPartOf': '', 'uri': ''}
        """
        # Strip whitespace from node display name
        node_display_name = node_display_name.strip()

        # Determine if property or class
        entry_type = self.get_entry_type(node_display_name=node_display_name)

        # If the node is an attribute, find its relationships.
        attr_relationships = {}
        if node_display_name in attr_rel_dict.keys():
            attr_relationships = attr_rel_dict[node_display_name]["Relationships"]

        # Initialize node_dict
        node_dict = {}

        # Look through relationship types that represent values (i.e. do not define edges)
        for key, csv_header in self.value_relationships.items():
            # Get key and defalt values current relationship type.
            rel_node = self.get_rel_node_dict_info(key)
            assert rel_node is not None
            rel_key, rel_node_dict = rel_node

            # If we have information to add about this particular node, get it
            if csv_header in attr_relationships.keys():
                # Check if the 'standard' specifies calling a function.
                if "standard" in rel_node_dict.keys() and isfunction(
                    rel_node_dict["standard"]
                ):
                    # Add to node_dict The value comes from the standard function call.
                    node_dict.update(
                        {
                            rel_key: self.run_rel_functions(
                                rel_node_dict["standard"],
                                node_display_name=node_display_name,
                                key=key,
                                attr_relationships=attr_relationships,
                                csv_header=csv_header,
                                entry_type=entry_type,
                                data_model_labels=data_model_labels,
                            )
                        }
                    )
                else:
                    # For standard entries, get information from attr_relationship dictionary
                    node_dict.update({rel_key: attr_relationships[csv_header]})
            # else, add default values
            else:
                # Check if the default specifies calling a function.
                if "default" in rel_node_dict.keys() and isfunction(
                    rel_node_dict["default"]
                ):
                    node_dict.update(
                        {
                            rel_key: self.run_rel_functions(
                                rel_node_dict["default"],
                                node_display_name=node_display_name,
                                key=key,
                                attr_relationships=attr_relationships,
                                csv_header=csv_header,
                                entry_type=entry_type,
                                data_model_labels=data_model_labels,
                            )
                        }
                    )
                else:
                    # Set value to defaults.
                    node_dict.update({rel_key: rel_node_dict["default"]})

        return node_dict

    def generate_node(
        self, graph: MULTI_GRAPH_TYPE, node_dict: dict
    ) -> MULTI_GRAPH_TYPE:
        """Create a node and add it to the networkx multidigraph being built
        Args:
            graph, nx.MultiDigraph: networkx multidigraph object, that is in the process of
              being fully built.
            node_dict, dict: dictionary of relationship information about the current node
        Returns:
            nx.MultiDigraph: networkx multidigraph object, that has had an additional
              node added to it.
        """
        graph.add_node(node_dict["label"], **node_dict)
        return graph

    def edit_node(self) -> None:
        """Stub for future node editor."""
        return


class DataModelEdges:  # pylint: disable=too-few-public-methods
    """Data Model Edges"""

    def __init__(self) -> None:
        self.dmr = DataModelRelationships()
        self.data_model_relationships = self.dmr.relationships_dictionary

    def generate_edge(  # pylint: disable=too-many-arguments
        self,
        node: str,
        all_node_dict: dict[str, dict[str, Any]],
        attr_rel_dict: dict[str, dict[str, Any]],
        edge_relationships: dict[str, str],
        edge_list: list[tuple[str, str, dict[str, Union[str, int]]]],
    ) -> list[tuple[str, str, dict[str, Union[str, int]]]]:
        """

        Generate an edge between a target node and relevant other nodes the data model.
          In short, does this current node belong to a recorded relationship in the attribute,
          relationshps dictionary.
          Go through each attribute and relationship to find where the node may be.

        Args:
            node (str): target node to look for connecting edges
            all_node_dict (dict): a dictionary containing information about all nodes in the model
                key: node display name
                value: node attribute dict, containing attributes to attach to each node.
            attr_rel_dict (dict):
                {Attribute Display Name: {--disallow-untyped-defs
                    Relationships: {
                        CSV Header: Value}
                    }
                }
            edge_relationships (dict): dict, rel_key: csv_header if the key represents a value
              relationship.
            edge_list (list): list of tuples describing the edges and the edge attributes,
              organized as (node_1, node_2, {key:edge_relationship_key, weight:int})
              At this point, the edge list will be in the process of being built.
              Adding edges from list so they will be added properly to the graph without being
              overwritten in the loop, and passing the Graph around more.


        """

        # For each attribute in the model.
        for attribute_display_name, relationship in attr_rel_dict.items():
            # Get the relationships associated with the current attribute
            relationships = relationship["Relationships"]
            # Add edge relationships one at a time
            for rel_key, csv_header in edge_relationships.items():
                # If the attribute has a relationship that matches the current edge being added
                if csv_header in relationships.keys():
                    # If the current node is part of that relationship and is not the current node
                    # Connect node to attribute as an edge.
                    if (
                        node in relationships[csv_header]
                        and node != attribute_display_name
                    ):
                        # Generate weights based on relationship type.
                        # Weights will allow us to preserve the order of entries order in the
                        # data model in later steps.
                        if rel_key == "domainIncludes":
                            # For 'domainIncludes'/properties relationship, users do not explicitly
                            # provide a list order (like for valid values, or dependsOn)
                            # so we pull the order/weight from the order of the attributes.
                            weight = list(attr_rel_dict.keys()).index(
                                attribute_display_name
                            )
                        elif isinstance(relationships[csv_header], list):
                            # For other relationships that pull in lists of values, we can
                            # explicilty pull the weight by their order in the provided list
                            weight = relationships[csv_header].index(node)
                        else:
                            # For single (non list) entries, add weight of 0
                            weight = 0
                        # Get the edge_key for the edge relationship we are adding at this step
                        edge_key = self.data_model_relationships[rel_key]["edge_key"]
                        # Add edges, in a manner that preserves directionality
                        # TODO: rewrite to use edge_dir pylint: disable=fixme
                        if rel_key in ["subClassOf", "domainIncludes"]:
                            edge_list.append(
                                (
                                    all_node_dict[node]["label"],
                                    all_node_dict[attribute_display_name]["label"],
                                    {
                                        "key": edge_key,
                                        "weight": weight,
                                    },
                                )
                            )
                        else:
                            edge_list.append(
                                (
                                    all_node_dict[attribute_display_name]["label"],
                                    all_node_dict[node]["label"],
                                    {"key": edge_key, "weight": weight},
                                )
                            )
                        # Add add rangeIncludes/valid value relationships in reverse as well,
                        # making the attribute the parent of the valid value.
                        if rel_key == "rangeIncludes":
                            edge_list.append(
                                (
                                    all_node_dict[attribute_display_name]["label"],
                                    all_node_dict[node]["label"],
                                    {"key": "parentOf", "weight": weight},
                                )
                            )
        return edge_list


class DataModelGraph:  # pylint: disable=too-few-public-methods
    """
    Generate graph network (networkx) from the attributes and relationships returned
    from the data model parser.

    Create a singleton.
    """

    __metaclass__ = DataModelGraphMeta

    def __init__(
        self,
        attribute_relationships_dict: dict,
        data_model_labels: DisplayLabelType = "class_label",
        logger: Logger = None,
    ) -> None:
        """Load parsed data model.
        Args:
            attributes_relationship_dict, dict: generated in data_model_parser
                {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
            data_model_labels: str, display_label or class_label.
                display_label, use the display name as a label, if it is valid
                (contains no blacklisted characters) otherwise will default to schema_label.
                class_label, default, use standard class or property label.
            logger: Logger instance for logging
        Raises:
            ValueError, attribute_relationship_dict not loaded.
        """
        self.logger = logger
        self.attribute_relationships_dict = attribute_relationships_dict
        self.dmn = DataModelNodes(self.attribute_relationships_dict, logger)
        self.dme = DataModelEdges()
        self.dmr = DataModelRelationships()
        self.data_model_labels = data_model_labels

        if not self.attribute_relationships_dict:
            raise ValueError(
                (
                    "Something has gone wrong, a data model was not loaded into the DataModelGraph "
                    "Class. Please check that your paths are correct"
                )
            )
        self.graph = self.generate_data_model_graph()

    def generate_data_model_graph(self) -> MULTI_GRAPH_TYPE:
        """
        Generate NetworkX Graph from the Relationships/attributes dictionary, the graph is built
          by first adding all nodes to the graph, then connecting nodes by the relationships defined
          in the attributes_relationship dictionary.
        Returns:
            G: nx.MultiDiGraph, networkx graph representation of the data model
        """
        from networkx import MultiDiGraph

        # Get all relationships with edges
        edge_relationships = self.dmr.retrieve_rel_headers_dict(edge=True)

        # Find all nodes
        all_nodes = self.dmn.gather_all_nodes_in_model(
            attr_rel_dict=self.attribute_relationships_dict
        )

        # Instantiate NetworkX MultiDigraph
        graph: MultiDiGraph = MultiDiGraph()

        all_node_dict = {}

        # Fill in MultiDigraph with nodes
        for node in all_nodes:
            # Gather information for each node
            node_dict = self.dmn.generate_node_dict(
                node_display_name=node,
                attr_rel_dict=self.attribute_relationships_dict,
                data_model_labels=self.data_model_labels,
            )

            # Add each node to the all_node_dict to be used for generating edges
            all_node_dict[node] = node_dict

            # Generate node and attach information (attributes) to each node
            graph = self.dmn.generate_node(graph, node_dict)

        edge_list: list[tuple[str, str, dict[str, Union[str, int]]]] = []
        # Connect nodes via edges
        for node in all_nodes:
            # Generate edges
            edge_list_2 = self.dme.generate_edge(
                node,
                all_node_dict,
                self.attribute_relationships_dict,
                edge_relationships,
                edge_list,
            )
            edge_list = edge_list_2.copy()

        # Add edges to the Graph
        for node_1, node_2, edge_dict in edge_list:
            graph.add_edge(
                node_1, node_2, key=edge_dict["key"], weight=edge_dict["weight"]
            )
        return graph


def check_characters_in_node_display_name(
    nodes: list[Node], blacklisted_characters: list[str]
) -> list[str]:
    """Checks each node 'displayName' field has no blacklisted characters

    Args:
        nodes (list[Node]): A list of nodes.
        blacklisted_characters (list[str]): A list of characters not allowed in the node
            display name

    Raises:
        ValueError: Any node is missing the 'displayName' field

    Returns:
        list[str]: A list of warning messages
    """
    warnings: list[str] = []
    for node in nodes:
        node_display_name = node.display_name

        blacklisted_characters_found = [
            character
            for character in node_display_name
            if character in blacklisted_characters
        ]

        if blacklisted_characters_found:
            warnings.append(
                create_blacklisted_characters_error_message(
                    blacklisted_characters_found, node_display_name
                )
            )
    return warnings


def create_blacklisted_characters_error_message(
    blacklisted_characters: list[str], node_name: str
) -> str:
    """Creates am error message for the presence of blacklisted characters

    Args:
        blacklisted_characters (list[str]): A list of characters that
          are unallowed in certain node field names
        node_name (str): The name of the node with the blacklisted characters

    Returns:
        str: _description_
    """
    blacklisted_characters_str = ",".join(blacklisted_characters)
    return (
        f"Node: {node_name} contains a blacklisted character(s): "
        f"{blacklisted_characters_str}, they will be striped if used in "
        "Synapse annotations."
    )


def match_node_names_with_reserved_names(
    node_names: Iterable, reserved_names: Iterable[str]
) -> list[Tuple[str, str]]:
    """Matches node names with those from a reserved list

    Args:
        node_names (Iterable): An iterable of node names
        reserved_names (Iterable[str]): A list of names to match with the node names

    Returns:
        list[Tuple[str, str]]: A List of tuples where the node name matches a reserved name
          The first item is the reserved name
          The second item is the node name
    """
    node_name_strings = [str(name) for name in node_names]
    node_name_product = product(reserved_names, node_name_strings)
    reserved_names_found = [
        node for node in node_name_product if node[0].lower() == node[1].lower()
    ]
    return reserved_names_found


def create_reserve_name_error_messages(
    reserved_names_found: list[Tuple[str, str]]
) -> list[str]:
    """Creates the error messages when a reserved name is used

    Args:
        reserved_names_found (list[Tuple[str, str]]): A list of tuples
          The first item is the reserved name
          The second item is the node name that overlapped with a reserved name

    Returns:
        list[str]: A list of error messages
    """
    return [
        (
            f"Your data model entry name: {node_name} overlaps with the reserved name: "
            f"{reserved_name}. Please change this name in your data model."
        )
        for reserved_name, node_name in reserved_names_found
    ]


def get_node_labels_from(input_dict: dict) -> list:
    """
    Searches dict, for nested dict.
    For each nested dict, if it contains the key "node label" that value is returned.

    Args:
        input_dict (dict): A dictionary with possible nested dictionaries

    Returns:
        list: All values for node labels
    """
    node_fields = []
    for value in input_dict.values():
        if isinstance(value, dict) and "node_label" in value.keys():
            node_fields.append(value["node_label"])
    return node_fields


def get_missing_fields_from(
    nodes: list[Node], required_fields: Iterable
) -> list[Tuple[str, str]]:
    """
    Iterates through each node and checks if it contains each required_field.
    Any missing fields are returned.

    Args:
        nodes (list[Node]): A list of nodes.
        required_fields (Iterable): A Iterable of fields each node should have

    Returns:
        list[Tuple[str, str]]: A list of missing fields.
            The first item in each field is the nodes name, and the second is the missing field.
    """
    missing_fields: list[Tuple[str, str]] = []
    for node in nodes:
        missing_fields.extend(
            [
                (str(node.name), str(field))
                for field in required_fields
                if field not in node.fields.keys()
            ]
        )
    return missing_fields


def create_missing_fields_error_messages(
    missing_fields: list[Tuple[str, str]]
) -> list[str]:
    """Creates the error message for when a node is missing a required field

    Args:
        missing_fields (list[Tuple[str, str]]): A list of tuples of nodes with missing fields
          The first item is the node
          The second item is the missing field

    Returns:
        list[str]: The error message
    """
    errors: list[str] = []
    for missing_field in missing_fields:
        errors.append(
            (
                f"For entry: {missing_field[0]}, "
                f"the required field {missing_field[1]} "
                "is missing in the data model graph, please double check your model and "
                "generate the graph again."
            )
        )
    return errors


class DataModelValidator:  # pylint: disable=too-few-public-methods
    """
    Check for consistency within data model.
    """

    def __init__(
        self,
        graph: MULTI_GRAPH_TYPE,
        logger: Logger,
    ):
        """
        Args:
            graph (nx.MultiDiGraph): Graph representation of the data model.
            logger: Logger instance for logging warnings and errors
        """
        self.logger = logger
        self.graph = graph
        self.node_info = [
            Node(node[0], node[1]) for node in self.graph.nodes(data=True)
        ]
        self.dmr = DataModelRelationships()

    def run_checks(self) -> tuple[list[list[str]], list[list[str]]]:
        """Run all validation checks on the data model graph.

        Returns:
            tuple[list, list]:  Returns a tuple of errors and warnings generated.

        TODO: In future could design a way for groups to customize tests run for their groups,
           run additional tests, or move some to issuing only warnings, vice versa.
        """
        error_checks = [
            self._check_graph_has_required_node_fields(),
            self._check_is_dag(),
            self._check_reserved_names(),
        ]
        warning_checks = [
            self._check_blacklisted_characters(),
        ]
        errors = [error for error in error_checks if error]
        warnings = [warning for warning in warning_checks if warning]
        return errors, warnings

    def _check_graph_has_required_node_fields(self) -> list[str]:
        """Checks that the graph has the required node fields for all nodes.

        Returns:
            list[str]: List of error messages for each missing field.
        """
        required_fields = get_node_labels_from(self.dmr.relationships_dictionary)
        missing_fields = get_missing_fields_from(self.node_info, required_fields)
        return create_missing_fields_error_messages(missing_fields)

    def _run_cycles(self) -> None:
        """run_cycles"""
        from networkx import simple_cycles

        cycles = simple_cycles(self.graph)
        if cycles:  # pylint:disable=using-constant-test
            for cycle in cycles:
                self.logger.warning(  # pylint:disable=logging-fstring-interpolation
                    (
                        f"Schematic requires models be a directed acyclic graph (DAG). Your graph "
                        f"is not a DAG, we found a loop between: {cycle[0]} and {cycle[1]}, "
                        "please remove this loop from your model and submit again."
                    )
                )

    def _check_is_dag(self) -> list[str]:
        """Check that generated graph is a directed acyclic graph

        Returns:
            list[str]:
              List of error messages if graph is not a DAG. List will include a message
                for each cycle found, if not there is a more generic message for the
                graph as a whole.
        """
        from networkx import is_directed_acyclic_graph

        error = []
        if not is_directed_acyclic_graph(self.graph):
            # TODO: Is multiprocessing going to work on all OS combos?
            cycles = multiprocessing.Process(
                target=self._run_cycles,
                name="Get Cycles",
            )
            cycles.start()

            # Give up to 5 seconds to find cycles, if not exit and issue standard error
            time.sleep(5)

            # If thread is active
            if cycles.is_alive():
                # Terminate foo
                cycles.terminate()
                # Cleanup
                cycles.join()

            error.append(
                (
                    "Schematic requires models be a directed acyclic graph (DAG). "
                    "Please inspect your model."
                )
            )

        return error

    def _check_blacklisted_characters(self) -> list[str]:
        """
        We strip these characters in store, so not sure if it matter if we have them now,
         maybe add warning

        Returns:
            list[str]: list of warnings for each node in the graph, that has a Display
              name that contains blacklisted characters.
        """
        return check_characters_in_node_display_name(
            self.node_info, BLACKLISTED_CHARACTERS_NODE_NAMES
        )

    def _check_reserved_names(self) -> list[str]:
        """Identify if any names nodes in the data model graph are the same as reserved name.
        Returns:
            error, list: List of errors for every node in the graph whose name overlaps
              with the reserved names.
        """
        reserved_names_found = match_node_names_with_reserved_names(
            self.graph.nodes, RESERVED_NODE_NAMES
        )
        return create_reserve_name_error_messages(reserved_names_found)


def export_schema(schema: dict, file_path: str, logger: Logger) -> None:
    """Export schema to given filepath.
    Args:
        schema, dict: JSONLD schema
        filepath, str: path to store the schema
    """
    file_path = os.path.expanduser(file_path)
    # Don't create directories if the path looks like a URL
    if not (file_path.startswith("http://") or file_path.startswith("https://")):
        json_schema_dirname = os.path.dirname(file_path)
        if json_schema_dirname != "":
            os.makedirs(json_schema_dirname, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(schema, json_file, sort_keys=True, indent=4, ensure_ascii=False)

    logger.info(f"The Data Model was created and saved to '{file_path}' location.")


def parsed_model_as_dataframe(
    parsed_model: dict[str, dict[str, Any]]
) -> DATA_FRAME_TYPE:
    """Convert parsed model dictionary to an unpacked pandas DataFrame.
    Args:
        parsed_model: dict, parsed data model dictionary.
    Returns:
        pd.DataFrame, DataFrame representation of the parsed model.
    """
    test_import_pandas()
    from pandas import DataFrame

    # Convert the parsed model dictionary to a DataFrame
    unpacked_model_dict = {}

    for top_key, nested_dict in parsed_model.items():
        for nested_key, value in nested_dict.items():
            unpacked_model_dict[top_key, nested_key] = value

    model_dataframe = DataFrame.from_dict(
        unpacked_model_dict,
        orient="index",
    ).reset_index(drop=True)

    return model_dataframe


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def filter_unused_inputted_rules(
    inputted_rules: list[str], logger: Logger
) -> list[str]:
    """Filters a list of validation rules for only those used to create JSON Schemas

    Arguments:
        inputted_rules: A list of validation rules

    Raises:
        warning: When any of the inputted rules are not used for JSON Schema creation

    Returns:
        A filtered list of validation rules
    """
    unused_rules = [
        rule
        for rule in inputted_rules
        if _get_name_from_inputted_rule(rule)
        not in [e.value for e in ValidationRuleName]
    ]
    if unused_rules:
        msg = f"These validation rules will be ignored in creating the JSON Schema: {unused_rules}"
        logger.warning(msg)

    return [
        rule
        for rule in inputted_rules
        if _get_name_from_inputted_rule(rule) in [e.value for e in ValidationRuleName]
    ]


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def check_for_duplicate_inputted_rules(inputted_rules: list[str]) -> None:
    """Checks that there are no rules with duplicate names

    Arguments:
        inputted_rules: A list of validation rules

    Raises:
        ValueError: If there are multiple rules with the same name
    """
    rule_names = get_names_from_inputted_rules(inputted_rules)
    if sorted(rule_names) != sorted(list(set(rule_names))):
        raise ValueError(f"Validation Rules contains duplicates: {inputted_rules}")


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def check_for_conflicting_inputted_rules(inputted_rules: list[str]) -> None:
    """Checks that each rule has no conflicts with any other rule

    Arguments:
        inputted_rules: A list of validation rules

    Raises:
        ValueError: If a rule is in conflict with any other rule
    """
    rule_names = get_names_from_inputted_rules(inputted_rules)
    rules: list[ValidationRule] = _get_rules_by_names(rule_names)
    for rule in rules:
        incompatible_rule_names = [rule.value for rule in rule.incompatible_rules]
        conflicting_rule_names = sorted(
            list(set(rule_names).intersection(incompatible_rule_names))
        )
        if conflicting_rule_names:
            msg = (
                f"Validation rule: {rule.name.value} "
                f"has conflicting rules: {conflicting_rule_names}"
            )
            raise ValueError(msg)


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def get_rule_from_inputted_rules(
    rule_name: ValidationRuleName, inputted_rules: list[str]
) -> Optional[str]:
    """Returns a rule from a list of rules

    Arguments:
        rule_name: A ValidationRuleName
        inputted_rules: A list of validation rules

    Raises:
        ValueError: If there are multiple of the rule in the list

    Returns:
        The rule if one is found, otherwise None is returned
    """
    inputted_rules = [
        rule for rule in inputted_rules if rule.startswith(rule_name.value)
    ]
    if len(inputted_rules) > 1:
        raise ValueError(f"Found duplicates of rule in rules: {inputted_rules}")
    if len(inputted_rules) == 0:
        return None
    return inputted_rules[0]


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def get_in_range_parameters_from_inputted_rule(
    inputted_rule: str,
) -> tuple[Optional[float], Optional[float]]:
    """
    Returns the min and max from an inRange rule if they exist

    Arguments:
        inputted_rule: The inRange rule

    Returns:
        The min and max from the rule
    """
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    parameters = _get_parameters_from_inputted_rule(inputted_rule)
    if parameters:
        if (
            "minimum" in parameters
            and parameters["minimum"] is not None
            and parameters["minimum"].isnumeric()
        ):
            minimum = float(parameters["minimum"])
        if (
            "maximum" in parameters
            and parameters["maximum"] is not None
            and parameters["maximum"].isnumeric()
        ):
            maximum = float(parameters["maximum"])
    return (minimum, maximum)


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def get_regex_parameters_from_inputted_rule(
    inputted_rule: str,
) -> Optional[str]:
    """
    Gets the pattern from the regex rule

    Arguments:
        inputted_rule: The full regex rule

    Returns:
        If the module parameter is search or match, and the pattern parameter exists
          the pattern is returned
        Otherwise None
    """
    module: Optional[str] = None
    pattern: Optional[str] = None
    parameters = _get_parameters_from_inputted_rule(inputted_rule)
    if parameters:
        if "module" in parameters:
            module = parameters["module"]
        if "pattern" in parameters:
            pattern = parameters["pattern"]
    if module is None or pattern is None:
        return None
    # Do not translate other modules
    if module not in [item.value for item in RegexModule]:
        return None
    # Match is just search but only at the beginning of the string
    if module == RegexModule.MATCH.value and not pattern.startswith("^"):
        return f"^{pattern}"
    return pattern


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def get_validation_rule_names_from_inputted_rules(
    inputted_rules: list[str],
) -> list[ValidationRuleName]:
    """Gets a list of ValidationRuleNames from a list of inputted validation rules

    Arguments:
        inputted_rules: A list of inputted validation rules from a data model

    Returns:
        A list of ValidationRuleNames
    """
    rule_names = get_names_from_inputted_rules(inputted_rules)
    rules = _get_rules_by_names(rule_names)
    return [rule.name for rule in rules]


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def get_names_from_inputted_rules(inputted_rules: list[str]) -> list[str]:
    """Gets the names from a list of inputted rules

    Arguments:
        inputted_rules: A list of inputted validation rules from a data model

    Returns:
        The names of the inputted rules
    """
    return [_get_name_from_inputted_rule(rule) for rule in inputted_rules]


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def _get_parameters_from_inputted_rule(inputted_rule: str) -> Optional[dict[str, str]]:
    """Creates a dictionary of parameters and values from an input rule string

    Arguments:
        inputted_rule: An inputted validation rule from a data model

    Returns:
        If the rule exists, a dictionary where
          the keys are the rule parameters
          the values are the input rule parameter values
        Else None
    """
    rule_name = _get_name_from_inputted_rule(inputted_rule)
    rule_values = inputted_rule.split(" ")[1:]
    rule = _VALIDATION_RULES.get(rule_name)
    if rule and rule.parameters:
        return dict(zip(rule.parameters, rule_values))
    return None


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def _get_name_from_inputted_rule(inputted_rule: str) -> str:
    """Gets the name from an inputted rule

    Arguments:
        inputted_rule: An inputted validation rule from a data model

    Returns:
        The name of the inputted rule
    """
    return inputted_rule.split(" ")[0]


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def _get_rules_by_names(names: list[str]) -> list[ValidationRule]:
    """Gets a list of ValidationRules by name if they exist

    Arguments:
        names: A list of names of ValidationRules

    Raises:
        ValueError: If any of the input names don't correspond to actual rules

    Returns:
        A list of ValidationRules
    """
    rule_dict = {name: _VALIDATION_RULES.get(name) for name in names}
    invalid_rule_names = [
        rule_name for (rule_name, rule) in rule_dict.items() if rule is None
    ]
    if invalid_rule_names:
        raise ValueError("Some input rule names are invalid:", invalid_rule_names)
    return [rule for rule in rule_dict.values() if rule is not None]


@deprecated(
    version="4.11.0",
    reason="This function is going to be deprecated. Use of validation rules will be removed in the future.",
)
def _get_validation_rule_based_fields(
    validation_rules: list[str],
    explicit_is_array: Optional[bool],
    explicit_format: Optional[JSONSchemaFormat],
    name: str,
    column_type: Optional[ColumnType],
    logger: Logger,
) -> tuple[
    bool,
    Optional[JSONSchemaFormat],
    Optional[float],
    Optional[float],
    Optional[str],
]:
    """
    Gets the fields for the Node class that are based on the validation rules

    JSON Schema docs:

    Array: https://json-schema.org/understanding-json-schema/reference/array
    Format: https://json-schema.org/understanding-json-schema/reference/type#format
    Pattern: https://json-schema.org/understanding-json-schema/reference/string#regexp
    Min/max: https://json-schema.org/understanding-json-schema/reference/numeric#range

    Arguments:
        validation_rules: A list of input validation rules
        explicit_is_array:
          True: If the type is set explicitly with a list-type
          False: If the type is set explicitly with a non list-type
          None: If the type was not set explicitly
        name: The name of the node the validation rules belong to
        column_type: The type of this node if set explicitly
        logger: A logger for handling warnings

    Raises:
       ValueError: When an explicit JSON Schema type is given, but the implicit type is different
       Warning: When no explicit JSON Schema type is given,
         and an implicit type is derived from  the validation rules

    Returns:
        A tuple containing fields for a Node object:
        - js_is_array: Whether or not the Node should be an array in JSON Schema
        - js_format: The JSON Schema format
        - js_minimum: If the type is numeric the JSON Schema minimum
        - js_maximum: If the type is numeric the JSON Schema maximum
        - js_pattern: If the type is string the JSON Schema pattern
    """
    js_is_array = False
    js_format = explicit_format
    js_minimum = None
    js_maximum = None
    js_pattern = None

    # If an array type is explicitly set then is_array can be set to True
    if explicit_is_array is not None:
        js_is_array = explicit_is_array

    if validation_rules:
        validation_rules = filter_unused_inputted_rules(
            inputted_rules=validation_rules, logger=logger
        )
        validation_rule_name_strings = get_names_from_inputted_rules(validation_rules)
        check_for_duplicate_inputted_rules(validation_rule_name_strings)
        check_for_conflicting_inputted_rules(validation_rule_name_strings)
        validation_rule_names = get_validation_rule_names_from_inputted_rules(
            validation_rules
        )

        # list validation rule is been deprecated for use in deciding type
        # TODO: Sunset both if blocks below: https://sagebionetworks.jira.com/browse/SYNPY-1692

        implicit_is_array = ValidationRuleName.LIST in validation_rule_names
        if explicit_is_array is None:
            # If an array type is not explicitly set then is_array can be set by using
            # whether or not a list validation rule is present.
            # Since this is deprecated behavior a warning should be given.
            js_is_array = implicit_is_array
            if implicit_is_array:
                msg = (
                    f"A list validation rule is set for property: {name}, "
                    f"but columnType is not a list type (current value: {column_type}). "
                    "To properly define an array property, set columnType to a list type "
                    "(e.g., 'string_list', 'integer_list', 'boolean_list') "
                    "instead of using the list validation rule."
                    "This behavior is deprecated and list validation rules will no longer "
                    "be used in the future.."
                )
                logger.warning(msg)
            else:
                msg = (
                    f"No columnType is set for property: {name}. "
                    "Please set the columnType."
                )
                logger.warning(msg)
        if explicit_is_array != implicit_is_array:
            # If an array type is explicitly but it is the opposite of whether or not a
            # list validation rule is present, then the user should be warned of the mismatch.
            if explicit_is_array:
                msg = (
                    f"For property: {name}, the columnType is a list-type: {column_type} "
                    "but no list validation rule is present. "
                    "The columnType will be used to set type."
                )
                logger.warning(msg)
            else:
                msg = (
                    f"For property: {name}, the columnType is not a list-type: {column_type} "
                    "but a list validation rule is present. "
                    "The columnType will be used to set type."
                )
                logger.warning(msg)

        # url and date rules are deprecated for adding format keyword
        # TODO: remove the if/else block below
        # https://sagebionetworks.jira.com/browse/SYNPY-1685

        if explicit_format:
            if (
                ValidationRuleName.DATE in validation_rule_names
                and explicit_format == JSONSchemaFormat.URI
            ):
                msg = (
                    f"For property: {name}, the format is uri, "
                    "but the validation rule date is present. "
                    "The format will be set to uri."
                )
                logger.warning(msg)
            elif (
                ValidationRuleName.URL in validation_rule_names
                and explicit_format == JSONSchemaFormat.DATE
            ):
                msg = (
                    f"For property: {name}, the format is date, "
                    "but the validation rule url is present. "
                    "The format will be set to date."
                )
                logger.warning(msg)

        else:
            if ValidationRuleName.URL in validation_rule_names:
                js_format = JSONSchemaFormat.URI
                msg = (
                    f"A url validation rule is set for property: {name}, but the format is not set. "
                    "The format will be set to uri, but this behavior is deprecated and validation "
                    "rules will no longer be used in the future."
                    "Please explicitly set the format to uri in the data model."
                )
                logger.warning(msg)
            elif ValidationRuleName.DATE in validation_rule_names:
                js_format = JSONSchemaFormat.DATE
                msg = (
                    f"A date validation rule is set for property: {name}, but the format is not set. "
                    "The format will be set to date, but this behavior is deprecated and validation "
                    "rules will no longer be used in the future."
                    "Please explicitly set the format to uri in the data model."
                )
                logger.warning(msg)

        in_range_rule = get_rule_from_inputted_rules(
            ValidationRuleName.IN_RANGE, validation_rules
        )
        if in_range_rule:
            js_minimum, js_maximum = get_in_range_parameters_from_inputted_rule(
                in_range_rule
            )
            msg = (
                f"An inRange validation rule is set for property: {name}, "
                "setting minimum and maximum values accordingly. "
                "This behavior is deprecated and validation rules will no longer "
                "be used in the future. Please use Minimum and Maximum columns in the data model instead. To use minimum and/or maximum values, you must set columnType to one of: 'integer', 'number', or 'integer_list'."
            )
            logger.warning(msg)

        regex_rule = get_rule_from_inputted_rules(
            ValidationRuleName.REGEX, validation_rules
        )
        if regex_rule:
            js_pattern = get_regex_parameters_from_inputted_rule(regex_rule)

    return (
        js_is_array,
        js_format,
        js_minimum,
        js_maximum,
        js_pattern,
    )


@dataclass
class TraversalNode:  # pylint: disable=too-many-instance-attributes
    """
    A Dataclass representing data about a node in a data model in graph form
    A DataModelGraphExplorer is used to infer most of the fields from the name of the node

    Attributes:
        name: The name of the node
        source_node: The name of the node where the graph traversal started
        dmge: A DataModelGraphExplorer with the data model loaded
        display_name: The display name of the node
        valid_values: The valid values of the node if any
        valid_value_display_names: The display names of the valid values of the node if any
        is_required: Whether or not this node is required
        dependencies: This nodes dependencies
        description: This nodes description, gotten from the comment in the data model
        is_array: Whether or not the property is an array (inferred from validation_rules)
        type: The type of the property (set by ColumnType in the data model)
        format: The format of the property (inferred from validation_rules)
        minimum: The minimum value of the property (if numeric) (inferred from validation_rules)
        maximum: The maximum value of the property (if numeric) (inferred from validation_rules)
        pattern: The regex pattern of the property (inferred from validation_rules)
    """

    name: str
    source_node: str
    dmge: DataModelGraphExplorer
    logger: Logger
    display_name: str = field(init=False)
    valid_values: list[str] = field(init=False)
    valid_value_display_names: list[str] = field(init=False)
    is_required: bool = field(init=False)
    dependencies: list[str] = field(init=False)
    description: str = field(init=False)
    is_array: bool = field(init=False)
    type: Optional[ColumnType] = field(init=False)
    format: Optional[JSONSchemaFormat] = field(init=False)
    minimum: Optional[float] = field(init=False)
    maximum: Optional[float] = field(init=False)
    pattern: Optional[str] = field(init=False)

    def __post_init__(self) -> None:
        """
        Uses the dmge to fill in most of the fields of the dataclass
        """
        self.display_name = self.dmge.get_nodes_display_names([self.name])[0]
        self.valid_values = sorted(self.dmge.get_node_range(node_label=self.name))
        self.valid_value_display_names = sorted(
            self.dmge.get_node_range(node_label=self.name, display_names=True)
        )
        validation_rules = self.dmge.get_component_node_validation_rules(
            manifest_component=self.source_node, node_display_name=self.display_name
        )
        self.is_required = self.dmge.get_component_node_required(
            manifest_component=self.source_node,
            node_validation_rules=validation_rules,
            node_display_name=self.display_name,
        )
        self.dependencies = sorted(
            self.dmge.get_node_dependencies(
                self.name, display_names=False, schema_ordered=False
            )
        )
        self.description = self.dmge.get_node_comment(
            node_display_name=self.display_name
        )
        column_type = self.dmge.get_node_column_type(
            node_display_name=self.display_name
        )
        maximum = self.dmge.get_node_maximum_minimum_value(
            relationship_value="maximum", node_display_name=self.display_name
        )
        minimum = self.dmge.get_node_maximum_minimum_value(
            relationship_value="minimum", node_display_name=self.display_name
        )
        pattern = self.dmge.get_node_column_pattern(node_display_name=self.display_name)
        format = self.dmge.get_node_format(node_display_name=self.display_name)

        self.type, explicit_is_array = self._determine_type_and_array(column_type)

        self._validate_column_type_compatibility(
            maximum=maximum, minimum=minimum, pattern=pattern, format=format
        )

        # TODO: refactor to not use _get_validation_rule_based_fields https://sagebionetworks.jira.com/browse/SYNPY-1724
        (
            self.is_array,
            self.format,
            implicit_minimum,
            implicit_maximum,
            implicit_pattern,
        ) = _get_validation_rule_based_fields(
            validation_rules=validation_rules,
            explicit_is_array=explicit_is_array,
            explicit_format=format,
            name=self.name,
            column_type=self.type,
            logger=self.logger,
        )

        # Priority: explicit values from data model take precedence over validation rules
        # Only use validation rule values if explicit values are not set
        self.minimum = minimum if minimum is not None else implicit_minimum
        self.maximum = maximum if maximum is not None else implicit_maximum

        self.pattern = pattern if pattern else implicit_pattern

        if implicit_pattern and not pattern:
            msg = (
                f"A regex validation rule is set for property: {self.name}, but the pattern is not set in the data model. "
                f"The regex pattern will be set to {self.pattern}, but the regex rule is deprecated and validation "
                "rules will no longer be used in the future. "
                "Please explicitly set the regex pattern in the 'Pattern' column in the data model."
            )
            self.logger.warning(msg)

        if self.pattern:
            try:
                re.compile(self.pattern)
            except re.error as e:
                raise SyntaxError(
                    f"The regex pattern '{self.pattern}' for property '{self.name}' is invalid."
                ) from e

    def _determine_type_and_array(
        self, column_type: Optional[ColumnType]
    ) -> tuple[Optional[AtomicColumnType], Optional[bool]]:
        """Determine the JSON Schema type and array from columnType

        Args:
            column_type: The columnType from the data model

        Returns:
            Tuple of (type, explicit_is_array)
        """
        if isinstance(column_type, AtomicColumnType):
            return column_type, False
        elif isinstance(column_type, ListColumnType):
            return LIST_TYPE_DICT[column_type], True
        else:
            return None, None

    def _validate_column_type_compatibility(
        self,
        maximum: Union[int, float, None],
        minimum: Union[int, float, None],
        pattern: Optional[str] = None,
        format: Optional[JSONSchemaFormat] = None,
    ) -> None:
        """Validate that columnType is compatible with JSONSchema constraints.


        Arguments:
            maximum: The maximum value constraint from the data model.
            minimum: The minimum value constraint from the data model.
            pattern: The regex pattern constraint from the data model.
            format: The format constraint from the data model.

        Raises:
            ValueError: If a constraint is set, but the columnType is incompatible with that constraint.

        Returns:
            None: This method performs validation only and doesn't return a value.
                It raises ValueError if validation fails.
        """
        keyword_types_dict = {
            "pattern": {
                "value": pattern,
                "types": [
                    AtomicColumnType.STRING.value,
                    ListColumnType.STRING_LIST.value,
                ],
            },
            "format": {
                "value": format.value if format else None,
                "types": [
                    AtomicColumnType.STRING.value,
                    ListColumnType.STRING_LIST.value,
                ],
            },
            "minimum": {
                "value": minimum,
                "types": [
                    AtomicColumnType.NUMBER.value,
                    AtomicColumnType.INTEGER.value,
                    ListColumnType.INTEGER_LIST.value,
                ],
            },
            "maximum": {
                "value": maximum,
                "types": [
                    AtomicColumnType.NUMBER.value,
                    AtomicColumnType.INTEGER.value,
                    ListColumnType.INTEGER_LIST.value,
                ],
            },
        }

        for keyword, keyword_dict in keyword_types_dict.items():
            if (
                keyword_dict["value"] is not None
                and self.type.value not in keyword_dict["types"]
            ):
                types = keyword_dict["types"]
                msg = (
                    f"For attribute '{self.display_name}': columnType is '{self.type.value}' "
                    f"but {keyword} constraint (value: {keyword_dict['value']}) "
                    f"is specified. Please set columnType to one of: {types}."
                )
                raise ValueError(msg)


@dataclass
class GraphTraversalState:  # pylint: disable=too-many-instance-attributes
    """
    This is a helper class for create_json_schema. It keeps track of the state as the function
    traverses a graph made from a data model.

    Attributes:
        dmge: A DataModelGraphExplorer for the graph
        source_node: The name of the node where the graph traversal started
        current_node: The node that is being processed
        _root_dependencies: The nodes the source node depends on
        _nodes_to_process: The nodes that are left to be processed
        _processed_nodes: The nodes that have already been processed
        _reverse_dependencies:
            Some nodes will have reverse dependencies (nodes that depend on them)
            This is a mapping: {"node_name" : [reverse_dependencies]}
        _valid_values_map:
            Some nodes will have valid_values (enums)
            This is a mapping {"valid_value" : [nodes_that_have_valid_value]}
    """

    dmge: DataModelGraphExplorer
    source_node: str
    logger: Logger
    current_node: Optional[Node] = field(init=False)
    _root_dependencies: list[str] = field(init=False)
    _nodes_to_process: list[str] = field(init=False)
    _processed_nodes: list[str] = field(init=False)
    _reverse_dependencies: dict[str, list[str]] = field(init=False)
    _valid_values_map: dict[str, list[str]] = field(init=False)

    def __post_init__(self) -> None:
        """
        The first nodes to process are the root dependencies.
        This sets the current node as the first node in root dependencies.
        """
        self.current_node = None
        self._processed_nodes = []
        self._reverse_dependencies = {}
        self._valid_values_map = {}
        root_dependencies = sorted(
            self.dmge.get_node_dependencies(
                self.source_node, display_names=False, schema_ordered=False
            )
        )
        if not root_dependencies:
            raise ValueError(
                f"'{self.source_node}' is not a valid datatype in the data model."
            )
        self._root_dependencies = root_dependencies
        self._nodes_to_process = self._root_dependencies.copy()
        self.move_to_next_node()

    def move_to_next_node(self) -> None:
        """Removes the first node in nodes to process and sets it as current node"""
        if self._nodes_to_process:
            node_name = self._nodes_to_process.pop(0)
            self.current_node = TraversalNode(
                name=node_name,
                dmge=self.dmge,
                source_node=self.source_node,
                logger=self.logger,
            )
            self._update_valid_values_map(
                self.current_node.name, self.current_node.valid_values
            )
            self._update_reverse_dependencies(
                self.current_node.name,
                self.current_node.dependencies,
            )
            self._update_nodes_to_process(sorted(self.current_node.valid_values))
            self._update_nodes_to_process(sorted(self.current_node.dependencies))
        else:
            self.current_node = None

    def are_nodes_remaining(self) -> bool:
        """
        Determines if there are any nodes left to process

        Returns:
            Whether or not there are any nodes left to process
        """
        return self.current_node is not None

    def is_current_node_processed(self) -> bool:
        """
        Determines if  the current node has been processed yet

        Raises:
            ValueError: If there is no current node

        Returns:
            Whether or not the current node has been processed yet
        """
        if self.current_node is None:
            raise ValueError("Current node is None")
        return self.current_node.name in self._processed_nodes

    def is_current_node_a_property(self) -> bool:
        """
        Determines if the current node should be written as a property

        Raises:
            ValueError: If there is no current node

        Returns:
            Whether or not the current node should be written as a property
        """
        if self.current_node is None:
            raise ValueError("Current node is None")

        return any(
            [
                self.current_node.name in self._reverse_dependencies,
                self.current_node.is_required,
                self.current_node.name in self._root_dependencies,
            ]
        )

    def is_current_node_in_reverse_dependencies(self) -> bool:
        """
        Determines if the current node is in the reverse dependencies

        Raises:
            ValueError: If there is no current node

        Returns:
            Whether or not the current node is in the reverse dependencies
        """
        if self.current_node is None:
            raise ValueError("Current node is None")
        return self.current_node.name in self._reverse_dependencies

    def update_processed_nodes_with_current_node(self) -> None:
        """
        Adds the current node to the list of processed nodes

        Raises:
            ValueError: If there is no current node
        """
        if self.current_node is None:
            raise ValueError("Current node is None")
        self._processed_nodes.append(self.current_node.name)

    def get_conditional_properties(
        self, use_node_display_names: bool = True
    ) -> list[tuple[str, str]]:
        """Returns the conditional dependencies for the current node

        Raises:
            ValueError: If there is no current node

        Arguments:
            use_node_display_names: If True the the attributes in the
              conditional dependencies are return with their display names

        Returns:
            The watched_property, and the value for it that triggers the condition
        """
        if self.current_node is None:
            raise ValueError("Current node is None")
        conditional_properties: list[tuple[str, str]] = []
        for value in self._reverse_dependencies[self.current_node.name]:
            if value in self._valid_values_map:
                properties = sorted(self._valid_values_map[value])
                for watched_property in properties:
                    if use_node_display_names:
                        watched_property = self.dmge.get_nodes_display_names(
                            [watched_property]
                        )[0]
                        value = self.dmge.get_nodes_display_names([value])[0]
                    conditional_properties.append((watched_property, value))
        return conditional_properties

    def _update_valid_values_map(
        self, node_display_name: str, valid_values_display_names: list[str]
    ) -> None:
        """Updates the valid_values map

        Arguments:
            node_display_name: The display name of the node
            valid_values_display_names: The display names of the the nodes valid values
        """
        for node in valid_values_display_names:
            if node not in self._valid_values_map:
                self._valid_values_map[node] = []
            self._valid_values_map[node].append(node_display_name)

    def _update_reverse_dependencies(
        self, node_display_name: str, node_dependencies_display_names: list[str]
    ) -> None:
        """Updates the reverse dependencies

        Arguments:
            node_display_name: The display name of the node
            node_dependencies_display_names: the display names of the reverse dependencies
        """
        for dep in node_dependencies_display_names:
            if dep not in self._reverse_dependencies:
                self._reverse_dependencies[dep] = []
            self._reverse_dependencies[dep].append(node_display_name)

    def _update_nodes_to_process(self, nodes: list[str]) -> None:
        """Updates the nodes to process with the input nodes

        Arguments:
            nodes: Nodes to add
        """
        self._nodes_to_process += nodes


@dataclass
class JSONSchema:  # pylint: disable=too-many-instance-attributes
    """
    A dataclass representing a JSON Schema.
    Each attribute represents a keyword in a JSON Schema.

    Attributes:
        schema_id: A URI for the schema.
        title: An optional title for this schema.
        schema: Specifies which draft of the JSON Schema standard the schema adheres to.
        type: The datatype of the schema. This will always be "object".
        description: An optional description of the object described by this schema.
        properties: A list of property schemas.
        required: A list of properties required by the schema.
        all_of: A list of conditions the schema must meet. This should be removed if empty.
    """

    schema_id: str = ""
    title: str = ""
    schema: str = "http://json-schema.org/draft-07/schema#"
    type: str = "object"
    description: str = "TBD"
    properties: dict[str, Property] = field(default_factory=dict)
    required: list[str] = field(default_factory=list)
    all_of: list[AllOf] = field(default_factory=list)

    def as_json_schema_dict(
        self,
    ) -> dict[str, Union[str, dict[str, Property], list[str], list[AllOf]]]:
        """
        Returns class as a JSON Schema dictionary, with proper keywords

        Returns:
            The dataclass as a dict.
        """
        json_schema_dict = asdict(self)
        keywords_to_change = {
            "schema_id": "$id",
            "schema": "$schema",
            "all_of": "allOf",
        }
        for old_word, new_word in keywords_to_change.items():
            json_schema_dict[new_word] = json_schema_dict.pop(old_word)
        if not self.all_of:
            json_schema_dict.pop("allOf")
        return json_schema_dict

    def add_required_property(self, name: str) -> None:
        """
        Adds a property to the required list

        Arguments:
            name: The name of the property
        """
        self.required.append(name)

    def add_to_all_of_list(self, item: AllOf) -> None:
        """
        Adds a property to the all_of list

        Arguments:
            item: The item to add to the all_of list
        """
        self.all_of.append(item)

    def update_property(self, property_dict: dict[str, Property]) -> None:
        """
        Updates the property dict

        Raises:
            ValueError: If the property dict has more than one key
            ValueError: If the property dict is empty
            ValueError: if the property dict key match a property that already exists

        Arguments:
            property_dict: The property dict to add to the properties dict
        """
        keys = list(property_dict.keys())
        if len(keys) > 1:
            raise ValueError(
                f"Attempting to add property dict with more than one key: {property_dict}"
            )
        if len(keys) == 0:
            raise ValueError(f"Attempting to add empty property dict: {property_dict}")
        if keys[0] in self.properties:
            raise ValueError(
                f"Attempting to add property that already exists: {property_dict}"
            )
        self.properties.update(property_dict)


def _set_conditional_dependencies(
    json_schema: JSONSchema,
    graph_state: GraphTraversalState,
    use_property_display_names: bool = True,
) -> None:
    """
    This sets conditional requirements in the "allOf" keyword.
    This is used when certain properties are required depending on the value of another property.

    For example:
      In the example data model the Patient component has the Diagnosis attribute.
      The Diagnosis attribute has valid values of ["Healthy", "Cancer"].
      The Cancer valid value is also an attribute that dependsOn on the
        attributes Cancer Type and Family History
      Cancer Type and Family History are attributes with valid values.
      Therefore: When Diagnosis == "Cancer", Cancer Type and Family History should become required

    Example conditional schema:
        "if":{
            "properties":{
               "Diagnosis":{
                  "enum":[
                     "Cancer"
                  ]
               }
            }
         },
         "then":{
            "properties":{
               "FamilyHistory":{
                  "not":{
                     "type":"null"
                  }
               }
            },
            "required":[
               "FamilyHistory"
            ]
         }

    Arguments:
        json_schema: The JSON Scheme where the node might be set as a property
        graph_state: The instance tracking the current state of the graph
        use_property_display_names: If True, the properties in the JSONSchema
          will be written using node display names
    """
    if graph_state.current_node is None:
        raise ValueError("Node Processor contains no node.")

    if use_property_display_names:
        node_name = graph_state.current_node.display_name
    else:
        node_name = graph_state.current_node.name

    conditional_properties = graph_state.get_conditional_properties(
        use_property_display_names
    )
    for prop in conditional_properties:
        attribute, value = prop
        conditional_schema = {
            "if": {"properties": {attribute: {"enum": [value]}}},
            "then": {
                "properties": {node_name: {"not": {"type": "null"}}},
                "required": [node_name],
            },
        }
        json_schema.add_to_all_of_list(conditional_schema)


def _create_enum_array_property(
    node: TraversalNode, use_valid_value_display_names: bool = True
) -> Property:
    """
    Creates a JSON Schema property array with enum items

    Example:
        {
            "type": "array",
            "title": "array",
            "items": {"enum": ["enum1"], "type": "string"},
        },


    Arguments:
        node: The node to make the property of
        use_valid_value_display_names: If True, the valid_values in the JSONSchema
          will be written using node display names

    Returns:
        JSON object
    """
    if use_valid_value_display_names:
        valid_values = node.valid_value_display_names
    else:
        valid_values = node.valid_values
    items: Items = {"enum": valid_values, "type": "string"}
    array_property = {
        "type": "array",
        "title": "array",
        "items": items,
    }
    return array_property


def _create_array_property(node: TraversalNode) -> Property:
    """
    Creates a JSON Schema property array

    Example:
        {
            "type": "array",
            "title": "array",
            "items": {"type": "integer", "minimum": 0, "maximum": 1},
        }

    Arguments:
        node: The node to make the property of

    Returns:
        JSON object
    """

    items: Items = {}
    if node.type:
        items["type"] = node.type.value
        _set_type_specific_keywords(items, node)

    array_type_dict: TypeDict = {"type": "array", "title": "array"}

    if items:
        array_type_dict["items"] = items

    return array_type_dict


def _create_enum_property(
    node: TraversalNode, use_valid_value_display_names: bool = True
) -> Property:
    """
    Creates a JSON Schema property enum

    Example:
        {
            "enum": ["enum1", "enum2"],
            "title": "enum"
        }

    Arguments:
        node: The node to make the property of

    Returns:
        JSON object
    """
    if use_valid_value_display_names:
        valid_values = node.valid_value_display_names
    else:
        valid_values = node.valid_values

    enum_property: Property = {}
    enum_property["enum"] = valid_values
    enum_property["title"] = "enum"

    return enum_property


def _create_simple_property(node: TraversalNode) -> Property:
    """
    Creates a JSON Schema property

    Example:
        {
            "title": "String",
            "type": "string"
        }

    Arguments:
        node: The node to make the property of

    Returns:
        JSON object
    """
    prop: Property = {}

    if node.type:
        prop["type"] = node.type.value
    elif node.is_required:
        prop["not"] = {"type": "null"}

    _set_type_specific_keywords(prop, node)

    return prop


def _set_type_specific_keywords(schema: dict[str, Any], node: TraversalNode) -> None:
    """Sets JSON Schema keywords that are allowed if type has been set

    Arguments:
        schema: The schema to set keywords on
        node (Node): The node the corresponds to the property which is being set in the JSON Schema
    """
    for attr in ["minimum", "maximum", "pattern"]:
        value = getattr(node, attr)
        if value is not None:
            schema[attr] = value

    if node.format is not None:
        schema["format"] = node.format.value


def _set_property(
    json_schema: JSONSchema,
    node: TraversalNode,
    use_property_display_names: bool = True,
    use_valid_value_display_names: bool = True,
) -> None:
    """
    Sets a property in the JSON schema. that is required by the schema

    Arguments:
        json_schema: The JSON Scheme where the node might be set as a property
        graph_state: The node the write the property for
        use_property_display_names: If True, the properties in the JSONSchema
          will be written using node display names
        use_valid_value_display_names: If True, the valid_values in the JSONSchema
          will be written using node display names
    """
    if use_property_display_names:
        node_name = node.display_name
    else:
        node_name = node.name

    if node.valid_values:
        if node.is_array:
            prop = _create_enum_array_property(node, use_valid_value_display_names)
        else:
            prop = _create_enum_property(node, use_valid_value_display_names)

    else:
        if node.is_array:
            prop = _create_array_property(node)
        else:
            prop = _create_simple_property(node)

    if node.pattern:
        prop["pattern"] = node.pattern

    prop["description"] = node.description
    prop["title"] = node.display_name
    schema_property = {node_name: prop}
    json_schema.update_property(schema_property)

    if node.is_required:
        json_schema.add_required_property(node_name)


def _process_node(
    json_schema: JSONSchema,
    graph_state: GraphTraversalState,
    logger: Logger,
    use_property_display_names: bool = True,
    use_valid_value_display_names: bool = True,
) -> None:
    """
    Processes a node in the data model graph.
    If it should be a property in the JSON Schema, that is set.
    If it is a property with reverse dependencies, conditional properties are set.

    Argument:
        json_schema: The JSON Scheme where the node might be set as a property
        graph_state: The instance tracking the current state of the graph
        use_property_display_names: If True, the properties in the JSONSchema
          will be written using node display names
        use_valid_value_display_names: If True, the valid_values in the JSONSchema
          will be written using node display names
    """
    if graph_state.current_node is None:
        raise ValueError("Node Processor contains no node.")
    logger.info("Processing node %s", graph_state.current_node.name)

    if graph_state.is_current_node_a_property():
        # Determine if current node has conditional dependencies that need to be set
        if graph_state.is_current_node_in_reverse_dependencies():
            _set_conditional_dependencies(
                json_schema=json_schema,
                graph_state=graph_state,
                use_property_display_names=use_property_display_names,
            )
            # This is to ensure that all properties that are conditional dependencies are not
            #   required, but only become required when the conditional dependency is met.
            graph_state.current_node.is_required = False
        _set_property(
            json_schema=json_schema,
            node=graph_state.current_node,
            use_property_display_names=use_property_display_names,
            use_valid_value_display_names=use_valid_value_display_names,
        )
        graph_state.update_processed_nodes_with_current_node()
        logger.info("Property set in JSON Schema for %s", graph_state.current_node.name)


def get_json_schema_log_file_path(data_model_path: str, source_node: str) -> str:
    """Get json schema log file name from the data_mdoel_path
    Args:
        data_model_path: str, path to the data model
        source_node: str, root node to create the JSON schema for
    Returns:
        json_schema_log_file_path: str, file name for the log file
    """
    # If it's a URL, extract just the filename
    if data_model_path.startswith("http://") or data_model_path.startswith("https://"):
        from urllib.parse import urlparse

        parsed_url = urlparse(data_model_path)
        # Get the last part of the path (filename)
        data_model_path = os.path.basename(parsed_url.path)

    data_model_path_root, _ = os.path.splitext(data_model_path)
    prefix = data_model_path_root
    prefix_root, prefix_ext = os.path.splitext(prefix)
    if prefix_ext == ".model":
        prefix = prefix_root
    json_schema_log_file_path = f"{prefix}.{source_node}.schema.json"
    return json_schema_log_file_path


def export_json(json_doc: Any, file_path: str, indent: Optional[int] = 4) -> None:
    """Export JSON doc to file"""
    with open(file_path, "w", encoding="utf8") as fle:
        json.dump(json_doc, fle, sort_keys=True, indent=indent, ensure_ascii=False)


def create_json_schema(  # pylint: disable=too-many-arguments
    dmge: DataModelGraphExplorer,
    datatype: str,
    schema_name: str,
    logger: Logger,
    write_schema: bool = True,
    schema_path: Optional[str] = None,
    jsonld_path: Optional[str] = None,
    use_property_display_names: bool = True,
    use_valid_value_display_names: bool = True,
) -> dict[str, Any]:
    """
    Creates a JSONSchema dict for the datatype in the data model.

    This uses the input graph starting at the node that corresponds to the input datatype.
    Starting at the given node it will(recursively):
    1. Find all the nodes this node depends on
    2. Find all the allowable metadata values / nodes that can be assigned to a particular
        node (if such a constraint is specified on the schema).

    Using the above data it will:
    - Cerate properties for each attribute of the datatype.
    - Create properties for attributes that are conditionally
        dependent on the datatypes attributes
    - Create conditional dependencies linking attributes to their dependencies

    Arguments:
        dmge: A DataModelGraphExplorer with the data model loaded
        datatype: the datatype to create the schema for.
            Its node is where we can start recursive dependency traversal
            (as mentioned above).
        write_schema: whether or not to write the schema as a json file
        schema_name: Name assigned to JSON-LD schema (to uniquely identify it via URI
            when it is hosted on the Internet).
        schema_path: Where to save the JSON Schema file
        jsonld_path: Used to name the file if the path isn't supplied
        use_property_display_names: If True, the properties in the JSONSchema
          will be written using node display names
        use_valid_value_display_names: If True, the valid_values in the JSONSchema
          will be written using node display names

    Returns:
        JSON Schema as a dictionary.
    """
    graph_state = GraphTraversalState(dmge=dmge, source_node=datatype, logger=logger)

    json_schema = JSONSchema(
        schema_id="http://example.com/" + schema_name,
        title=schema_name,
        description=dmge.get_node_comment(node_label=datatype),
    )

    while graph_state.are_nodes_remaining():
        if not graph_state.is_current_node_processed():
            _process_node(
                json_schema=json_schema,
                graph_state=graph_state,
                logger=logger,
                use_property_display_names=use_property_display_names,
                use_valid_value_display_names=use_valid_value_display_names,
            )
        graph_state.move_to_next_node()

    logger.info("JSON schema successfully created for %s", datatype)

    json_schema_dict = json_schema.as_json_schema_dict()

    if write_schema:
        _write_data_model(
            json_schema_dict=json_schema_dict,
            schema_path=schema_path,
            name=datatype,
            jsonld_path=jsonld_path,
            logger=logger,
        )
    return json_schema_dict


def _write_data_model(
    json_schema_dict: dict[str, Any],
    logger: Logger,
    schema_path: Optional[str] = None,
    name: Optional[str] = None,
    jsonld_path: Optional[str] = None,
) -> None:
    """
    Creates the JSON Schema file

    Arguments:
        json_schema_dict: The JSON schema in dict form
        schema_path: Where to save the JSON Schema file
        jsonld_path:
          The path to the JSONLD model, used to create the path
          Used if schema_path is None
        name:
          The name of the datatype(source node) the schema is being created for
          Used if schema_path is None
    """
    if schema_path:
        json_schema_path = schema_path
    elif name and jsonld_path:
        json_schema_path = get_json_schema_log_file_path(
            data_model_path=jsonld_path, source_node=name
        )
        json_schema_dirname = os.path.dirname(json_schema_path)
        # Don't create directories if the path looks like a URL
        if json_schema_dirname != "" and not (
            json_schema_path.startswith("http://")
            or json_schema_path.startswith("https://")
        ):
            os.makedirs(json_schema_dirname, exist_ok=True)

        logger.info(
            "The JSON schema file can be inspected by setting the following "
            "nested key in the configuration: (model > location)."
        )
    else:
        raise ValueError(
            "Either schema_path or both name and jsonld_path must be provided."
        )
    export_json(json_doc=json_schema_dict, file_path=json_schema_path, indent=2)
    logger.info("The JSON schema has been saved at %s", json_schema_path)


def generate_jsonschema(
    data_model_source: str,
    synapse_client: Synapse,
    data_types: Optional[list[str]] = None,
    output: Optional[str] = None,
    data_model_labels: DisplayLabelType = "class_label",
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Generate JSON Schema files from a data model.

    Arguments:
        data_model_source: Path or URL to the data model file (CSV or JSONLD). Can accept:
            - A local CSV file with your data model specification (will be parsed automatically)
            - A local JSONLD file generated from `generate_jsonld()` or equivalent
            - A URL pointing to a raw CSV data model (e.g., from GitHub)
            - A URL pointing to a raw JSONLD data model (e.g., from GitHub)
        synapse_client: Synapse client instance for logging. Use `Synapse.get_client()`
            or pass an existing authenticated client.
        data_types: List of specific cdata types to generate schemas for. If None, generates schemas for all data types in the data model.
        output: One of: None, a directory path, or a file path.
            - If None, schemas will be written to the current working directory, with filenames formatted as `<DataType>.json`.
            - If a directory path, schemas will be written to that directory, with filenames formatted as `<Output>/<DataType>.json`.
            - If a file path (must end with `.json`) and a single data type is specified, the schema for that data type will be written to that file.
        data_model_labels: Label format for properties in the generated schema:
            - `"class_label"` (default): Uses standard attribute names as property keys
            - `"display_label"`: Uses display names if valid (no blacklisted characters),.

    Returns:
        A tuple containing:
            - A list of JSON schema dictionaries, each corresponding to a data type
            - A list of file paths where the schemas were written

    Raises:
        ValueError: If a single output file is specified but multiple data types are requested.

    Example: Using this function to generate JSON Schema files:
        Generate schema for one datatype:

        ```python
        from synapseclient import Synapse
        from synapseclient.extensions.curator import generate_jsonschema

        syn = Synapse()
        syn.login()

        schemas, file_paths = generate_jsonschema(
            data_model_source="path/to/model.csv",
            output="output.json",
            data_types=None,  # All data types
            synapse_client=syn
        )
        ```

        Generate schema for specific data types:

        ```python
        schemas, file_paths = generate_jsonschema(
            data_model_source="path/to/model.csv",
            output="./schemas",
            data_types=["Patient", "Biospecimen"],
            synapse_client=syn
        )
        ```

        Generate schemas for all data types:
        Generate schema for specific components from URL:

        ```python
        schemas, file_paths = generate_jsonschema(
            data_model_source="path/to/model.csv",
            output="./schemas",
            synapse_client=syn
        )
        ```

        Generate schema from CSV URL:

        ```python
        schemas, file_paths = generate_jsonschema(
            data_model_source="https://raw.githubusercontent.com/org/repo/main/model.csv",
            output_directory="./schemas",
            data_type=None,
            data_model_labels="class_label",
            synapse_client=syn
        )
        ```
    """

    data_model_parser = DataModelParser(
        path_to_data_model=data_model_source, logger=synapse_client.logger
    )
    parsed_data_model = data_model_parser.parse_model()
    data_model_graph = DataModelGraph(parsed_data_model)
    graph_data_model = data_model_graph.graph
    dmge = DataModelGraphExplorer(graph_data_model, logger=synapse_client.logger)

    if output is not None and not output.endswith(".json"):
        dirname = output
        os.makedirs(dirname, exist_ok=True)
    else:
        dirname = "./"

    # Gets all data types if none are specified
    if data_types is None or len(data_types) == 0:
        data_types = [
            dmge.get_node_label(node[0])
            for node in [
                (k, v)
                for k, v in parsed_data_model.items()
                if v["Relationships"].get("Parent") == ["DataType"]
            ]
        ]

    if len(data_types) != 1 and output is not None and output.endswith(".json"):
        raise ValueError(
            f"Cannot write {len(data_types)} schemas to single file '{output}'. "
            "Specify a directory path instead, or request only one data type."
        )

    if len(data_types) == 1 and output is not None and output.endswith(".json"):
        schema_paths = [output]
    else:
        schema_paths = [
            os.path.join(dirname, f"{data_type}.json") for data_type in data_types
        ]

    schemas = [
        create_json_schema(
            dmge=dmge,
            datatype=data_type,
            schema_name=data_type,
            logger=synapse_client.logger,
            write_schema=True,
            schema_path=schema_path,
            use_property_display_names=(data_model_labels == "display_label"),
        )
        for data_type, schema_path in zip(data_types, schema_paths)
    ]
    return schemas, schema_paths


def generate_jsonld(
    schema: Any,
    data_model_labels: DisplayLabelType,
    output_jsonld: Optional[str],
    *,
    synapse_client: Optional[Synapse] = None,
) -> dict:
    """
    Convert a CSV data model specification to JSON-LD format with validation and error checking.

    This function parses your CSV data model (containing attributes, validation rules,
    dependencies, and valid values), converts it to a graph-based JSON-LD representation,
    validates the structure for common errors, and saves the result. The generated JSON-LD
    file serves as input for `generate_jsonschema()` and other data model operations.

    **Data Model Requirements:**

    Your CSV should include columns defining:

    - **Attribute names**: Property/attribute identifiers
    - **Display names**: Human-readable labels (optional but recommended)
    - **Descriptions**: Documentation for each attribute
    - **Valid values**: Allowed enum values for attributes (comma-separated)
    - **Validation rules**: Rules like `list`, `regex`, `inRange`, `required`, etc.
    - **Dependencies**: Relationships between attributes using `dependsOn`
    - **Required status**: Whether attributes are mandatory

    **Validation Checks Performed:**

    - Ensures all required fields (like `displayName`) are present
    - Detects cycles in attribute dependencies (which would create invalid schemas)
    - Checks for blacklisted characters in display names that Synapse doesn't allow
    - Validates that attribute names don't conflict with reserved system names
    - Verifies the graph structure is a valid directed acyclic graph (DAG)

    Arguments:
        schema: Path or URL to your data model CSV file. Can be a local file path or a URL
            (e.g., from GitHub). This file should contain your complete data model
            specification with all attributes, validation rules, and relationships.
        data_model_labels: Label format for the JSON-LD output:

            - `"class_label"` (default, recommended): Uses standard attribute names as labels
            - `"display_label"`: Uses display names as labels if they contain no blacklisted
              characters (parentheses, periods, spaces, hyphens), otherwise falls back to
              class labels. Use cautiously as this can affect downstream compatibility.
        output_jsonld: Path where the JSON-LD file will be saved. If None, saves alongside
            the input CSV with a `.jsonld` extension (e.g., `model.csv` → `model.jsonld`).
        synapse_client: Optional Synapse client instance for logging. If None, creates a
            new client instance. Use `Synapse.get_client()` or pass an authenticated client.

    **Output:**

    The function logs validation errors and warnings to help you fix data model issues
    before generating JSON schemas. Errors indicate critical problems that must be fixed,
    while warnings suggest improvements but won't block schema generation.

    Returns:
        The generated data model as a dictionary in JSON-LD format. The same data is
            also saved to the file path specified in `output_jsonld`.


    Example: Using this function to generate JSONLD Schema files:
        Basic usage with default output path:

        ```python
        from synapseclient import Synapse
        from synapseclient.extensions.curator import generate_jsonld

        syn = Synapse()
        syn.login()

        jsonld_model = generate_jsonld(
            schema="path/to/my_data_model.csv",
            data_model_labels="class_label",
            output_jsonld=None,  # Saves to my_data_model.jsonld
            synapse_client=syn
        )
        ```

        Specify custom output path:

        ```python
        jsonld_model = generate_jsonld(
            schema="models/patient_model.csv",
            data_model_labels="class_label",
            output_jsonld="~/output/patient_model_v1.jsonld",
            synapse_client=syn
        )
        ```

        Use display labels:
        ```python
        jsonld_model = generate_jsonld(
            schema="my_model.csv",
            data_model_labels="display_label",
            output_jsonld="my_model.jsonld",
            synapse_client=syn
        )
        ```

        Load from URL:
        ```python
        jsonld_model = generate_jsonld(
            schema="https://raw.githubusercontent.com/org/repo/main/model.csv",
            data_model_labels="class_label",
            output_jsonld="downloaded_model.jsonld",
            synapse_client=syn
        )
        ```
    """
    syn = Synapse.get_client(synapse_client=synapse_client)

    # Instantiate Parser
    data_model_parser = DataModelParser(path_to_data_model=schema, logger=syn.logger)

    # Parse Model
    syn.logger.info("Parsing data model.")
    parsed_data_model = data_model_parser.parse_model()

    # Convert parsed model to graph
    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(
        parsed_data_model, data_model_labels, syn.logger
    )

    # Generate graphschema
    syn.logger.info("Generating data model graph.")
    graph_data_model = data_model_grapher.graph

    # Validate generated data model.
    syn.logger.info("Validating the data model internally.")
    data_model_validator = DataModelValidator(graph=graph_data_model, logger=syn.logger)
    data_model_errors, data_model_warnings = data_model_validator.run_checks()

    # If there are errors log them.
    if data_model_errors:
        for err in data_model_errors:
            if isinstance(err, str):
                syn.logger.error(err)
            elif isinstance(err, list):
                for error in err:
                    syn.logger.error(error)

    # If there are warnings log them.
    if data_model_warnings:
        for war in data_model_warnings:
            if isinstance(war, str):
                syn.logger.warning(war)
            elif isinstance(war, list):
                for warning in war:
                    syn.logger.warning(warning)

    syn.logger.info("Converting data model to JSON-LD")
    jsonld_data_model = convert_graph_to_jsonld(
        graph=graph_data_model, logger=syn.logger
    )

    # output JSON-LD file alongside CSV file by default, get path.
    if output_jsonld is None:
        if ".jsonld" not in schema:
            # If schema is a URL, extract just the filename for local output
            schema_path = schema
            if schema.startswith("http://") or schema.startswith("https://"):
                from urllib.parse import urlparse

                parsed_url = urlparse(schema)
                schema_path = os.path.basename(parsed_url.path)
            csv_no_ext = re.sub("[.]csv$", "", schema_path)
            output_jsonld = csv_no_ext + ".jsonld"
        else:
            output_jsonld = schema

        syn.logger.info(
            "By default, the JSON-LD output will be stored alongside the first "
            f"input CSV or JSON-LD file. In this case, it will appear here: '{output_jsonld}'. "
            "You can use the `--output_jsonld` argument to specify another file path."
        )

    # saving updated schema.org schema
    try:
        export_schema(
            schema=jsonld_data_model, file_path=output_jsonld, logger=syn.logger
        )
    except Exception:
        syn.logger.exception(
            (
                f"The Data Model could not be created by using '{output_jsonld}' location. "
                "Please check your file path again"
            )
        )
    return jsonld_data_model
