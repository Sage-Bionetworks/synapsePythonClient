"""
# Tables

Synapse Tables enable storage of tabular data in Synapse in a form that can be queried using a SQL-like query language.

A table has a [Schema][synapseclient.table.Schema] and holds a set of rows conforming to that schema.

A [Schema][synapseclient.table.Schema] defines a series of [Column][synapseclient.table.Column] of the following types:

- `STRING`
- `DOUBLE`
- `INTEGER`
- `BOOLEAN`
- `DATE`
- `ENTITYID`
- `FILEHANDLEID`
- `LINK`
- `LARGETEXT`
- `USERID`

[Read more information about using Table in synapse in the tutorials section](/tutorials/tables).
"""

import abc
import collections
import collections.abc
import copy
import csv
import enum
import io
import itertools
import json
import os
import re
import tempfile
from builtins import zip
from typing import Dict, List, Tuple, TypeVar, Union

from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import SynapseError
from synapseclient.core.models.dict_object import DictObject
from synapseclient.core.utils import from_unix_epoch_time, id_of, itersubclasses

from .entity import Entity, Folder, Project, entity_type_to_class
from .evaluation import Evaluation

aggregate_pattern = re.compile(r"(count|max|min|avg|sum)\((.+)\)")

# default is STRING, only need to put the non-STRING keys in here
PANDAS_TABLE_TYPE = {
    "floating": "DOUBLE",
    "decimal": "DOUBLE",
    "integer": "INTEGER",
    "mixed-integer-float": "DOUBLE",
    "boolean": "BOOLEAN",
    "datetime64": "DATE",
    "datetime": "DATE",
    "date": "DATE",
}
# These are all the synapse columns that are lists
# Be sure to edit the values in the `cast_values` function as well
# when lists column types are added
LIST_COLUMN_TYPES = {
    "STRING_LIST",
    "INTEGER_LIST",
    "BOOLEAN_LIST",
    "DATE_LIST",
    "ENTITYID_LIST",
    "USERID_LIST",
}
MAX_NUM_TABLE_COLUMNS = 152


DEFAULT_QUOTE_CHARACTER = '"'
DEFAULT_SEPARATOR = ","
DEFAULT_ESCAPSE_CHAR = "\\"

DataFrameType = TypeVar("pd.DataFrame")


# This Enum is used to help users determine which Entity types they want in their view
# Each item will be used to construct the viewTypeMask
class EntityViewType(enum.Enum):
    FILE = 0x01
    PROJECT = 0x02
    TABLE = 0x04
    FOLDER = 0x08
    VIEW = 0x10
    DOCKER = 0x20
    SUBMISSION_VIEW = 0x40
    DATASET = 0x80
    DATASET_COLLECTION = 0x100
    MATERIALIZED_VIEW = 0x200


def _get_view_type_mask(types_to_include):
    if not types_to_include:
        raise ValueError(
            "Please include at least one of the entity types specified in EntityViewType."
        )
    mask = 0x00
    for input in types_to_include:
        if not isinstance(input, EntityViewType):
            raise ValueError(
                "Please use EntityViewType to specify the type you want to include in a View."
            )
        mask = mask | input.value
    return mask


def _get_view_type_mask_for_deprecated_type(type):
    if not type:
        raise ValueError(
            "Please specify the deprecated type to convert to viewTypeMask"
        )
    if type == "file":
        return EntityViewType.FILE.value
    if type == "project":
        return EntityViewType.PROJECT.value
    if type == "file_and_table":
        return EntityViewType.FILE.value | EntityViewType.TABLE.value
    raise ValueError("The provided value is not a valid type: %s", type)


def test_import_pandas():
    try:
        import pandas as pd  # noqa F401
    # used to catch when pandas isn't installed
    except ModuleNotFoundError:
        raise ModuleNotFoundError(
            """\n\nThe pandas package is required for this function!\n
        Most functions in the synapseclient package don't require the
        installation of pandas, but some do. Please refer to the installation
        instructions at: http://pandas.pydata.org/.
        \n\n\n"""
        )
    # catch other errors (see SYNPY-177)
    except:  # noqa
        raise


def as_table_columns(values: Union[str, DataFrameType]):
    """
    Return a list of Synapse table [Column][synapseclient.table.Column] objects
    that correspond to the columns in the given values.

    Arguments:
        values: An object that holds the content of the tables.

            - A string holding the path to a CSV file, a filehandle, or StringIO containing valid csv content
            - A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

    Returns:
        A list of Synapse table [Column][synapseclient.table.Column] objects

    Example:

        import pandas as pd

        df = pd.DataFrame(dict(a=[1, 2, 3], b=["c", "d", "e"]))
        cols = as_table_columns(df)
    """
    test_import_pandas()
    import pandas as pd
    from pandas.api.types import infer_dtype

    df = None

    # pandas DataFrame
    if isinstance(values, pd.DataFrame):
        df = values
    # filename of a csv file
    # in Python 3, we can check that the values is instanceof io.IOBase
    # for now, check if values has attr `read`
    elif isinstance(values, str) or hasattr(values, "read"):
        df = _csv_to_pandas_df(values)

    if df is None:
        raise ValueError("Values of type %s is not yet supported." % type(values))

    cols = list()
    for col in df:
        inferred_type = infer_dtype(df[col], skipna=True)
        columnType = PANDAS_TABLE_TYPE.get(inferred_type, "STRING")
        if columnType == "STRING":
            maxStrLen = df[col].str.len().max()
            if maxStrLen > 1000:
                cols.append(Column(name=col, columnType="LARGETEXT", defaultValue=""))
            else:
                size = int(
                    round(min(1000, max(30, maxStrLen * 1.5)))
                )  # Determine the length of the longest string
                cols.append(
                    Column(
                        name=col,
                        columnType=columnType,
                        maximumSize=size,
                        defaultValue="",
                    )
                )
        else:
            cols.append(Column(name=col, columnType=columnType))
    return cols


def df2Table(df, syn, tableName, parentProject):
    """Creates a new table from data in pandas data frame.
    parameters: df, tableName, parentProject
    """

    # Create columns:
    cols = as_table_columns(df)
    cols = [syn.store(col) for col in cols]

    # Create Table Schema
    schema1 = Schema(name=tableName, columns=cols, parent=parentProject)
    schema1 = syn.store(schema1)

    # Add data to Table
    for i in range(0, df.shape[0] / 1200 + 1):
        start = i * 1200
        end = min((i + 1) * 1200, df.shape[0])
        rowset1 = RowSet(
            columns=cols,
            schema=schema1,
            rows=[Row(list(df.ix[j, :])) for j in range(start, end)],
        )
        syn.store(rowset1)

    return schema1


def to_boolean(value):
    """
    Convert a string to boolean, case insensitively,
    where true values are: true, t, and 1 and false values are: false, f, 0.
    Raise a ValueError for all other values.
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        lower_value = value.lower()
        if lower_value in ["true", "t", "1"]:
            return True
        if lower_value in ["false", "f", "0"]:
            return False

    raise ValueError("Can't convert %s to boolean." % value)


def column_ids(columns):
    if columns is None:
        return []
    return [col.id for col in columns if "id" in col]


def row_labels_from_id_and_version(rows):
    return ["_".join(map(str, row)) for row in rows]


def row_labels_from_rows(rows):
    return row_labels_from_id_and_version(
        [
            (
                (row["rowId"], row["versionNumber"], row["etag"])
                if "etag" in row
                else (row["rowId"], row["versionNumber"])
            )
            for row in rows
        ]
    )


def cast_values(values, headers):
    """
    Convert a row of table query results from strings to the correct column type.

    See: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/ColumnType.html>
    """
    if len(values) != len(headers):
        raise ValueError(
            "The number of columns in the csv file does not match the given headers. %d fields, %d headers"
            % (len(values), len(headers))
        )

    result = []
    for header, field in zip(headers, values):
        columnType = header.get("columnType", "STRING")

        # convert field to column type
        if field is None or field == "":
            result.append(None)
        elif columnType in {
            "STRING",
            "ENTITYID",
            "FILEHANDLEID",
            "LARGETEXT",
            "USERID",
            "LINK",
        }:
            result.append(field)
        elif columnType == "DOUBLE":
            result.append(float(field))
        elif columnType == "INTEGER":
            result.append(int(field))
        elif columnType == "BOOLEAN":
            result.append(to_boolean(field))
        elif columnType == "DATE":
            result.append(from_unix_epoch_time(field))
        elif columnType in {
            "STRING_LIST",
            "INTEGER_LIST",
            "BOOLEAN_LIST",
            "ENTITYID_LIST",
            "USERID_LIST",
        }:
            result.append(json.loads(field))
        elif columnType == "DATE_LIST":
            result.append(json.loads(field, parse_int=from_unix_epoch_time))
        else:
            # default to string for unknown column type
            result.append(field)

    return result


def cast_row(row, headers):
    row["values"] = cast_values(row["values"], headers)
    return row


def cast_row_set(rowset):
    for i, row in enumerate(rowset["rows"]):
        rowset["rows"][i]["values"] = cast_row(row, rowset["headers"])
    return rowset


def escape_column_name(column: Union[str, collections.abc.Mapping]) -> str:
    """
    Escape the name of the given column for use in a Synapse table query statement

    Arguments:
        column: a string or column dictionary object with a 'name' key

    Returns:
        Escaped column name
    """
    col_name = (
        column["name"] if isinstance(column, collections.abc.Mapping) else str(column)
    )
    escaped_name = col_name.replace('"', '""')
    return f'"{escaped_name}"'


def join_column_names(columns: Union[List, Dict[str, str]]):
    """
    Join the names of the given columns into a comma delimited list suitable for use in a Synapse table query

    Arguments:
        columns: A sequence of column string names or dictionary objets with column 'name' keys
    """
    return ",".join(escape_column_name(c) for c in columns)


def _convert_df_date_cols_to_datetime(
    df: DataFrameType, date_columns: List
) -> DataFrameType:
    """
    Convert date columns with epoch time to date time in UTC timezone

    Argumenets:
        df: a pandas dataframe
        date_columns: name of date columns

    Returns:
        A dataframe with epoch time converted to date time in UTC timezone
    """
    test_import_pandas()
    import numpy as np
    import pandas as pd

    # find columns that are in date_columns list but not in dataframe
    diff_cols = list(set(date_columns) - set(df.columns))
    if diff_cols:
        raise ValueError("Please ensure that date columns are already in the dataframe")
    try:
        df[date_columns] = df[date_columns].astype(np.float64)
    except ValueError:
        raise ValueError(
            "Cannot convert epoch time to integer. Please make sure that the date columns that you specified contain valid epoch time value"
        )
    df[date_columns] = df[date_columns].apply(
        lambda x: pd.to_datetime(x, unit="ms", utc=True)
    )
    return df


def _csv_to_pandas_df(
    filepath,
    separator=DEFAULT_SEPARATOR,
    quote_char=DEFAULT_QUOTE_CHARACTER,
    escape_char=DEFAULT_ESCAPSE_CHAR,
    contain_headers=True,
    lines_to_skip=0,
    date_columns=None,
    list_columns=None,
    rowIdAndVersionInIndex=True,
    dtype=None,
    **kwargs,
):
    """
    Convert a csv file to a pandas dataframe

    Arguments:
        filepath: The path to the file.
        separator: The separator for the file, Defaults to `DEFAULT_SEPARATOR`.
        quote_char: The quote character for the file,
          Defaults to `DEFAULT_QUOTE_CHARACTER`.
        escape_char: The escape character for the file,
                    Defaults to `DEFAULT_ESCAPSE_CHAR`.
                    contain_headers: Whether the file contains headers,
                    Defaults to `True`.
        lines_to_skip: The number of lines to skip at the beginning of the file,
                        Defaults to `0`.
        date_columns: The names of the date columns in the file
        list_columns: The names of the list columns in the file
        rowIdAndVersionInIndex: Whether the file contains rowId and
                                version in the index, Defaults to `True`.
        dtype: The data type for the file, Defaults to `None`.
        **kwargs: Additional keyword arguments to pass to pandas.read_csv. See
                    https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
                    for complete list of supported arguments.

    Returns:
        A pandas dataframe
    """
    test_import_pandas()
    import pandas as pd

    line_terminator = str(os.linesep)

    # assign line terminator only if for single character
    # line terminators (e.g. not '\r\n') 'cause pandas doesn't
    # longer line terminators. See: <https://github.com/pydata/pandas/issues/3501>
    # "ValueError: Only length-1 line terminators supported"
    df = pd.read_csv(
        filepath,
        dtype=dtype,
        sep=separator,
        lineterminator=line_terminator if len(line_terminator) == 1 else None,
        quotechar=quote_char,
        escapechar=escape_char,
        header=0 if contain_headers else None,
        skiprows=lines_to_skip,
        **kwargs,
    )
    # parse date columns if exists
    if date_columns:
        df = _convert_df_date_cols_to_datetime(df, date_columns)
    # Turn list columns into lists
    if list_columns:
        for col in list_columns:
            # Fill NA values with empty lists, it must be a string for json.loads to work
            df.fillna({col: "[]"}, inplace=True)
            df[col] = df[col].apply(json.loads)

    if (
        rowIdAndVersionInIndex
        and "ROW_ID" in df.columns
        and "ROW_VERSION" in df.columns
    ):
        # combine row-ids (in index) and row-versions (in column 0) to
        # make new row labels consisting of the row id and version
        # separated by a dash.
        zip_args = [df["ROW_ID"], df["ROW_VERSION"]]
        if "ROW_ETAG" in df.columns:
            zip_args.append(df["ROW_ETAG"])

        df.index = row_labels_from_id_and_version(zip(*zip_args))
        del df["ROW_ID"]
        del df["ROW_VERSION"]
        if "ROW_ETAG" in df.columns:
            del df["ROW_ETAG"]

    return df


def _create_row_delete_csv(row_id_vers_iterable: Tuple[str, int]) -> str:
    """
    Creates a temporary csv used for deleting rows

    Arguments:
        row_id_vers_iterable: An iterable containing tuples with format: (row_id, row_version)

    Returns:
        A filepath of created csv file
    """
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as temp_csv:
        csv_writer = csv.writer(temp_csv)
        csv_writer.writerow(("ROW_ID", "ROW_VERSION"))
        csv_writer.writerows(row_id_vers_iterable)
        return temp_csv.name


def _delete_rows(syn, schema, row_id_vers_list: Tuple[str, int]) -> None:
    """
    Deletes rows from a synapse table

    Arguments:
        syn:              An instance of [Synapse][synapseclient.client.Synapse]
        schema:           The Schema is an [Entity][synapseclient.entity.Entity] that defines
                          a set of columns in a table.
        row_id_vers_list: An iterable containing tuples with format: (row_id, row_version)
    """
    delete_row_csv_filepath = _create_row_delete_csv(row_id_vers_list)
    try:
        syn._uploadCsv(delete_row_csv_filepath, schema)
    finally:
        os.remove(delete_row_csv_filepath)


def delete_rows(
    syn,
    table_id: str,
    row_id_vers_list: List[Tuple[int, int]],
):
    """
    Deletes rows from a synapse table

    Arguments:
        syn:              An instance of [Synapse][synapseclient.client.Synapse]
        table_id:         The ID of the table to delete rows from
        row_id_vers_list: An iterable containing tuples with format: (row_id, row_version)
    """
    delete_row_csv_filepath = _create_row_delete_csv(
        row_id_vers_iterable=row_id_vers_list
    )
    try:
        syn._uploadCsv(filepath=delete_row_csv_filepath, schema=table_id)
    finally:
        os.remove(delete_row_csv_filepath)


class SchemaBase(Entity, metaclass=abc.ABCMeta):
    """
    This is the an Abstract Class for EntityViewSchema and Schema containing the common methods for both.
    You can not create an object of this type.
    """

    _property_keys = Entity._property_keys + ["columnIds"]
    _local_keys = Entity._local_keys + ["columns_to_store"]

    @property
    @abc.abstractmethod  # forces subclasses to define _synapse_entity_type
    def _synapse_entity_type(self):
        pass

    @abc.abstractmethod
    def __init__(
        self, name, columns, properties, annotations, local_state, parent, **kwargs
    ):
        self.properties.setdefault("columnIds", [])
        self.__dict__.setdefault("columns_to_store", [])

        if name:
            kwargs["name"] = name
        super(SchemaBase, self).__init__(
            properties=properties,
            annotations=annotations,
            local_state=local_state,
            parent=parent,
            **kwargs,
        )
        if columns:
            self.addColumns(columns)

    def addColumn(self, column) -> None:
        """
        Store the column

        Arguments:
            column: A column object or its ID

        Raises:
            ValueError: If the given column is not a string, integer or [Column][synapseclient.table.Column] object
        """
        if isinstance(column, str) or isinstance(column, int) or hasattr(column, "id"):
            self.properties.columnIds.append(id_of(column))
        elif isinstance(column, Column):
            if not self.__dict__.get("columns_to_store", None):
                self.__dict__["columns_to_store"] = []
            self.__dict__["columns_to_store"].append(column)
        else:
            raise ValueError("Not a column? %s" % str(column))

    def addColumns(self, columns: list) -> None:
        """
        Add columns

        Arguments:
            columns: A list of column objects or their ID
        """
        for column in columns:
            self.addColumn(column)

    def removeColumn(self, column) -> None:
        """
        Remove column

        Arguments:
            column: A column object or its ID

        Raises:
            ValueError: If the given column is not a string, integer or [Column][synapseclient.table.Column] object
        """
        if isinstance(column, str) or isinstance(column, int) or hasattr(column, "id"):
            self.properties.columnIds.remove(id_of(column))
        elif isinstance(column, Column) and self.columns_to_store:
            self.columns_to_store.remove(column)
        else:
            ValueError("Can't remove column %s" + str(column))

    def has_columns(self):
        """Does this schema have columns specified?"""
        return bool(
            self.properties.get("columnIds", None)
            or self.__dict__.get("columns_to_store", None)
        )

    def _before_synapse_store(self, syn):
        if len(self.columns_to_store) + len(self.columnIds) > MAX_NUM_TABLE_COLUMNS:
            raise ValueError(
                "Too many columns. The limit is %s columns per table"
                % MAX_NUM_TABLE_COLUMNS
            )

        # store any columns before storing table
        if self.columns_to_store:
            self.properties.columnIds.extend(
                column.id for column in syn.createColumns(self.columns_to_store)
            )
            self.columns_to_store = []


class Schema(SchemaBase):
    """
    A Schema is an [Entity][synapseclient.entity.Entity] that defines a set of columns in a table.

    Attributes:
        name:        The name for the Table Schema object
        description: User readable description of the schema
        columns:     A list of [Column][synapseclient.table.Column] objects or their IDs
        parent:      The project in Synapse to which this table belongs
        properties:  A map of Synapse properties
        annotations: A map of user defined annotations
        local_state: Internal use only

    Example:

        cols = [Column(name='Isotope', columnType='STRING'),
                Column(name='Atomic Mass', columnType='INTEGER'),
                Column(name='Halflife', columnType='DOUBLE'),
                Column(name='Discovered', columnType='DATE')]

        schema = syn.store(Schema(name='MyTable', columns=cols, parent=project))
    """

    _synapse_entity_type = "org.sagebionetworks.repo.model.table.TableEntity"

    def __init__(
        self,
        name=None,
        columns=None,
        parent=None,
        properties=None,
        annotations=None,
        local_state=None,
        **kwargs,
    ):
        super(Schema, self).__init__(
            name=name,
            columns=columns,
            properties=properties,
            annotations=annotations,
            local_state=local_state,
            parent=parent,
            **kwargs,
        )


class MaterializedViewSchema(SchemaBase):
    """
    A MaterializedViewSchema is an [Entity][synapseclient.entity.Entity] that defines a set of columns in a
    materialized view along with the SQL statement.

    Attributes:
        name:        The name for the Materialized View Schema object
        description: User readable description of the schema
        definingSQL: The synapse SQL statement that defines the data in the materialized view. The SQL                   contain JOIN clauses on multiple tables.
        columns:     A list of [Column][synapseclient.table.Column] objects or their IDs
        parent:      The project in Synapse to which this Materialized View belongs
        properties:  A map of Synapse properties
        annotations: A map of user defined annotations
        local_state: Internal use only

    Example:

        defining_sql = "SELECT * FROM syn111 F JOIN syn2222 P on (F.patient_id = P.patient_id)"

        schema = syn.store(MaterializedViewSchema(name='MyTable', parent=project, definingSQL=defining_sql))
    """

    _synapse_entity_type = "org.sagebionetworks.repo.model.table.MaterializedView"
    _property_keys = SchemaBase._property_keys + ["definingSQL"]

    def __init__(
        self,
        name=None,
        columns=None,
        parent=None,
        definingSQL=None,
        properties=None,
        annotations=None,
        local_state=None,
        **kwargs,
    ):
        if definingSQL is not None:
            kwargs["definingSQL"] = definingSQL
        super(MaterializedViewSchema, self).__init__(
            name=name,
            columns=columns,
            properties=properties,
            annotations=annotations,
            local_state=local_state,
            parent=parent,
            **kwargs,
        )


class ViewBase(SchemaBase):
    """
    This is a helper class for EntityViewSchema and SubmissionViewSchema
    containing the common methods for both.
    """

    _synapse_entity_type = ""
    _property_keys = SchemaBase._property_keys + ["viewTypeMask", "scopeIds"]
    _local_keys = SchemaBase._local_keys + [
        "addDefaultViewColumns",
        "addAnnotationColumns",
        "ignoredAnnotationColumnNames",
    ]

    def add_scope(self, entities: Union[Project, Folder, Evaluation, list, str]):
        """
        Add scope

        Arguments:
            entities: A [Project][synapseclient.entity.Project], [Folder][synapseclient.entity.Folder],
                      [Evaluation][synapseclient.evaluation.Evaluation] object or its ID, can also be a list of them
        """
        if isinstance(entities, list):
            # add ids to a temp list so that we don't partially modify scopeIds on an exception in id_of()
            temp_list = [id_of(entity) for entity in entities]
            self.scopeIds.extend(temp_list)
        else:
            self.scopeIds.append(id_of(entities))

    def _filter_duplicate_columns(self, syn, columns_to_add):
        """
        If a column to be added has the same name and same type as an existing column, it will be considered a duplicate
         and not added.

        Arguments:
            syn:             A [Synapse][synapseclient.client.Synapse] object that is logged in
            columns_to_add:  A iterable collection of type [Column][synapseclient.table.Column] objects

        Returns:
            A filtered list of columns to add
        """

        # no point in making HTTP calls to retrieve existing Columns if we not adding any new columns
        if not columns_to_add:
            return columns_to_add

        # set up Column name/type tracking
        # map of str -> set(str), where str is the column type as a string and set is a set of column name strings
        column_type_to_annotation_names = {}

        # add to existing columns the columns that user has added but not yet created in synapse
        column_generator = (
            itertools.chain(syn.getColumns(self.columnIds), self.columns_to_store)
            if self.columns_to_store
            else syn.getColumns(self.columnIds)
        )

        for column in column_generator:
            column_name = column["name"]
            column_type = column["columnType"]

            column_type_to_annotation_names.setdefault(column_type, set()).add(
                column_name
            )

        valid_columns = []
        for column in columns_to_add:
            new_col_name = column["name"]
            new_col_type = column["columnType"]

            typed_col_name_set = column_type_to_annotation_names.setdefault(
                new_col_type, set()
            )
            if new_col_name not in typed_col_name_set:
                typed_col_name_set.add(new_col_name)
                valid_columns.append(column)
        return valid_columns

    def _before_synapse_store(self, syn):
        # get the default EntityView columns from Synapse and add them to the columns list
        additional_columns = []
        view_type = self._synapse_entity_type.split(".")[-1].lower()
        mask = self.get("viewTypeMask")

        if self.addDefaultViewColumns:
            additional_columns.extend(
                syn._get_default_view_columns(view_type, view_type_mask=mask)
            )

        # get default annotations
        if self.addAnnotationColumns:
            anno_columns = [
                x
                for x in syn._get_annotation_view_columns(
                    self.scopeIds, view_type, view_type_mask=mask
                )
                if x["name"] not in self.ignoredAnnotationColumnNames
            ]
            additional_columns.extend(anno_columns)

        self.addColumns(self._filter_duplicate_columns(syn, additional_columns))

        # set these boolean flags to false so they are not repeated.
        self.addDefaultViewColumns = False
        self.addAnnotationColumns = False

        super(ViewBase, self)._before_synapse_store(syn)


class Dataset(ViewBase):
    """
    A Dataset is an [Entity][synapseclient.entity.Entity] that defines a
    flat list of entities as a tableview (a.k.a. a "dataset").

    Attributes:
        name:          The name for the Dataset object
        description:   User readable description of the schema
        columns:       A list of [Column][synapseclient.table.Column] objects or their IDs
        parent:        The Synapse Project to which this Dataset belongs
        properties:    A map of Synapse properties
        annotations:   A map of user defined annotations
        dataset_items: A list of items characterized by entityId and versionNumber
        folder:        A list of Folder IDs
        local_state:   Internal use only

    Example: Using Dataset
        Load Dataset

            from synapseclient import Dataset

        Create a Dataset with pre-defined DatasetItems. Default Dataset columns
        are used if no schema is provided.

            dataset_items = [
                {'entityId': "syn000", 'versionNumber': 1},
                {...},
            ]

            dataset = syn.store(Dataset(
                name="My Dataset",
                parent=project,
                dataset_items=dataset_items))

        Add/remove specific Synapse IDs to/from the Dataset

            dataset.add_item({'entityId': "syn111", 'versionNumber': 1})
            dataset.remove_item("syn000")
            dataset = syn.store(dataset)

        Add a list of Synapse IDs to the Dataset

            new_items = [
                {'entityId': "syn222", 'versionNumber': 2},
                {'entityId': "syn333", 'versionNumber': 1}
            ]
            dataset.add_items(new_items)
            dataset = syn.store(dataset)

    Folders can easily be added recursively to a dataset, that is, all files
    within the folder (including sub-folders) will be added.  Note that using
    the following methods will add files with the latest version number ONLY.
    If another version number is desired, use [add_item][synapseclient.table.Dataset.add_item]
    or [add_items][synapseclient.table.Dataset.add_items].

    Example: Add folder to Dataset
        Add a single Folder to the Dataset.

            dataset.add_folder("syn123")

        Add a list of Folders, overwriting any existing files in the dataset.

            dataset.add_folders(["syn456", "syn789"], force=True)
            dataset = syn.store(dataset)

    Example: Truncate a Dataset
        empty() can be used to truncate a dataset, that is, remove all current items from the set.

            dataset.empty()
            dataset = syn.store(dataset)



    Example: Check items in a Dataset
        To get the number of entities in the dataset, use len().

            print(f"{dataset.name} has {len(dataset)} items.")

    Example: Create a snapshot of the Dataset
        To create a snapshot version of the Dataset, use
        [create_snapshot_version][synapseclient.Synapse.create_snapshot_version].

            syn = synapseclient.login()
            syn.create_snapshot_version(
                dataset.id,
                label="v1.0",
                comment="This is version 1")
    """

    _synapse_entity_type: str = "org.sagebionetworks.repo.model.table.Dataset"
    _property_keys: List[str] = ViewBase._property_keys + ["datasetItems"]
    _local_keys: List[str] = ViewBase._local_keys + ["folders_to_add", "force"]

    def __init__(
        self,
        name=None,
        columns=None,
        parent=None,
        properties=None,
        addDefaultViewColumns=True,
        addAnnotationColumns=True,
        ignoredAnnotationColumnNames=[],
        annotations=None,
        local_state=None,
        dataset_items=None,
        folders=None,
        force=False,
        **kwargs,
    ):
        self.properties.setdefault("datasetItems", [])
        self.__dict__.setdefault("folders_to_add", set())
        self.ignoredAnnotationColumnNames = set(ignoredAnnotationColumnNames)
        self.viewTypeMask = EntityViewType.DATASET.value
        super(Dataset, self).__init__(
            name=name,
            columns=columns,
            properties=properties,
            annotations=annotations,
            local_state=local_state,
            parent=parent,
            **kwargs,
        )

        self.force = force
        if dataset_items:
            self.add_items(dataset_items, force)
        if folders:
            self.add_folders(folders, force)

        # HACK: make sure we don't try to add columns to schemas that we retrieve from synapse
        is_from_normal_constructor = not (properties or local_state)
        # allowing annotations because user might want to update annotations all at once
        self.addDefaultViewColumns = (
            addDefaultViewColumns and is_from_normal_constructor
        )
        self.addAnnotationColumns = addAnnotationColumns and is_from_normal_constructor

    def __len__(self):
        return len(self.properties.datasetItems)

    @staticmethod
    def _check_needed_keys(keys: List[str]):
        required_keys = {"entityId", "versionNumber"}
        if required_keys - keys:
            raise LookupError(
                "DatasetItem missing a required property: %s"
                % str(required_keys - keys)
            )
        return True

    def add_item(self, dataset_item: Dict[str, str], force: bool = True):
        """
        Add a dataset item

        Arguments:
            dataset_item: A single dataset item
            force:        Force add item

        Raises:
            ValueError: If duplicate item is found
            ValueError: The item is not a DatasetItem
        """
        if isinstance(dataset_item, dict) and self._check_needed_keys(
            dataset_item.keys()
        ):
            if not self.has_item(dataset_item.get("entityId")):
                self.properties.datasetItems.append(dataset_item)
            else:
                if force:
                    self.remove_item(dataset_item.get("entityId"))
                    self.properties.datasetItems.append(dataset_item)
                else:
                    raise ValueError(
                        f"Duplicate item found: {dataset_item.get('entityId')}. "
                        "Set force=True to overwrite the existing item."
                    )
        else:
            raise ValueError("Not a DatasetItem? %s" % str(dataset_item))

    def add_items(self, dataset_items: List[Dict[str, str]], force: bool = True):
        """
        Add items

        Arguments:
            dataset_items: A list of dataset items
            force:         Force add items
        """
        for dataset_item in dataset_items:
            self.add_item(dataset_item, force)

    def remove_item(self, item_id: str):
        """
        Remove item

        Arguments:
            item_id: A single dataset item Synapse ID
        """
        item_id = id_of(item_id)
        if item_id.startswith("syn"):
            for i, curr_item in enumerate(self.properties.datasetItems):
                if curr_item.get("entityId") == item_id:
                    del self.properties.datasetItems[i]
                    break
        else:
            raise ValueError("Not a Synapse ID: %s" % str(item_id))

    def empty(self):
        self.properties.datasetItems = []

    def has_item(self, item_id: str) -> bool:
        """
        Check if has dataset item

        Arguments:
            item_id: A single dataset item Synapse ID
        """
        return any(item["entityId"] == item_id for item in self.properties.datasetItems)

    def add_folder(self, folder: str, force: bool = True):
        """
        Add a folder

        Arguments:
            folder: A single Synapse Folder ID
            force:  Force add items from folder
        """
        if not self.__dict__.get("folders_to_add", None):
            self.__dict__["folders_to_add"] = set()
        self.__dict__["folders_to_add"].add(folder)
        # if self.force != force:
        self.force = force

    def add_folders(self, folders: List[str], force: bool = True):
        """
        Add folders

        Arguments:
            folders: A list of Synapse Folder IDs
            force:   Force add items from folders
        """
        if (
            isinstance(folders, list)
            or isinstance(folders, set)
            or isinstance(folders, tuple)
        ):
            self.force = force
            for folder in folders:
                self.add_folder(folder, force)
        else:
            raise ValueError(f"Not a list of Folder IDs: {folders}")

    def _add_folder_files(self, syn, folder):
        files = []
        children = syn.getChildren(folder)
        for child in children:
            if child.get("type") == "org.sagebionetworks.repo.model.Folder":
                files.extend(self._add_folder_files(syn, child.get("id")))
            elif child.get("type") == "org.sagebionetworks.repo.model.FileEntity":
                files.append(
                    {
                        "entityId": child.get("id"),
                        "versionNumber": child.get("versionNumber"),
                    }
                )
            else:
                raise ValueError(f"Not a Folder?: {folder}")
        return files

    def _before_synapse_store(self, syn):
        # Add files from folders (if any) before storing dataset.
        if self.folders_to_add:
            for folder in self.folders_to_add:
                items_to_add = self._add_folder_files(syn, folder)
                self.add_items(items_to_add, self.force)
            self.folders_to_add = set()
        # Must set this scopeIds is used to get all annotations from the
        # entities
        self.scopeIds = [item["entityId"] for item in self.properties.datasetItems]
        super()._before_synapse_store(syn)
        # Reset attribute to force-add items from folders.
        self.force = True
        # Remap `datasetItems` back to `items` before storing (since `items`
        # is the accepted field name in the API, not `datasetItems`).
        self.properties.items = self.properties.datasetItems


class EntityViewSchema(ViewBase):
    """
    A EntityViewSchema is a [Entity][synapseclient.entity.Entity] that displays all files/projects
    (depending on user choice) within a given set of scopes.

    Attributes:
        name:                         The name of the Entity View Table object
        columns:                      (Optional) A list of [Column][synapseclient.table.Column] objects or their IDs.
        parent:                       The project in Synapse to which this table belongs
        scopes:                       A list of Projects/Folders or their ids
        type:                         This field is deprecated. Please use `includeEntityTypes`
        includeEntityTypes:           A list of entity types to include in the view. Supported entity types are:

            - `EntityViewType.FILE`
            - `EntityViewType.PROJECT`
            - `EntityViewType.TABLE`
            - `EntityViewType.FOLDER`
            - `EntityViewType.VIEW`
            - `EntityViewType.DOCKER`

            If none is provided, the view will default to include `EntityViewType.FILE`.
        addDefaultViewColumns:        If true, adds all default columns (e.g. name, createdOn, modifiedBy etc.)
                                      Defaults to True.
                                      The default columns will be added after a call to
                                      [store][synapseclient.Synapse.store].
        addAnnotationColumns:         If true, adds columns for all annotation keys defined across all Entities in
                                      the EntityViewSchema's scope. Defaults to True.
                                      The annotation columns will be added after a call to
                                      [store][synapseclient.Synapse.store].
        ignoredAnnotationColumnNames: A list of strings representing annotation names.
                                      When addAnnotationColumns is True, the names in this list will not be
                                      automatically added as columns to the EntityViewSchema if they exist in any
                                      of the defined scopes.
        properties:                   A map of Synapse properties
        annotations:                  A map of user defined annotations
        local_state:                  Internal use only

    Example:

        from synapseclient import EntityViewType

        project_or_folder = syn.get("syn123")
        schema = syn.store(EntityViewSchema(name='MyTable', parent=project, scopes=[project_or_folder_id, 'syn123'],
         includeEntityTypes=[EntityViewType.FILE]))
    """

    _synapse_entity_type = "org.sagebionetworks.repo.model.table.EntityView"

    def __init__(
        self,
        name=None,
        columns=None,
        parent=None,
        scopes=None,
        type=None,
        includeEntityTypes=None,
        addDefaultViewColumns=True,
        addAnnotationColumns=True,
        ignoredAnnotationColumnNames=[],
        properties=None,
        annotations=None,
        local_state=None,
        **kwargs,
    ):
        if includeEntityTypes:
            kwargs["viewTypeMask"] = _get_view_type_mask(includeEntityTypes)
        elif type:
            kwargs["viewTypeMask"] = _get_view_type_mask_for_deprecated_type(type)
        elif properties and "type" in properties:
            kwargs["viewTypeMask"] = _get_view_type_mask_for_deprecated_type(
                properties["type"]
            )
            properties["type"] = None

        self.ignoredAnnotationColumnNames = set(ignoredAnnotationColumnNames)
        super(EntityViewSchema, self).__init__(
            name=name,
            columns=columns,
            properties=properties,
            annotations=annotations,
            local_state=local_state,
            parent=parent,
            **kwargs,
        )

        # This is a hacky solution to make sure we don't try to add columns to schemas that we retrieve from synapse
        is_from_normal_constructor = not (properties or local_state)
        # allowing annotations because user might want to update annotations all at once
        self.addDefaultViewColumns = (
            addDefaultViewColumns and is_from_normal_constructor
        )
        self.addAnnotationColumns = addAnnotationColumns and is_from_normal_constructor

        # set default values after constructor so we don't overwrite the values defined in properties using .get()
        # because properties, unlike local_state, do not have nonexistent keys assigned with a value of None
        if self.get("viewTypeMask") is None:
            self.viewTypeMask = EntityViewType.FILE.value
        if self.get("scopeIds") is None:
            self.scopeIds = []

        # add the scopes last so that we can append the passed in scopes to those defined in properties
        if scopes is not None:
            self.add_scope(scopes)

    def set_entity_types(self, includeEntityTypes):
        """
        Set entity types

        Arguments:
            includeEntityTypes: A list of entity types to include in the view. This list will replace the previous
                                settings. Supported entity types are:

                - `EntityViewType.FILE`
                - `EntityViewType.PROJECT`
                - `EntityViewType.TABLE`
                - `EntityViewType.FOLDER`
                - `EntityViewType.VIEW`
                - `EntityViewType.DOCKER`
        """
        self.viewTypeMask = _get_view_type_mask(includeEntityTypes)


class SubmissionViewSchema(ViewBase):
    """
    A SubmissionViewSchema is a [Entity][synapseclient.entity.Entity] that displays all files/projects
    (depending on user choice) within a given set of scopes.

    Attributes:
        name:                         The name of the Entity View Table object
        columns:                      A list of [Column][synapseclient.table.Column] objects or their IDs. These are optional.
        parent:                       The project in Synapse to which this table belongs
        scopes:                       A list of Evaluation Queues or their ids
        addDefaultViewColumns:        If true, adds all default columns (e.g. name, createdOn, modifiedBy etc.)
                                      Defaults to True.
                                      The default columns will be added after a call to
                                      [store][synapseclient.Synapse.store].
        addAnnotationColumns:         If true, adds columns for all annotation keys defined across all Entities in
                                      the SubmissionViewSchema's scope. Defaults to True.
                                      The annotation columns will be added after a call to
                                      [store][synapseclient.Synapse.store].
        ignoredAnnotationColumnNames: A list of strings representing annotation names.
                                      When addAnnotationColumns is True, the names in this list will not be
                                      automatically added as columns to the SubmissionViewSchema if they exist in
                                      any of the defined scopes.
        properties:                   A map of Synapse properties
        annotations:                  A map of user defined annotations
        local_state:                  Internal use only

    Example:
        from synapseclient import SubmissionViewSchema

        project = syn.get("syn123")
        schema = syn.store(SubmissionViewSchema(name='My Submission View', parent=project, scopes=['9614543']))
    """

    _synapse_entity_type = "org.sagebionetworks.repo.model.table.SubmissionView"

    def __init__(
        self,
        name=None,
        columns=None,
        parent=None,
        scopes=None,
        addDefaultViewColumns=True,
        addAnnotationColumns=True,
        ignoredAnnotationColumnNames=[],
        properties=None,
        annotations=None,
        local_state=None,
        **kwargs,
    ):
        self.ignoredAnnotationColumnNames = set(ignoredAnnotationColumnNames)
        super(SubmissionViewSchema, self).__init__(
            name=name,
            columns=columns,
            properties=properties,
            annotations=annotations,
            local_state=local_state,
            parent=parent,
            **kwargs,
        )
        # This is a hacky solution to make sure we don't try to add columns to schemas that we retrieve from synapse
        is_from_normal_constructor = not (properties or local_state)
        # allowing annotations because user might want to update annotations all at once
        self.addDefaultViewColumns = (
            addDefaultViewColumns and is_from_normal_constructor
        )
        self.addAnnotationColumns = addAnnotationColumns and is_from_normal_constructor

        if self.get("scopeIds") is None:
            self.scopeIds = []

        # add the scopes last so that we can append the passed in scopes to those defined in properties
        if scopes is not None:
            self.add_scope(scopes)


# add Schema to the map of synapse entity types to their Python representations
for cls in itersubclasses(SchemaBase):
    entity_type_to_class[cls._synapse_entity_type] = cls
# HACK: viewbase extends schema base, so need to remove ViewBase
entity_type_to_class.pop("")


class SelectColumn(DictObject):
    """
    Defines a column to be used in a table [Schema][synapseclient.table.Schema].

    Attributes:
        id:         An immutable ID issued by the platform
        columnType: Can be any of:

            - `STRING`
            - `DOUBLE`
            - `INTEGER`
            - `BOOLEAN`
            - `DATE`
            - `FILEHANDLEID`
            - `ENTITYID`

        name:       The display name of the column
    """

    def __init__(self, id=None, columnType=None, name=None, **kwargs):
        super(SelectColumn, self).__init__()
        if id:
            self.id = id

        if name:
            self.name = name

        if columnType:
            self.columnType = columnType

        # Notes that this param is only used to support forward compatibility.
        self.update(kwargs)

    @classmethod
    def from_column(cls, column):
        return cls(
            column.get("id", None),
            column.get("columnType", None),
            column.get("name", None),
        )


class Column(DictObject):
    """
    Defines a column to be used in a table [Schema][synapseclient.table.Schema]
    [EntityViewSchema][synapseclient.table.EntityViewSchema].

    Attributes:
        id:                An immutable ID issued by the platform
        columnType:        The column type determines the type of data that can be stored in a column. It can be any
                           of:

            - `STRING`
            - `DOUBLE`
            - `INTEGER`
            - `BOOLEAN`
            - `DATE`
            - `FILEHANDLEID`
            - `ENTITYID`
            - `LINK`
            - `LARGETEXT`
            - `USERID`

            For more information, please see:
            <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/ColumnType.html>
        maximumSize:       A parameter for columnTypes with a maximum size. For example, ColumnType.STRINGs have a
                           default maximum size of 50 characters, but can be set to a `maximumSize` of 1 to 1000
                           characters.
        maximumListLength: Required if using a columnType with a "_LIST" suffix. Describes the maximum number of
                           values that will appear in that list. Value range 1-100 inclusive. Default 100
        name:              The display name of the column
        enumValues:        Columns type of STRING can be constrained to an enumeration values set on this list.
        defaultValue:      The default value for this column. Columns of type FILEHANDLEID and ENTITYID are not
                           allowed to have default values.
    """

    @classmethod
    def getURI(cls, id):
        return "/column/%s" % id

    def __init__(self, **kwargs):
        super(Column, self).__init__(kwargs)
        self["concreteType"] = concrete_types.COLUMN_MODEL

    def postURI(self):
        return "/column"


class AppendableRowset(DictObject, metaclass=abc.ABCMeta):
    """
    Abstract Base Class for [RowSet][synapseclient.table.RowSet] and [PartialRowset][synapseclient.table.PartialRowset]
    """

    @abc.abstractmethod
    def __init__(self, schema, **kwargs):
        if ("tableId" not in kwargs) and schema:
            kwargs["tableId"] = id_of(schema)

        if not kwargs.get("tableId", None):
            raise ValueError(
                "Table schema ID must be defined to create a %s" % type(self).__name__
            )
        super(AppendableRowset, self).__init__(kwargs)

    def _synapse_store(self, syn):
        """
        Creates and POSTs an [AppendableRowSetRequest](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/AppendableRowSetRequest.html)
        """
        append_rowset_request = {
            "concreteType": concrete_types.APPENDABLE_ROWSET_REQUEST,
            "toAppend": self,
            "entityId": self.tableId,
        }

        response = syn._async_table_update(
            self.tableId, [append_rowset_request], wait=True
        )
        syn._check_table_transaction_response(response)
        return response["results"][0]


class PartialRowset(AppendableRowset):
    """
    A set of Partial Rows used for updating cells of a table.
    PartialRowsets allow you to push only the individual cells you wish to change instead of pushing entire rows with
    many unchanged cells.

    Attributes:
        schema: The [Schema][synapseclient.table.Schema] of the table to update or its tableId as a string
        rows:   A list of PartialRows

    Example: Update cells in
        The following code will change cells in a hypothetical table, syn123:
        these same steps will also work for using EntityView tables to change Entity annotations

        From:

                +-------------+--------------+
                | Column One  | Column Two   |
                +=============+==============+
                | Data 1      | Data A       |
                +-------------+--------------+
                | Data 2      | Data B       |
                +-------------+--------------+
                | Data 3      | Data C       |
                +-------------+--------------+

        To

                +-------------+--------------+
                | Column One  | Column Two   |
                +=============+==============+
                | Data 1  2   | Data A       |
                +-------------+--------------+
                | Data 2      | Data B  D    |
                +-------------+--------------+
                | Data 3      | Data C       |
                +-------------+--------------+

            query_results = syn.tableQuery("SELECT * FROM syn123")

        The easiest way to know the rowId of the row you wish to change
        is by converting the table to a pandas DataFrame with rowIdAndVersionInIndex=False

            df = query_results.asDataFrame(rowIdAndVersionInIndex=False)

            partial_changes = {df['ROW_ID'][0]: {'fooCol': 'foo foo 1'},
                               df['ROW_ID'][1]: {'barCol': 'bar bar 2'}}

        You will need to pass in your original query result as an argument
        so that we can perform column id translation and etag retrieval on your behalf:

            partial_rowset = PartialRowset.from_mapping(partial_changes, query_results)
            syn.store(partial_rowset)
    """

    @classmethod
    def from_mapping(cls, mapping, originalQueryResult):
        """
        Creates a PartialRowset

        Arguments:
            mapping:             A mapping of mappings in the structure: {ROW_ID : {COLUMN_NAME: NEW_COL_VALUE}}
            originalQueryResult: The original query result

        Returns:
            A PartialRowset that can be syn.store()-ed to apply the changes
        """
        if not isinstance(mapping, collections.abc.Mapping):
            raise ValueError("mapping must be a supported Mapping type such as 'dict'")

        try:
            name_to_column_id = {
                col.name: col.id for col in originalQueryResult.headers if "id" in col
            }
        except AttributeError:
            raise ValueError(
                "originalQueryResult must be the result of a syn.tableQuery()"
            )

        row_ids = set(int(id) for id in mapping.keys())

        # row_ids in the originalQueryResult are not guaranteed to be in ascending order
        # iterate over all etags but only map the row_ids used for this partial update to their etags
        row_etags = {
            row_id: etag
            for row_id, row_version, etag in originalQueryResult.iter_row_metadata()
            if row_id in row_ids and etag is not None
        }

        partial_rows = [
            PartialRow(
                row_changes,
                row_id,
                etag=row_etags.get(int(row_id)),
                nameToColumnId=name_to_column_id,
            )
            for row_id, row_changes in mapping.items()
        ]

        return cls(originalQueryResult.tableId, partial_rows)

    def __init__(self, schema, rows):
        super(PartialRowset, self).__init__(schema)
        self.concreteType = concrete_types.PARTIAL_ROW_SET

        if isinstance(rows, PartialRow):
            self.rows = [rows]
        else:
            try:
                if all(isinstance(row, PartialRow) for row in rows):
                    self.rows = list(rows)
                else:
                    raise ValueError("rows must contain only values of type PartialRow")
            except TypeError:
                raise ValueError("rows must be iterable")


class RowSet(AppendableRowset):
    """
    A Synapse object of type [org.sagebionetworks.repo.model.table.RowSet](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/RowSet.html).

    Attributes:
        schema:  A [Schema][synapseclient.table.Schema] object that will be used to set the tableId
        headers: The list of SelectColumn objects that describe the fields in each row.
        columns: An alternative to 'headers', a list of column objects that describe the fields in each row.
        tableId: The ID of the TableEntity that owns these rows
        rows:    The [Row][synapseclient.table.Row] s of this set. The index of each row value aligns with the
                 index of each header.
        etag:    Any RowSet returned from Synapse will contain the current etag of the change set. To update any
                 rows from a RowSet the etag must be provided with the POST.
    """

    @classmethod
    def from_json(cls, json):
        headers = [SelectColumn(**header) for header in json.get("headers", [])]
        rows = [cast_row(Row(**row), headers) for row in json.get("rows", [])]
        return cls(
            headers=headers,
            rows=rows,
            **{key: json[key] for key in json.keys() if key not in ["headers", "rows"]},
        )

    def __init__(self, columns=None, schema=None, **kwargs):
        if "headers" not in kwargs:
            if columns and schema:
                raise ValueError(
                    "Please only user either 'columns' or 'schema' as an argument but not both."
                )
            if columns:
                kwargs.setdefault("headers", []).extend(
                    [SelectColumn.from_column(column) for column in columns]
                )
            elif schema and isinstance(schema, Schema):
                kwargs.setdefault("headers", []).extend(
                    [SelectColumn(id=id) for id in schema["columnIds"]]
                )

        if not kwargs.get("headers", None):
            raise ValueError("Column headers must be defined to create a RowSet")
        kwargs["concreteType"] = "org.sagebionetworks.repo.model.table.RowSet"

        super(RowSet, self).__init__(schema, **kwargs)

    def _synapse_store(self, syn):
        response = super(RowSet, self)._synapse_store(syn)
        return response.get("rowReferenceSet", response)

    def _synapse_delete(self, syn):
        """
        Delete the rows in the RowSet.
        Example:
            syn.delete(syn.tableQuery('select name from %s where no_good = true' % schema1.id))
        """
        row_id_vers_generator = ((row.rowId, row.versionNumber) for row in self.rows)
        _delete_rows(syn, self.tableId, row_id_vers_generator)


class Row(DictObject):
    """
    A [row](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/Row.html) in a Table.

    Attributes:
        values:        A list of values
        rowId:         The immutable ID issued to a new row
        versionNumber: The version number of this row. Each row version is immutable, so when a row is updated a
                       new version is created.
    """

    def __init__(self, values, rowId=None, versionNumber=None, etag=None, **kwargs):
        super(Row, self).__init__()
        self.values = values
        if rowId is not None:
            self.rowId = rowId
        if versionNumber is not None:
            self.versionNumber = versionNumber
        if etag is not None:
            self.etag = etag

        # Notes that this param is only used to support forward compatibility.
        self.update(kwargs)


class PartialRow(DictObject):
    """
    This is a lower-level class for use in [PartialRowset][synapseclient.table.PartialRowset] to update individual
    cells within a table.

    Attributes:
        values:         A Mapping where:

            - The key is name of the column (or its columnId) to change in the desired row
            - The value is the new desired value for that column

        rowId:          The id of the row to be updated
        etag:           Used for updating File/Project Views([EntityViewSchema][synapseclient.table.EntityViewSchema]).
                        Not necessary for a [Schema][synapseclient.table.Schema] Table
        nameToColumnId: Optional map column names to column Ids. If this is provided, the keys of your `values`
                        Mapping will be replaced with the column ids in the `nameToColumnId` dict. Include this
                        as an argument when you are providing the column names instead of columnIds as the keys
                        to the `values` Mapping.

    Example: Using PartialRow
        It is recommended you use [from_mapping][synapseclient.table.PartialRowset.from_mapping]
        to construct partial change sets to a table.

        If you want to do the tedious parts yourself:

        To change cells in the "foo"(colId:1234) and "bar"(colId:456) columns of a row with `rowId = 5`
        Pass in with `columnIds` as key:

            PartialRow({123: 'fooVal', 456:'barVal'}, rowId)

        Pass in with a `nameToColumnId` argument. You can either manually define:

            nameToColumnId = {'foo':123, 'bar':456}

        OR if you have the result of a `tableQuery()` you can generate `nameToColumnId` using:

            query_result = syn.tableQuery("SELECT * FROM syn123")
            nameToColumnId = {col.name:col.id for col in query_result.headers}

            PartialRow({'foo': 'fooVal', 'bar':'barVal'}, rowId, nameToColumnId=nameToColumnId)
    """

    def __init__(self, values, rowId, etag=None, nameToColumnId=None):
        super(PartialRow, self).__init__()
        if not isinstance(values, collections.abc.Mapping):
            raise ValueError("values must be a Mapping")

        rowId = int(rowId)

        self.values = [
            {
                "key": nameToColumnId[x_key] if nameToColumnId is not None else x_key,
                "value": x_value,
            }
            for x_key, x_value in values.items()
        ]
        self.rowId = rowId
        if etag is not None:
            self.etag = etag


def build_table(name, parent, values):
    """
    Build a Table object

    Arguments:
        name:    The name for the Table Schema object
        parent:  The project in Synapse to which this table belongs
        values:  An object that holds the content of the tables

            - A string holding the path to a CSV file
            - A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

    Returns:
        A Table object suitable for storing

    Example:

        path = "/path/to/file.csv"
        table = build_table("simple_table", "syn123", path)
        table = syn.store(table)

        import pandas as pd

        df = pd.DataFrame(dict(a=[1, 2, 3], b=["c", "d", "e"]))
        table = build_table("simple_table", "syn123", df)
        table = syn.store(table)
    """
    test_import_pandas()
    import pandas as pd

    if not isinstance(values, pd.DataFrame) and not isinstance(values, str):
        raise ValueError("Values of type %s is not yet supported." % type(values))
    cols = as_table_columns(values)
    schema = Schema(name=name, columns=cols, parent=parent)
    headers = [SelectColumn.from_column(col) for col in cols]
    return Table(schema, values, headers=headers)


def Table(schema, values, **kwargs):
    """
    Combine a table schema and a set of values into some type of Table object
    depending on what type of values are given.

    Arguments:
        schema: A table [Schema][synapseclient.table.Schema] object or Synapse Id of Table.
        values: An object that holds the content of the tables

            - A [RowSet][synapseclient.table.RowSet]
            - A list of lists (or tuples) where each element is a row
            - A string holding the path to a CSV file
            - A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)
            - A dict which will be wrapped by a [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)

    Returns:
        A Table object suitable for storing

    Usually, the immediate next step after creating a Table object is to store it:

        table = syn.store(Table(schema, values))

    End users should not need to know the details of these Table subclasses:

    - [TableAbstractBaseClass][synapseclient.table.TableAbstractBaseClass]
    - [RowSetTable][synapseclient.table.RowSetTable]
    - [TableQueryResult][synapseclient.table.TableQueryResult]
    - [CsvFileTable][synapseclient.table.CsvFileTable]
    """

    try:
        import pandas as pd

        pandas_available = True
    except:  # noqa
        pandas_available = False

    # a RowSet
    if isinstance(values, RowSet):
        return RowSetTable(schema, values, **kwargs)

    # a list of rows
    elif isinstance(values, (list, tuple)):
        return CsvFileTable.from_list_of_rows(schema, values, **kwargs)

    # filename of a csv file
    elif isinstance(values, str):
        return CsvFileTable(schema, filepath=values, **kwargs)

    # pandas DataFrame
    elif pandas_available and isinstance(values, pd.DataFrame):
        return CsvFileTable.from_data_frame(schema, values, **kwargs)

    # dict
    elif pandas_available and isinstance(values, dict):
        return CsvFileTable.from_data_frame(schema, pd.DataFrame(values), **kwargs)

    else:
        raise ValueError(
            "Don't know how to make tables from values of type %s." % type(values)
        )


class TableAbstractBaseClass(collections.abc.Iterable, collections.abc.Sized):
    """
    Abstract base class for Tables based on different data containers.
    """

    RowMetadataTuple = collections.namedtuple(
        "RowMetadataTuple", ["row_id", "row_version", "row_etag"]
    )

    def __init__(self, schema, headers=None, etag=None):
        if isinstance(schema, Schema):
            self.schema = schema
            self.tableId = schema.id if schema and "id" in schema else None
            self.headers = (
                headers if headers else [SelectColumn(id=id) for id in schema.columnIds]
            )
            self.etag = etag
        elif isinstance(schema, str):
            self.schema = None
            self.tableId = schema
            self.headers = headers
            self.etag = etag
        else:
            ValueError("Must provide a schema or a synapse ID of a Table Entity")

    def asDataFrame(self):
        raise NotImplementedError()

    def asRowSet(self):
        return RowSet(
            headers=self.headers,
            tableId=self.tableId,
            etag=self.etag,
            rows=[row if isinstance(row, Row) else Row(row) for row in self],
        )

    def _synapse_store(self, syn):
        raise NotImplementedError()

    def _synapse_delete(self, syn):
        """
        Delete the rows that result from a table query.

        Example:
            syn.delete(syn.tableQuery('select name from %s where no_good = true' % schema1.id))
        """
        row_id_vers_generator = (
            (metadata.row_id, metadata.row_version)
            for metadata in self.iter_row_metadata()
        )
        _delete_rows(syn, self.tableId, row_id_vers_generator)

    @abc.abstractmethod
    def iter_row_metadata(self):
        """
        Iterates the table results to get row_id and row_etag. If an etag does not exist for a row, it will
        generated as (row_id, None)

        Returns:
            A generator that gives [collections.namedtuple](https://docs.python.org/3/library/collections.html#collections.namedtuple) with format (row_id, row_etag)
        """
        pass


class RowSetTable(TableAbstractBaseClass):
    """
    A Table object that wraps a RowSet.
    """

    def __init__(self, schema, rowset):
        super(RowSetTable, self).__init__(schema, etag=rowset.get("etag", None))
        self.rowset = rowset

    def _synapse_store(self, syn):
        row_reference_set = syn.store(self.rowset)
        return RowSetTable(self.schema, row_reference_set)

    def asDataFrame(self):
        test_import_pandas()
        import pandas as pd

        if any([row["rowId"] for row in self.rowset["rows"]]):
            rownames = row_labels_from_rows(self.rowset["rows"])
        else:
            rownames = None

        series = collections.OrderedDict()
        for i, header in enumerate(self.rowset["headers"]):
            series[header.name] = pd.Series(
                name=header.name,
                data=[row["values"][i] for row in self.rowset["rows"]],
                index=rownames,
            )

        return pd.DataFrame(data=series, index=rownames)

    def asRowSet(self):
        return self.rowset

    def __iter__(self):
        def iterate_rows(rows, headers):
            for row in rows:
                yield cast_values(row, headers)

        return iterate_rows(self.rowset["rows"], self.rowset["headers"])

    def __len__(self):
        return len(self.rowset["rows"])

    def iter_row_metadata(self):
        raise NotImplementedError("iter_metadata is not supported for RowSetTable")


class TableQueryResult(TableAbstractBaseClass):
    """
    An object to wrap rows returned as a result of a table query.
    The TableQueryResult object can be used to iterate over results of a query.

    Example:

        results = syn.tableQuery("select * from syn1234")
        for row in results:
            print(row)
    """

    def __init__(self, synapse, query, limit=None, offset=None, isConsistent=True):
        self.syn = synapse

        self.query = query
        self.limit = limit
        self.offset = offset
        self.isConsistent = isConsistent

        result = self.syn._queryTable(
            query=query, limit=limit, offset=offset, isConsistent=isConsistent
        )

        self.rowset = RowSet.from_json(result["queryResult"]["queryResults"])

        self.columnModels = [Column(**col) for col in result.get("columnModels", [])]
        self.nextPageToken = result["queryResult"].get("nextPageToken", None)
        self.count = result.get("queryCount", None)
        self.maxRowsPerPage = result.get("maxRowsPerPage", None)
        self.i = -1

        super(TableQueryResult, self).__init__(
            schema=self.rowset.get("tableId", None),
            headers=self.rowset.headers,
            etag=self.rowset.get("etag", None),
        )

    def _synapse_store(self, syn):
        raise SynapseError(
            "A TableQueryResult is a read only object and can't be stored in Synapse. Convert to a"
            " DataFrame or RowSet instead."
        )

    def asDataFrame(self, rowIdAndVersionInIndex=True):
        """
        Convert query result to a Pandas DataFrame.

        Arguments:
            rowIdAndVersionInIndex: Make the dataframe index consist of the row_id and row_version (and row_etag
                                    if it exists)
        """
        test_import_pandas()
        import pandas as pd

        # To turn a TableQueryResult into a data frame, we add a page of rows
        # at a time on the untested theory that it's more efficient than
        # adding a single row at a time to the data frame.

        def construct_rownames(rowset, offset=0):
            try:
                return (
                    row_labels_from_rows(rowset["rows"])
                    if rowIdAndVersionInIndex
                    else None
                )
            except KeyError:
                # if we don't have row id and version, just number the rows
                # python3 cast range to list for safety
                return list(range(offset, offset + len(rowset["rows"])))

        # first page of rows
        offset = 0
        rownames = construct_rownames(self.rowset, offset)
        offset += len(self.rowset["rows"])
        series = collections.OrderedDict()

        if not rowIdAndVersionInIndex:
            # Since we use an OrderedDict this must happen before we construct the other columns
            # add row id, verison, and etag as rows
            append_etag = False  # only useful when (not rowIdAndVersionInIndex), hooray for lazy variables!
            series["ROW_ID"] = pd.Series(
                name="ROW_ID", data=[row["rowId"] for row in self.rowset["rows"]]
            )
            series["ROW_VERSION"] = pd.Series(
                name="ROW_VERSION",
                data=[row["versionNumber"] for row in self.rowset["rows"]],
            )

            row_etag = [row.get("etag") for row in self.rowset["rows"]]
            if any(row_etag):
                append_etag = True
                series["ROW_ETAG"] = pd.Series(name="ROW_ETAG", data=row_etag)

        for i, header in enumerate(self.rowset["headers"]):
            column_name = header.name
            series[column_name] = pd.Series(
                name=column_name,
                data=[row["values"][i] for row in self.rowset["rows"]],
                index=rownames,
            )

        # subsequent pages of rows
        while self.nextPageToken:
            result = self.syn._queryTableNext(self.nextPageToken, self.tableId)
            self.rowset = RowSet.from_json(result["queryResults"])
            self.nextPageToken = result.get("nextPageToken", None)
            self.i = 0

            rownames = construct_rownames(self.rowset, offset)
            offset += len(self.rowset["rows"])

            if not rowIdAndVersionInIndex:
                # TODO: Look into why this isn't being assigned
                series["ROW_ID"].append(
                    pd.Series(
                        name="ROW_ID", data=[row["id"] for row in self.rowset["rows"]]
                    )
                )
                series["ROW_VERSION"].append(
                    pd.Series(
                        name="ROW_VERSION",
                        data=[row["version"] for row in self.rowset["rows"]],
                    )
                )
                if append_etag:
                    series["ROW_ETAG"] = pd.Series(
                        name="ROW_ETAG",
                        data=[row.get("etag") for row in self.rowset["rows"]],
                    )

            for i, header in enumerate(self.rowset["headers"]):
                column_name = header.name
                series[column_name] = pd.concat(
                    [
                        series[column_name],
                        pd.Series(
                            name=column_name,
                            data=[row["values"][i] for row in self.rowset["rows"]],
                            index=rownames,
                        ),
                    ],
                    # can't verify integrity when indices are just numbers instead of 'rowid_rowversion'
                    verify_integrity=rowIdAndVersionInIndex,
                )

        return pd.DataFrame(data=series)

    def asRowSet(self):
        # Note that as of stack 60, an empty query will omit the headers field
        # see PLFM-3014
        return RowSet(
            headers=self.headers,
            tableId=self.tableId,
            etag=self.etag,
            rows=[row for row in self],
        )

    def __iter__(self):
        return self

    def next(self):
        """
        Python 2 iterator
        """
        self.i += 1
        if self.i >= len(self.rowset["rows"]):
            if self.nextPageToken:
                result = self.syn._queryTableNext(self.nextPageToken, self.tableId)
                self.rowset = RowSet.from_json(result["queryResults"])
                self.nextPageToken = result.get("nextPageToken", None)
                self.i = 0
            else:
                raise StopIteration()
        return self.rowset["rows"][self.i]

    def __next__(self):
        """
        Python 3 iterator
        """
        return self.next()

    def __len__(self):
        return len(self.rowset["rows"])

    def iter_row_metadata(self):
        """
        Iterates the table results to get row_id and row_etag. If an etag does not exist for a row, it will
        generated as (row_id, row_version,None)

        Returns:
            A generator that gives [collections.namedtuple](https://docs.python.org/3/library/collections.html#collections.namedtuple)
            with format (row_id, row_version, row_etag)
        """
        for row in self:
            yield type(self).RowMetadataTuple(
                int(row["rowId"]), int(row["versionNumber"]), row.get("etag")
            )


class CsvFileTable(TableAbstractBaseClass):
    """
    An object to wrap a CSV file that may be stored into a Synapse table or
    returned as a result of a table query.
    """

    @classmethod
    def from_table_query(
        cls,
        synapse,
        query,
        quoteCharacter='"',
        escapeCharacter="\\",
        lineEnd=str(os.linesep),
        separator=",",
        header=True,
        includeRowIdAndRowVersion=True,
        downloadLocation=None,
    ):
        """
        Create a Table object wrapping a CSV file resulting from querying a Synapse table.
        Mostly for internal use.
        """

        download_from_table_result, path = synapse._queryTableCsv(
            query=query,
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=lineEnd,
            separator=separator,
            header=header,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion,
            downloadLocation=downloadLocation,
        )

        # A dirty hack to find out if we got back row ID and Version
        # in particular, we don't get these back from aggregate queries
        with io.open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(
                f,
                delimiter=separator,
                escapechar=escapeCharacter,
                lineterminator=lineEnd,
                quotechar=quoteCharacter,
            )
            first_line = next(reader)
        if len(download_from_table_result["headers"]) + 2 == len(first_line):
            includeRowIdAndRowVersion = True
        else:
            includeRowIdAndRowVersion = False

        self = cls(
            filepath=path,
            schema=download_from_table_result.get("tableId", None),
            etag=download_from_table_result.get("etag", None),
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=lineEnd,
            separator=separator,
            header=header,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion,
            headers=[
                SelectColumn(**header)
                for header in download_from_table_result["headers"]
            ],
        )

        return self

    @classmethod
    def from_data_frame(
        cls,
        schema,
        df,
        filepath=None,
        etag=None,
        quoteCharacter='"',
        escapeCharacter="\\",
        lineEnd=str(os.linesep),
        separator=",",
        header=True,
        includeRowIdAndRowVersion=None,
        headers=None,
        **kwargs,
    ):
        # infer columns from data frame if not specified
        if not headers:
            cols = as_table_columns(df)
            headers = [SelectColumn.from_column(col) for col in cols]

        # if the schema has no columns, use the inferred columns
        if isinstance(schema, Schema) and not schema.has_columns():
            schema.addColumns(cols)

        # convert row names in the format [row_id]_[version] or [row_id]_[version]_[etag] back to columns
        # etag is essentially a UUID
        etag_pattern = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}"
        row_id_version_pattern = re.compile(r"(\d+)_(\d+)(_(" + etag_pattern + r"))?")

        row_id = []
        row_version = []
        row_etag = []
        for row_name in df.index.values:
            m = row_id_version_pattern.match(str(row_name))
            row_id.append(m.group(1) if m else None)
            row_version.append(m.group(2) if m else None)
            row_etag.append(m.group(4) if m else None)

        # include row ID and version, if we're asked to OR if it's encoded in row names
        if includeRowIdAndRowVersion or (
            includeRowIdAndRowVersion is None and any(row_id)
        ):
            df2 = df.copy()

            cls._insert_dataframe_column_if_not_exist(df2, 0, "ROW_ID", row_id)
            cls._insert_dataframe_column_if_not_exist(
                df2, 1, "ROW_VERSION", row_version
            )
            if any(row_etag):
                cls._insert_dataframe_column_if_not_exist(df2, 2, "ROW_ETAG", row_etag)

            df = df2
            includeRowIdAndRowVersion = True

        f = None
        try:
            if not filepath:
                temp_dir = tempfile.mkdtemp()
                filepath = os.path.join(temp_dir, "table.csv")

            f = io.open(filepath, mode="w", encoding="utf-8", newline="")

            test_import_pandas()
            import pandas as pd

            if isinstance(schema, Schema):
                for col in schema.columns_to_store:
                    if col["columnType"] == "DATE":

                        def _trailing_date_time_millisecond(t):
                            if isinstance(t, str):
                                return t[:-3]

                        df[col.name] = pd.to_datetime(
                            df[col.name], errors="coerce"
                        ).dt.strftime("%s%f")
                        df[col.name] = df[col.name].apply(
                            lambda x: _trailing_date_time_millisecond(x)
                        )

            df.to_csv(
                f,
                index=False,
                sep=separator,
                header=header,
                quotechar=quoteCharacter,
                escapechar=escapeCharacter,
                lineterminator=lineEnd,
                na_rep=kwargs.get("na_rep", ""),
                float_format="%.12g",
            )
            # NOTE: reason for flat_format='%.12g':
            # pandas automatically converts int columns into float64 columns when some cells in the column have no
            # value. If we write the whole number back as a decimal (e.g. '3.0'), Synapse complains that we are writing
            # a float into a INTEGER(synapse table type) column. Using the 'g' will strip off '.0' from whole number
            # values. pandas by default (with no float_format parameter) seems to keep 12 values after decimal, so we
            # use '%.12g'.c
            # see SYNPY-267.
        finally:
            if f:
                f.close()

        return cls(
            schema=schema,
            filepath=filepath,
            etag=etag,
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=lineEnd,
            separator=separator,
            header=header,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion,
            headers=headers,
        )

    @staticmethod
    def _insert_dataframe_column_if_not_exist(
        dataframe, insert_index, col_name, insert_column_data
    ):
        # if the column already exists verify the column data is same as what we parsed
        if col_name in dataframe.columns:
            if dataframe[col_name].tolist() != insert_column_data:
                raise SynapseError(
                    (
                        "A column named '{0}' already exists and does not match the '{0}' values present in"
                        " the DataFrame's row names. Please refain from using or modifying '{0}' as a"
                        " column for your data because it is necessary for version tracking in Synapse's"
                        " tables"
                    ).format(col_name)
                )
        else:
            dataframe.insert(insert_index, col_name, insert_column_data)

    @classmethod
    def from_list_of_rows(
        cls,
        schema,
        values,
        filepath=None,
        etag=None,
        quoteCharacter='"',
        escapeCharacter="\\",
        lineEnd=str(os.linesep),
        separator=",",
        linesToSkip=0,
        includeRowIdAndRowVersion=None,
        headers=None,
    ):
        # create CSV file
        f = None
        try:
            if not filepath:
                temp_dir = tempfile.mkdtemp()
                filepath = os.path.join(temp_dir, "table.csv")

            f = io.open(filepath, "w", encoding="utf-8", newline="")

            writer = csv.writer(
                f,
                quoting=csv.QUOTE_NONNUMERIC,
                delimiter=separator,
                escapechar=escapeCharacter,
                lineterminator=lineEnd,
                quotechar=quoteCharacter,
                skipinitialspace=linesToSkip,
            )

            # if we haven't explicitly set columns, try to grab them from
            # the schema object
            if (
                not headers
                and "columns_to_store" in schema
                and schema.columns_to_store is not None
            ):
                headers = [
                    SelectColumn.from_column(col) for col in schema.columns_to_store
                ]

            # write headers?
            if headers:
                writer.writerow([header.name for header in headers])
                header = True
            else:
                header = False

            # write row data
            for row in values:
                writer.writerow(row)

        finally:
            if f:
                f.close()

        return cls(
            schema=schema,
            filepath=filepath,
            etag=etag,
            quoteCharacter=quoteCharacter,
            escapeCharacter=escapeCharacter,
            lineEnd=lineEnd,
            separator=separator,
            header=header,
            headers=headers,
            includeRowIdAndRowVersion=includeRowIdAndRowVersion,
        )

    def __init__(
        self,
        schema,
        filepath,
        etag=None,
        quoteCharacter=DEFAULT_QUOTE_CHARACTER,
        escapeCharacter=DEFAULT_ESCAPSE_CHAR,
        lineEnd=str(os.linesep),
        separator=DEFAULT_SEPARATOR,
        header=True,
        linesToSkip=0,
        includeRowIdAndRowVersion=None,
        headers=None,
    ):
        self.filepath = filepath

        self.includeRowIdAndRowVersion = includeRowIdAndRowVersion

        # CsvTableDescriptor fields
        self.linesToSkip = linesToSkip
        self.quoteCharacter = quoteCharacter
        self.escapeCharacter = escapeCharacter
        self.lineEnd = lineEnd
        self.separator = separator
        self.header = header

        super(CsvFileTable, self).__init__(schema, headers=headers, etag=etag)

        self.setColumnHeaders(headers)

    def _synapse_store(self, syn):
        copied_self = copy.copy(self)
        return copied_self._update_self(syn)

    def _update_self(self, syn):
        if isinstance(self.schema, Schema) and self.schema.get("id", None) is None:
            # store schema
            self.schema = syn.store(self.schema)
            self.tableId = self.schema.id

        result = syn._uploadCsv(
            self.filepath,
            self.schema if self.schema else self.tableId,
            updateEtag=self.etag,
            quoteCharacter=self.quoteCharacter,
            escapeCharacter=self.escapeCharacter,
            lineEnd=self.lineEnd,
            separator=self.separator,
            header=self.header,
            linesToSkip=self.linesToSkip,
        )

        upload_to_table_result = result["results"][0]

        assert upload_to_table_result["concreteType"] in (
            "org.sagebionetworks.repo.model.table.EntityUpdateResults",
            "org.sagebionetworks.repo.model.table.UploadToTableResult",
        ), "Not an UploadToTableResult or EntityUpdateResults."
        if "etag" in upload_to_table_result:
            self.etag = upload_to_table_result["etag"]
        return self

    def asDataFrame(
        self,
        rowIdAndVersionInIndex: bool = True,
        convert_to_datetime: bool = False,
        **kwargs,
    ):
        """Convert query result to a Pandas DataFrame.

        Arguments:
            rowIdAndVersionInIndex: Make the dataframe index consist of the
                                    row_id and row_version (and row_etag if it exists)
            convert_to_datetime:    If set to True, will convert all Synapse DATE columns from UNIX timestamp
                                    integers into UTC datetime objects
            kwargs:                 Additional keyword arguments to pass to
                                    pandas.read_csv via _csv_to_pandas_df. See
                                    https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
                                    for complete list of supported arguments.

        Returns:
            A Pandas dataframe with results
        """
        test_import_pandas()
        import pandas as pd

        try:
            # Handle bug in pandas 0.19 requiring quotechar to be str not unicode or newstr
            quoteChar = self.quoteCharacter

            # determine which columns are DATE columns so we can convert milisecond timestamps into datetime objects
            date_columns = []
            list_columns = []
            dtype = {}

            if self.headers is not None:
                for select_column in self.headers:
                    if select_column.columnType == "STRING":
                        # we want to identify string columns so that pandas doesn't try to
                        # automatically parse strings in a string column to other data types
                        dtype[select_column.name] = str
                    elif select_column.columnType in LIST_COLUMN_TYPES:
                        list_columns.append(select_column.name)
                    elif select_column.columnType == "DATE" and convert_to_datetime:
                        date_columns.append(select_column.name)

            return _csv_to_pandas_df(
                self.filepath,
                separator=self.separator,
                quote_char=quoteChar,
                escape_char=self.escapeCharacter,
                contain_headers=self.header,
                lines_to_skip=self.linesToSkip,
                date_columns=date_columns,
                list_columns=list_columns,
                rowIdAndVersionInIndex=rowIdAndVersionInIndex,
                dtype=dtype,
                **kwargs,
            )

        except pd.errors.ParserError:
            return pd.DataFrame()

    def asRowSet(self):
        # Extract row id and version, if present in rows
        row_id_col = None
        row_ver_col = None
        for i, header in enumerate(self.headers):
            if header.name == "ROW_ID":
                row_id_col = i
            elif header.name == "ROW_VERSION":
                row_ver_col = i

        def to_row_object(row, row_id_col=None, row_ver_col=None):
            if isinstance(row, Row):
                return row
            rowId = row[row_id_col] if row_id_col is not None else None
            versionNumber = row[row_ver_col] if row_ver_col is not None else None
            values = [
                elem for i, elem in enumerate(row) if i not in [row_id_col, row_ver_col]
            ]
            return Row(values, rowId=rowId, versionNumber=versionNumber)

        return RowSet(
            headers=[
                elem
                for i, elem in enumerate(self.headers)
                if i not in [row_id_col, row_ver_col]
            ],
            tableId=self.tableId,
            etag=self.etag,
            rows=[to_row_object(row, row_id_col, row_ver_col) for row in self],
        )

    def setColumnHeaders(self, headers):
        """
        Set the list of [SelectColumn][synapseclient.table.SelectColumn] objects that will be used to convert fields to the
        appropriate data types.

        Column headers are automatically set when querying.
        """
        if self.includeRowIdAndRowVersion:
            names = [header.name for header in headers]
            if "ROW_ID" not in names and "ROW_VERSION" not in names:
                headers = [
                    SelectColumn(name="ROW_ID", columnType="STRING"),
                    SelectColumn(name="ROW_VERSION", columnType="STRING"),
                ] + headers
        self.headers = headers

    def __iter__(self):
        def iterate_rows(filepath, headers):
            if not self.header or not self.headers:
                raise ValueError("Iteration not supported for table without headers.")

            header_name = {header.name for header in headers}
            row_metadata_headers = {"ROW_ID", "ROW_VERSION", "ROW_ETAG"}
            num_row_metadata_in_headers = len(header_name & row_metadata_headers)
            with io.open(filepath, encoding="utf-8", newline=self.lineEnd) as f:
                reader = csv.reader(
                    f,
                    delimiter=self.separator,
                    escapechar=self.escapeCharacter,
                    lineterminator=self.lineEnd,
                    quotechar=self.quoteCharacter,
                )
                csv_header = set(next(reader))
                # the number of row metadata differences between the csv headers and self.headers
                num_metadata_cols_diff = (
                    len(csv_header & row_metadata_headers) - num_row_metadata_in_headers
                )
                # we only process 2 cases:
                # 1. matching row metadata
                # 2. if metadata does not match, self.headers must not contains row metadata
                if num_metadata_cols_diff == 0 or num_row_metadata_in_headers == 0:
                    for row in reader:
                        yield cast_values(row[num_metadata_cols_diff:], headers)
                else:
                    raise ValueError(
                        "There is mismatching row metadata in the csv file and in headers."
                    )

        return iterate_rows(self.filepath, self.headers)

    def __len__(self):
        with io.open(self.filepath, encoding="utf-8", newline=self.lineEnd) as f:
            if self.header:  # ignore the header line
                f.readline()

            return sum(1 for line in f)

    def iter_row_metadata(self):
        """
        Iterates the table results to get row_id and row_etag. If an etag does not exist for a row,
        it will generated as (row_id, None)

        Returns:
            A generator that gives [collections.namedtuple](https://docs.python.org/3/library/collections.html#collections.namedtuple) with format (row_id, row_etag)
        """
        with io.open(self.filepath, encoding="utf-8", newline=self.lineEnd) as f:
            reader = csv.reader(
                f,
                delimiter=self.separator,
                escapechar=self.escapeCharacter,
                lineterminator=self.lineEnd,
                quotechar=self.quoteCharacter,
            )
            header = next(reader)

            # The ROW_... headers are always in a predefined order
            row_id_index = header.index("ROW_ID")
            row_version_index = header.index("ROW_VERSION")
            try:
                row_etag_index = header.index("ROW_ETAG")
            except ValueError:
                row_etag_index = None

            for row in reader:
                yield type(self).RowMetadataTuple(
                    int(row[row_id_index]),
                    int(row[row_version_index]),
                    row[row_etag_index] if (row_etag_index is not None) else None,
                )
