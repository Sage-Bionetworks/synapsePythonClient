import logging
from copy import deepcopy
from time import perf_counter
from typing import Any, Optional, Union

import numpy as np
import pandas as pd
from pandarallel import pandarallel  # type: ignore

# pylint:disable=no-name-in-module
from pandas._libs.parsers import STR_NA_VALUES  # type: ignore

STR_NA_VALUES_FILTERED = deepcopy(STR_NA_VALUES)

try:
    STR_NA_VALUES_FILTERED.remove("None")
except KeyError:
    pass

logger = logging.getLogger(__name__)


def read_csv(
    path_or_buffer: str,
    keep_default_na: bool = False,
    encoding: str = "utf8",
    **load_args: Any,
) -> pd.DataFrame:
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
    na_values = load_args.pop(
        "na_values", STR_NA_VALUES_FILTERED if not keep_default_na else None
    )
    return pd.read_csv(  # type: ignore
        path_or_buffer,
        na_values=na_values,
        keep_default_na=keep_default_na,
        encoding=encoding,
        **load_args,
    )


def trim_commas_df(
    dataframe: pd.DataFrame,
    allow_na_values: Optional[bool] = False,
) -> pd.DataFrame:
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


def convert_ints(string: str) -> Union[np.int64, bool]:
    """
    Lambda function to convert a string to an integer if possible, otherwise returns False
    Args:
        string: string to attempt conversion to int
    Returns:
        string converted to type int if possible, otherwise False
    """
    if isinstance(string, str) and str.isdigit(string):
        return np.int64(string)
    return False


def find_and_convert_ints(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Find strings that represent integers and convert to type int
    Args:
        dataframe: dataframe with nulls masked as empty strings
    Returns:
        ints: dataframe with values that were converted to type int
        is_int: dataframe with boolean values indicating which cells were converted to type int

    """
    # pylint: disable=unnecessary-lambda
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
    is_int = ints.map(pd.api.types.is_integer)  # type:ignore

    assert isinstance(ints, pd.DataFrame)
    assert isinstance(is_int, pd.DataFrame)

    return ints, is_int


def convert_floats(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Convert strings that represent floats to type float
    Args:
        dataframe: dataframe with nulls masked as empty strings
    Returns:
        float_df: dataframe with values that were converted to type float. Columns are type object
    """
    # create a separate copy of the manifest
    # before beginning conversions to store float values
    float_df = deepcopy(dataframe)

    # convert strings to numerical dtype (float) if possible, preserve non-numerical strings
    for col in dataframe.columns:
        float_df[col] = pd.to_numeric(float_df[col], errors="coerce").astype("object")

        # replace values that couldn't be converted to float with the original str values
        float_df[col].fillna(dataframe[col][float_df[col].isna()], inplace=True)

    return float_df


def load_df(
    file_path: str,
    preserve_raw_input: bool = True,
    data_model: bool = False,
    allow_na_values: bool = False,
    **load_args: Any,
) -> pd.DataFrame:
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
    # start performance timer
    t_load_df = perf_counter()

    # Read CSV to df as type specified in kwargs
    org_df = read_csv(file_path, encoding="utf8", **load_args)  # type: ignore
    if not isinstance(org_df, pd.DataFrame):
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
        logger.debug(f"Load Elapsed time {perf_counter()-t_load_df}")
        return org_df

    ints, is_int = find_and_convert_ints(org_df)

    float_df = convert_floats(org_df)

    # Store values that were converted to type int in the final dataframe
    processed_df = float_df.mask(is_int, other=ints)

    logger.debug(f"Load Elapsed time {perf_counter()-t_load_df}")
    return processed_df
