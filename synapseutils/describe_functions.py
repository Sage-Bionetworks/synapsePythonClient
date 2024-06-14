import json
import os
import sys
import typing
from collections import defaultdict

import synapseclient
from synapseclient import table


def _open_entity_as_df(syn, entity: str):
    """
    Gets a csv or tsv Synapse entity and returns it as a dataframe

    Arguments:
        syn:    A [Synapse][synapseclient.client.Synapse] object
        entity: A Synapse [Entity][synapseclient.entity.Entity] to be extracted and converted into a dataframe

    Returns:
        A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)
        if flow of execution is successful; None if not.
    """
    table.test_import_pandas()
    import pandas as pd

    dataset = None

    try:
        entity = syn.get(entity)
        name, format = os.path.splitext(entity.path)
    except synapseclient.core.exceptions.SynapseHTTPError:
        syn.logger.error(str(entity) + " is not a valid Synapse id")
        return dataset  # its value is None here

    if format == ".csv":
        dataset = pd.read_csv(entity.path)
    elif format == ".tsv":
        dataset = pd.read_csv(entity.path, sep="\t")
    else:
        syn.logger.info("File type not supported.")

    return dataset


def _describe_wrapper(df) -> dict:
    """
    Returns the mode, min, max, mean, and dtype of each column in a dataframe

    Arguments:
        df: A [Pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe) from the csv or tsv file

    Returns:
        See param mode
    """
    table.test_import_pandas()
    import pandas as pd

    stats = defaultdict(dict)

    for column in df.columns:
        stats[column] = {}
        try:
            if pd.api.types.is_numeric_dtype(df[column].dtype):
                stats[column]["mode"] = df[column].mode()[0]
                stats[column]["min"] = df[column].min()
                stats[column]["max"] = df[column].max()
                stats[column]["mean"] = df[column].mean()
                stats[column]["dtype"] = df[column].dtype.name
            else:
                stats[column]["mode"] = df[column].mode()[0]
                stats[column]["dtype"] = df[column].dtype.name

        except TypeError:
            print("Invalid column type.", file=sys.stderr)

    return stats


def describe(syn, entity: str) -> typing.Union[dict, None]:
    """
    Gets a synapse entity and returns summary statistics about it.

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        entity: synapse id of the entity to be described

    Example: Using this function
        Describing columns of a table

            import synapseclient
            import synapseutils
            syn = synapseclient.login()
            statistics = synapseutils(syn, entity="syn123")
            print(statistics)
            {
                "column1": {
                    "dtype": "object",
                    "mode": "FOOBAR"
                },
                "column2": {
                    "dtype": "int64",
                    "mode": 1,
                    "min": 1,
                    "max": 2,
                    "mean": 1.4
                },
                "column3": {
                    "dtype": "bool",
                    "mode": false,
                    "min": false,
                    "max": true,
                    "mean": 0.5
                }
            }

    Returns:
        A dict if the dataset is valid; None if not.
    """
    df = _open_entity_as_df(syn=syn, entity=entity)

    if df is None:
        return None

    stats = _describe_wrapper(df)
    syn.logger.info(json.dumps(stats, indent=2, default=str))
    return stats
