from collections import defaultdict
import json
import os

import synapseclient
from synapseclient import table


def _open_entity_as_df(syn, entity: str):
    """
    Gets a csv or tsv Synapse entity and returns it as a dataframe
    :param syn: synapse object
    :param entity: a synapse entity to be extracted and converted into a dataframe
    :return: a pandas DataFrame if flow of execution is successful; None if not.
    """
    table.test_import_pandas()
    import pandas as pd

    dataset = None

    try:
        entity = syn.get(entity)
        name, format = os.path.splitext(entity.path)
    except synapseclient.core.exceptions.SynapseHTTPError:
        print(str(entity) + " is not a valid Synapse id")
        return dataset  # its value is None here

    if format == ".csv":
        dataset = pd.read_csv(entity.path)
    elif format == ".tsv":
        dataset = pd.read_csv(entity.path, sep='\t')
    else:
        print("File type not supported.")

    return dataset


def _describe_wrapper(df) -> dict:
    """
    Returns the mode, min, max, mean, and dtype of each column in a dataframe
    :param df: pandas dataframe from the csv or tsv file
    :return: see param mode
    """
    table.test_import_pandas()
    import pandas as pd

    stats = defaultdict(dict)

    for column in df.columns:
        stats[column] = {}
        try:
            if pd.api.types.is_numeric_dtype(df[column].dtype):
                stats[column]['mode'] = df[column].mode()[0]
                stats[column]['min'] = df[column].min()
                stats[column]['max'] = df[column].max()
                stats[column]['mean'] = df[column].mean()
                stats[column]['dtype'] = df[column].dtype.name
            else:
                stats[column]['mode'] = df[column].mode()[0]
                stats[column]['dtype'] = df[column].dtype.name

        except TypeError:
            print("Invalid column type.")

    return stats


def describe(syn, entity: str):
    """
    Gets a synapse entity and returns summary statistics about it
    :param syn: synapse object
    :param entity: synapse id of the entity to be described
    :return: if dataset is valid, returns either a string or object; otherwise None
    """
    df = _open_entity_as_df(syn=syn, entity=entity)

    if df is None:
        return None

    stats = _describe_wrapper(df)
    print(json.dumps(stats, indent=2, default=str))
    return stats
