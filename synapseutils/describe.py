import synapseclient
import pandas as pd
from os import path
from collections import defaultdict
from json import dumps


def _open_entity_as_df(syn, entity: str) -> pd.DataFrame:
    """
    Gets a csv or tsv Synapse entity and returns it as a dataframe
    :param syn: synapse object
    :param entity: a synapse entity to be extracted and converted into a dataframe
    :return: a pandas DataFrame if flow of execution is successful; None if not.
    """

    dataset = None

    try:
        entity = syn.get(entity)
        name, format = path.splitext(entity.path)
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


def _describe_wrapper(df: pd.DataFrame, mode: str = 'string'):
    """
    Returns the mode, min, max, mean, and dtype of each column in a dataframe
    :param df: pandas dataframe from the csv or tsv file
    :param mode: string defining the return value.  Can be either 'dict' or 'string'
    :return: see param mode
    """

    stats = defaultdict(dict)

    for column in df.columns:
        stats[column] = {}
        try:
            if pd.api.types.is_numeric_dtype(df[column].dtype):
                stats[column]['mode'] = df[column].mode()[0]
                stats[column]['min'] = df[column].min()
                stats[column]['max'] = df[column].max()
                stats[column]['mean'] = df[column].mean()
                stats[column]['dtype'] = df[column].dtype
            else:
                stats[column]['mode'] = df[column].mode()[0]
                stats[column]['dtype'] = df[column].dtype

        except TypeError:
            print("Invalid column type.")

    if mode == 'string':
        print(dumps(stats, indent=2, default=str))
    else:
        return stats

def describe(syn, entity: str, mode: str = 'string'):
    """
    Synapse_describe gets a synapse entity and returns summary statistics about it
    :param syn: synapse object
    :param entity: synapse id of the entity to be described
    :param mode: how it should be returned (string or object)
    :return: if dataset is valid, returns either a string or object; otherwise None
    """
    df = _open_entity_as_df(syn=syn, entity=entity)

    if df is None:
        return None

    return _describe_wrapper(df, mode)
