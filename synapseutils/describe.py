import synapseclient
from synapseclient import File, Project, Folder, Table, Schema, Link, Wiki, Entity, Activity
import sys
import pandas as pd


def _open_entity_as_df(syn, entity: str) -> pd.DataFrame:
    """
    :param syn: synapse object
    :param entity: a synapse entity to be extracted and converted into a dataframe
    :return: a pandas DataFrame if flow of execution is successful; None if not.
    """

    dataset = None

    try:
        entity = syn.get(entity)
        format = entity.path.split(".")[-1]
    except synapseclient.core.exceptions.SynapseHTTPError:
        print(str(entity) + " is not a valid Synapse id")
        return dataset  # its value is None here

    print(format)

    if format == "csv":
        dataset = pd.read_csv(entity.path)
    elif format == "tsv":
        dataset = pd.read_csv(entity.path, sep='\\t')
    else:
        print("File type not supported.")

    return dataset


def synapse_describe(df: pd.DataFrame, mode: str = 'string'):
    """
    :param df: pandas dataframe from the csv or tsv file
    :param mode: string defining the return value.  Can be either 'object' or 'string'
    :return: see param mode
    """
    pass