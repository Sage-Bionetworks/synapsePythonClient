import synapseclient
from synapseclient import File, Project, Folder, Table, Schema, Link, Wiki, Entity, Activity
import sys
import pandas as pd

# 1: function to open csv and tsv files that returns dataframes
# 2: function to returns summary statistics


def _open_entity_as_df(syn, entity: str) -> pd.DataFrame:
    """
    :param syn: synapse object
    :param entity: a synapse entity to be extracted and converted into a dataframe
    :return: a pandas DataFrame
    """

    synapse_file = syn.get(entity)

    df = None

    try:
        df = pd.read_csv(synapse_file.path)
    except: # needs to be narrowed down
        print("Please provide a valid file.")

    return df