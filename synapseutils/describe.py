import synapseclient
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
    :param mode: string defining the return value.  Can be either 'dict' or 'string'
    :return: see param mode
    """

    stats = {}
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
        print(stats)
    else:
        return stats