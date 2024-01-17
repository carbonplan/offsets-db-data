import json
import typing
from collections import defaultdict

import numpy as np
import pandas as pd
import pandas_flavor as pf
import pandera as pa
import upath

CREDIT_SCHEMA_UPATH = (
    upath.UPath(__file__).parents[0] / 'configs' / 'credits-raw-columns-mapping.json'
)

PROTOCOL_MAPPING_UPATH = upath.UPath(__file__).parents[0] / 'configs' / 'all-protocol-mapping.json'
PROJECT_SCHEMA_UPATH = (
    upath.UPath(__file__).parents[0] / 'configs' / 'projects-raw-columns-mapping.json'
)


def load_registry_project_column_mapping(
    *, registry_name: str, file_path: upath.UPath = PROJECT_SCHEMA_UPATH
) -> dict:
    with open(file_path) as file:
        data = json.load(file)

    mapping: dict = {}
    for key1, value_dict in data.items():
        for key2, value in value_dict.items():
            if key2 not in mapping:
                mapping[key2] = {}
            if value:
                mapping[key2][key1] = value
    return mapping[registry_name]


def load_protocol_mapping(path: upath.UPath = PROTOCOL_MAPPING_UPATH) -> dict:
    return json.loads(path.read_text())


def load_inverted_protocol_mapping() -> dict:
    protocol_mapping = load_protocol_mapping()
    store = defaultdict(list)
    for protocol_str, metadata in protocol_mapping.items():
        for known_string in metadata.get('known-strings', []):
            store[known_string].append(protocol_str)

    return store


def load_column_mapping(*, registry_name: str, download_type: str, mapping_path: str) -> dict:
    with open(mapping_path) as f:
        registry_credit_column_mapping = json.load(f)
    return registry_credit_column_mapping[registry_name][download_type]


@pf.register_dataframe_method
def set_registry(df: pd.DataFrame, registry_name: str) -> pd.DataFrame:
    """
    Set the registry name for each record in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    registry_name : str
        Name of the registry to set.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'registry' column set to the specified registry name."""

    df['registry'] = registry_name
    return df


@pf.register_dataframe_method
def convert_to_datetime(
    df: pd.DataFrame, *, columns: list, utc: bool = True, **kwargs: typing.Any
) -> pd.DataFrame:
    """
    Convert specified columns in the DataFrame to datetime format.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    columns : list
        List of column names to convert to datetime.
    utc : bool, optional
        Whether to convert to UTC (default is True).
    **kwargs : typing.Any
        Additional keyword arguments passed to pd.to_datetime.

    Returns
    -------
    pd.DataFrame
        DataFrame with specified columns converted to datetime format.
    """

    for column in columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], utc=utc, **kwargs).dt.normalize()
        else:
            raise KeyError(f"The column '{column}' is missing.")
    return df


@pf.register_dataframe_method
def add_missing_columns(df: pd.DataFrame, *, schema: pa.DataFrameSchema) -> pd.DataFrame:
    """
    Add any missing columns to the DataFrame and initialize them with None.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    schema : pa.DataFrameSchema
        Pandera schema to validate against.


    Returns
    -------
    pd.DataFrame
        DataFrame with all specified columns, adding missing ones initialized to None.
    """

    default_values = {
        np.dtype('int64'): 0,
        np.dtype('int32'): 0,
        np.dtype('float64'): 0.0,
        np.dtype('float32'): 0.0,
        np.dtype('O'): None,
        np.dtype('<U'): None,
        np.dtype('U'): None,
        np.dtype('bool'): False,
        np.dtype('<M8[ns]'): pd.NaT,  # datetime64[ns]
    }

    for column, value in schema.columns.items():
        dtype = value.dtype.type
        if column not in df.columns:
            default_value = default_values.get(dtype, None)
            df[column] = pd.Series([default_value] * len(df), index=df.index, dtype=dtype)
    return df


@pf.register_dataframe_method
def validate(df: pd.DataFrame, schema: pa.DataFrameSchema) -> pd.DataFrame:
    """
    Validate the DataFrame against a given Pandera schema.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    schema : pa.DataFrameSchema
        Pandera schema to validate against.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns sorted according to the schema and validated against it.
    """

    results = schema.validate(df)
    keys = sorted(list(schema.columns.keys()))
    results = results[keys]

    return results


@pf.register_dataframe_method
def clean_and_convert_numeric_columns(df: pd.DataFrame, *, columns: list[str]) -> pd.DataFrame:
    """
    Clean and convert specified columns to numeric format in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    columns : list[str]
        List of column names to clean and convert to numeric format.

    Returns
    -------
    pd.DataFrame
        DataFrame with specified columns cleaned (removing commas) and converted to numeric format.
    """

    for column in columns:
        df[column] = df[column].str.replace(',', '', regex=True)
        df[column] = pd.to_numeric(df[column], errors='coerce')
    return df
