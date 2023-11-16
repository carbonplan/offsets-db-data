import json
from collections import defaultdict

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
    df['registry'] = registry_name
    return df


@pf.register_dataframe_method
def convert_to_datetime(
    df: pd.DataFrame, *, columns: list, date_format: str = 'mixed', utc: bool = True
) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], format=date_format, utc=utc)
        else:
            raise KeyError(f"The column '{column}' is missing.")
    return df


@pf.register_dataframe_method
def add_missing_columns(df: pd.DataFrame, *, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df.loc[:, column] = None
    return df


@pf.register_dataframe_method
def validate(df: pd.DataFrame, schema: pa.DataFrameSchema) -> pd.DataFrame:
    results = schema.validate(df)
    keys = sorted(list(schema.columns.keys()))
    results = results[keys]

    return results


@pf.register_dataframe_method
def clean_and_convert_numeric_columns(df: pd.DataFrame, *, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        df[column] = df[column].str.replace(',', '', regex=True)
        df[column] = pd.to_numeric(df[column], errors='coerce')
    return df
