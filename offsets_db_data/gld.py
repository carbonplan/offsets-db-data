import ast

import numpy as np  # noqa: F401
import pandas as pd
import pandas_flavor as pf

from offsets_db_data.common import (
    CREDIT_SCHEMA_UPATH,
    PROJECT_SCHEMA_UPATH,
    load_column_mapping,
    load_inverted_protocol_mapping,
    load_protocol_mapping,
    load_registry_project_column_mapping,
)
from offsets_db_data.credits import *  # noqa: F403
from offsets_db_data.models import credit_without_id_schema, project_schema
from offsets_db_data.projects import *  # noqa: F403


@pf.register_dataframe_method
def determine_gld_transaction_type(df: pd.DataFrame, *, download_type: str) -> pd.DataFrame:
    transaction_type_mapping = {'issuances': 'issuance', 'retirements': 'retirement'}
    df['transaction_type'] = transaction_type_mapping[download_type]
    return df


@pf.register_dataframe_method
def add_gld_project_id_from_credits(df: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    df['project'] = df['project'].apply(lambda x: x if isinstance(x, dict) else ast.literal_eval(x))
    df['project_id'] = prefix + df['project'].apply(
        lambda x: x.get('sustaincert_id', np.nan)
    ).astype(str)
    return df


@pf.register_dataframe_method
def process_gld_credits(
    df: pd.DataFrame,
    *,
    download_type: str,
    registry_name: str = 'gold-standard',
    prefix: str = 'GLD',
    arb: pd.DataFrame | None = None,
) -> pd.DataFrame:
    df = df.copy()
    column_mapping = load_column_mapping(
        registry_name=registry_name, download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )

    columns = {v: k for k, v in column_mapping.items()}
    data = (
        df.rename(columns=columns)
        .set_registry(registry_name=registry_name)
        .determine_gld_transaction_type(download_type=download_type)
        .add_gld_project_id_from_credits(prefix=prefix)
    )

    if download_type == 'issuances':
        data = data.aggregate_issuance_transactions()

    data = data.convert_to_datetime(columns=['transaction_date']).validate(
        schema=credit_without_id_schema
    )

    if arb is not None and not arb.empty:
        data = data.merge_with_arb(arb=arb)

    return data


@pf.register_dataframe_method
def add_gld_project_url(df: pd.DataFrame) -> pd.DataFrame:
    """Add url for gold standard projects

    gs project ids are different from the id used in gold standard urls.
    """
    df['project_url'] = 'https://registry.goldstandard.org/projects/details/' + df['id'].apply(str)
    return df


@pf.register_dataframe_method
def add_gld_project_id(df: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    df['project_id'] = df['project_id'].apply(lambda x: f'{prefix}{str(x)}')
    return df


@pf.register_dataframe_method
def process_gld_projects(
    df: pd.DataFrame,
    *,
    credits: pd.DataFrame,
    registry_name: str = 'gold-standard',
    prefix: str = 'GLD',
) -> pd.DataFrame:
    df = df.copy()
    credits = credits.copy()

    registry_project_column_mapping = load_registry_project_column_mapping(
        registry_name=registry_name, file_path=PROJECT_SCHEMA_UPATH
    )
    inverted_column_mapping = {value: key for key, value in registry_project_column_mapping.items()}
    protocol_mapping = load_protocol_mapping()
    inverted_protocol_mapping = load_inverted_protocol_mapping()
    data = (
        df.rename(columns=inverted_column_mapping)
        .set_registry(registry_name=registry_name)
        .add_gld_project_id(prefix=prefix)
        .add_gld_project_url()
        .harmonize_country_names()
        .harmonize_status_codes()
        .map_protocol(inverted_protocol_mapping=inverted_protocol_mapping)
        .add_category(protocol_mapping=protocol_mapping)
        .add_is_compliance_flag()
        .add_retired_and_issued_totals(credits=credits)
        .add_first_issuance_and_retirement_dates(credits=credits)
        .add_missing_columns(columns=project_schema.columns.keys())
        .convert_to_datetime(columns=['listed_at'])
        .validate(schema=project_schema)
    )
    return data
