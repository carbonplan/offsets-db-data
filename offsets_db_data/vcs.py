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
def generate_vcs_project_ids(df: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    df['project_id'] = prefix + df['ID'].astype(str)
    return df


@pf.register_dataframe_method
def determine_vcs_transaction_type(df: pd.DataFrame, *, date_column: str) -> pd.DataFrame:
    # Verra doesn't have a transaction type column, and doesn't differentitate between retirements and cancelattions
    # So we'll use the date column to determine whether a transaction is a retirement or issuance and set the
    # transaction type accordingly
    df['transaction_type'] = df[date_column].apply(
        lambda x: 'retirement' if pd.notnull(x) else 'issuance'
    )
    return df


@pf.register_dataframe_method
def set_vcs_transaction_dates(
    df: pd.DataFrame, *, date_column: str, fallback_column: str
) -> pd.DataFrame:
    df['transaction_date'] = df[date_column].where(df[date_column].notnull(), df[fallback_column])
    return df


@pf.register_dataframe_method
def set_vcs_vintage_year(df: pd.DataFrame, *, date_column: str) -> pd.DataFrame:
    df[date_column] = pd.to_datetime(df[date_column], format='%d/%m/%Y', utc=True)
    df['vintage'] = df[date_column].dt.year
    return df


@pf.register_dataframe_method
def calculate_vcs_issuances(df: pd.DataFrame) -> pd.DataFrame:
    """Logic to calculate verra transactions from prepocessed transaction data

    Verra allows rolling/partial issuances. This requires inferring vintage issuance from `Total Vintage Quantity`
    """

    df_issuance = df.sort_values('transaction_date').drop_duplicates(
        ['vintage', 'project_id', 'Total Vintage Quantity'], keep='first'
    )

    df_issuance = df_issuance.rename(columns={'Total Vintage Quantity': 'quantity'})

    df_issuance['transaction_type'] = 'issuance'

    return df_issuance


@pf.register_dataframe_method
def calculate_vcs_retirements(df: pd.DataFrame) -> pd.DataFrame:
    """retirements + cancelations, but data doesnt allow us to distinguish the two"""
    retirements = df[df['transaction_type'] != 'issuance']
    retirements = retirements.rename(columns={'Quantity Issued': 'quantity'})
    return retirements


@pf.register_dataframe_method
def process_vcs_credits(
    df: pd.DataFrame,
    *,
    download_type: str = 'transactions',
    registry_name: str = 'verra',
    prefix: str = 'VCS',
    arb: pd.DataFrame | None = None,
) -> pd.DataFrame:
    df = df.copy()
    data = (
        df.set_registry(registry_name=registry_name)
        .generate_vcs_project_ids(prefix=prefix)
        .determine_vcs_transaction_type(date_column='Retirement/Cancellation Date')
        .set_vcs_transaction_dates(
            date_column='Retirement/Cancellation Date', fallback_column='Issuance Date'
        )
        .clean_and_convert_numeric_columns(columns=['Total Vintage Quantity', 'Quantity Issued'])
        .set_vcs_vintage_year(date_column='Vintage End')
        .convert_to_datetime(columns=['transaction_date'])
    )

    issuances = data.calculate_vcs_issuances()
    retirements = data.calculate_vcs_retirements()

    column_mapping = load_column_mapping(
        registry_name=registry_name, download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )

    columns = {v: k for k, v in column_mapping.items()}

    merged_df = pd.concat([issuances, retirements]).reset_index(drop=True).rename(columns=columns)

    issuances = merged_df.aggregate_issuance_transactions()
    retirements = merged_df[merged_df['transaction_type'].str.contains('retirement')]
    data = (
        pd.concat([issuances, retirements])
        .reset_index(drop=True)
        .validate(schema=credit_without_id_schema)
    )

    if arb is not None and not arb.empty:
        data = data.merge_with_arb(arb=arb)

    return data


@pf.register_dataframe_method
def add_vcs_compliance_projects(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add details about two compliance projects to projects database.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame containing project data with a 'project_id' column.

    Returns
    --------
    df: pd.DataFrame
        A pandas DataFrame with two additional rows, describing two projects from the mostly unused Verra compliance
        registry portal.
    """

    vcs_project_dicts = [
        {
            'project_id': 'VCSOPR2',
            'name': 'Corinth Abandoned Mine Methane Recovery Project',
            'protocol': ['arb-mine-methane'],
            'category': ['mine-methane'],
            'proponent': 'Keyrock Energy LLC',
            'country': 'United States',
            'status': 'registered',
            'is_compliance': True,
            'registry': 'verra',
            'project_url': 'https://registry.verra.org/app/projectDetail/VCS/2265',
        },
        {
            'project_id': 'VCSOPR10',
            'name': 'Blue Source-Alford Improved Forest Management Project',
            'protocol': ['arb-forest'],
            'category': ['forest'],
            'proponent': 'Ozark Regional Land Trust',
            'country': 'United States',
            'status': 'registered',
            'is_compliance': True,
            'registry': 'verra',
            'project_url': 'https://registry.verra.org/app/projectDetail/VCS/2271',
        },
    ]
    vcs_projects = pd.DataFrame(vcs_project_dicts)
    return pd.concat([df, vcs_projects], ignore_index=True)


@pf.register_dataframe_method
def add_vcs_project_url(df: pd.DataFrame) -> pd.DataFrame:
    """Create url based on verra project id"""
    df['project_url'] = (
        'https://registry.verra.org/app/projectDetail/VCS/' + df['project_id'].str[3:]
    )
    return df


@pf.register_dataframe_method
def add_vcs_project_id(df: pd.DataFrame) -> pd.DataFrame:
    df['project_id'] = df['project_id'].apply(lambda x: f'VCS{str(x)}')
    return df


@pf.register_dataframe_method
def process_vcs_projects(
    df: pd.DataFrame,
    *,
    credits: pd.DataFrame,
    registry_name: str = 'verra',
    download_type: str = 'projects',
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
        .add_vcs_project_id()
        .add_vcs_project_url()
        .harmonize_country_names()
        .harmonize_status_codes()
        .map_protocol(inverted_protocol_mapping=inverted_protocol_mapping)
        .add_category(protocol_mapping=protocol_mapping)
        .add_is_compliance_flag()
        .add_vcs_compliance_projects()
        .add_retired_and_issued_totals(credits=credits)
        .add_first_issuance_and_retirement_dates(credits=credits)
        .add_missing_columns(columns=project_schema.columns.keys())
        .convert_to_datetime(columns=['listed_at'])
        .validate(schema=project_schema)
    )

    return data
