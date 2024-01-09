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
def determine_transaction_type(df: pd.DataFrame, *, download_type: str) -> pd.DataFrame:
    """
    Assign a transaction type to each record in the DataFrame based on the download type.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing transaction data.
    download_type : str
        Type of transaction ('issuances', 'retirements', 'cancellations') to determine the transaction type.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'transaction_type' column, containing assigned transaction types based on download_type.
    """

    transaction_type_mapping = {
        'issuances': 'issuance',
        'retirements': 'retirement',
        'cancellations': 'cancellation',
    }
    df['transaction_type'] = transaction_type_mapping[download_type]
    return df


@pf.register_dataframe_method
def process_apx_credits(
    df: pd.DataFrame, *, download_type: str, registry_name: str, arb: pd.DataFrame | None = None
) -> pd.DataFrame:
    """
    Process APX credits data by setting registry, determining transaction types, renaming columns,
    converting date columns, aggregating issuances (if applicable), and validating the schema.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with raw APX credits data.
    download_type : str
        Type of download ('issuances', 'retirements', etc.).
    registry_name : str
        Name of the registry for setting and mapping columns.
    arb : pd.DataFrame | None, optional
        Additional DataFrame for data merging (default is None).

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with APX credits data.
    """

    df = df.copy()

    column_mapping = load_column_mapping(
        registry_name=registry_name, download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )

    columns = {v: k for k, v in column_mapping.items()}

    data = (
        df.set_registry(registry_name=registry_name)
        .determine_transaction_type(download_type=download_type)
        .rename(columns=columns)
        .convert_to_datetime(columns=['transaction_date'])
    )

    if download_type == 'issuances':
        data = data.aggregate_issuance_transactions()

    data = data.validate(schema=credit_without_id_schema)
    if arb is not None and not arb.empty:
        data = data.merge_with_arb(arb=arb)
    return data


def harmonize_acr_status(row: pd.Series) -> str:
    """Derive single project status for CAR and ACR projects

    Raw CAR and ACR data has two status columns -- one for compliance status, one for voluntary.
    Handle and harmonize.

    Parameters
    ----------
    row : pd.Series
        A row from a pandas DataFrame

    Returns
    -------
    value : str
        The status of the project
    """
    if row['Compliance Program Status (ARB or Ecology)'] == 'Not ARB or Ecology Eligible':
        return row['Voluntary Status'].lower()
    ACR_COMPLIANCE_STATE_MAP = {
        'Listed - Active ARB Project': 'active',
        'ARB Completed': 'completed',
        'ARB Inactive': 'completed',
        'Listed - Proposed Project': 'listed',
        'Listed - Active Registry Project': 'listed',
        'ARB Terminated': 'completed',
        'Submitted': 'listed',
        'Transferred ARB or Ecology Project': 'active',
        'Listed – Active ARB Project': 'active',
    }

    return ACR_COMPLIANCE_STATE_MAP.get(
        row['Compliance Program Status (ARB or Ecology)'], 'unknown'
    )


@pf.register_dataframe_method
def add_project_url(df: pd.DataFrame, *, registry_name: str) -> pd.DataFrame:
    """
    Add a project URL to each record in the DataFrame based on the registry name and project ID.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing project data.
    registry_name : str
        Name of the registry ('american-carbon-registry', 'climate-action-reserve', 'art-trees').

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'project_url' column, containing URLs for each project.
    """

    if registry_name == 'american-carbon-registry':
        base = 'https://acr2.apx.com/mymodule/reg/prjView.asp?id1='
    elif registry_name == 'climate-action-reserve':
        base = 'https://thereserve2.apx.com/mymodule/reg/prjView.asp?id1='
    elif registry_name == 'art-trees':
        base = 'https://art.apx.com/mymodule/reg/prjView.asp?id1='

    else:
        raise ValueError(f'Unknown registry name: {registry_name}')

    df['project_url'] = base + df['project_id'].str[3:]
    return df


@pf.register_dataframe_method
def process_apx_projects(
    df: pd.DataFrame, *, credits: pd.DataFrame, registry_name: str
) -> pd.DataFrame:
    """
    Process APX projects data, including renaming, adding, and validating columns, harmonizing statuses,
    and merging with credits data.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with raw projects data.
    credits : pd.DataFrame
        DataFrame containing credits data for merging.
    registry_name : str
        Name of the registry for specific processing steps.

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with harmonized and validated APX projects data.
    """

    df = df.copy()
    credits = credits.copy()
    registry_project_column_mapping = load_registry_project_column_mapping(
        registry_name=registry_name, file_path=PROJECT_SCHEMA_UPATH
    )
    inverted_column_mapping = {value: key for key, value in registry_project_column_mapping.items()}
    protocol_mapping = load_protocol_mapping()
    inverted_protocol_mapping = load_inverted_protocol_mapping()
    data = df.rename(columns=inverted_column_mapping)
    if registry_name == 'art-trees':
        data['protocol'] = [['art-trees']] * len(data)
        data['category'] = [['forest']] * len(data)
    else:
        data = data.map_protocol(inverted_protocol_mapping=inverted_protocol_mapping).add_category(
            protocol_mapping=protocol_mapping
        )

    if registry_name == 'american-carbon-registry':
        data['status'] = data.apply(harmonize_acr_status, axis=1)
    else:
        data = data.harmonize_status_codes()

    data = (
        data.set_registry(registry_name=registry_name)
        .add_project_url(registry_name=registry_name)
        .harmonize_country_names()
        .add_is_compliance_flag()
        .add_retired_and_issued_totals(credits=credits)
        .add_first_issuance_and_retirement_dates(credits=credits)
        .add_missing_columns(schema=project_schema)
        .convert_to_datetime(columns=['listed_at'])
        .validate(schema=project_schema)
    )
    return data
