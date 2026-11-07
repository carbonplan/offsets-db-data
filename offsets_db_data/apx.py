import numpy as np  # noqa: F401
import pandas as pd
import pandas_flavor as pf

from offsets_db_data.common import (
    BERKELEY_PROJECT_TYPE_UPATH,
    CREDIT_SCHEMA_UPATH,
    PROJECT_SCHEMA_UPATH,
    load_column_mapping,
    load_inverted_protocol_mapping,
    load_protocol_mapping,
    load_registry_project_column_mapping,
    load_type_category_mapping,
)
from offsets_db_data.credits import *  # noqa: F403
from offsets_db_data.credits import harmonize_beneficiary_data
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
def strip_project_id_padding(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the zeros Greentrace puts in front of project numbers: 'ACR0731' becomes 'ACR731'.

    APX, the ARB data, the Berkeley project types and OffsetsDB all use the unpadded IDs.
    """
    df['project_id'] = df['project_id'].str.replace(r'^([A-Z]+)0+(?=\d)', r'\1', regex=True)
    return df


@pf.register_dataframe_method
def process_apx_credits(
    df: pd.DataFrame,
    *,
    download_type: str,
    registry_name: str,
    arb: pd.DataFrame | None = None,
    harmonize_beneficiary_info: bool = False,
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

    # Greentrace exports can repeat a row. A serial number names one block of credits, so a
    # repeated row with serial numbers is the same credits twice. APX issuances have no serial
    # numbers, and two identical rows there can be two issuances, so those are kept.
    if 'Credit Serial Numbers' in df:
        df = df[~(df.duplicated() & df['Credit Serial Numbers'].notna())]

    column_mapping = load_column_mapping(
        registry_name=registry_name, download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )

    columns = {v: k for k, v in column_mapping.items()}

    data = (
        df.set_registry(registry_name=registry_name)
        .determine_transaction_type(download_type=download_type)
        .rename(columns=columns)
        .strip_project_id_padding()
    )

    # APX dates mix formats ('5/27/2026 8:50:10 PM', '11/16/2011'), so keep the part before the
    # time. Greentrace's ISO timestamps ('2026-05-27T20:50:10Z') have no space and pass unchanged.
    data['transaction_date'] = data['transaction_date'].str.split().str[0]

    data = data.convert_to_datetime(columns=['transaction_date'])

    if download_type == 'issuances':
        data = data.aggregate_issuance_transactions()

    data = data.add_missing_columns(schema=credit_without_id_schema).validate(
        schema=credit_without_id_schema
    )
    if arb is not None and not arb.empty:
        data = data.merge_with_arb(arb=arb)

    if harmonize_beneficiary_info:
        data = data.pipe(
            harmonize_beneficiary_data, registry_name=registry_name, download_type=download_type
        )

    data = (
        data.add_missing_columns(schema=credit_without_id_schema)
        .convert_to_datetime(columns=['transaction_date'], format='%Y-%m-%d')
        .validate(schema=credit_without_id_schema)
    )
    return data


GREENTRACE_REGISTRIES = {'american-carbon-registry', 'art-trees'}

# Status for each (Project Status, Project Listing Status) pair in ACR and ART's Greentrace
# exports. Most pairs carry a label of the APX status table these replace; None is an empty
# listing status.
# TODO(Grayson): confirm 'Inactive' as completed, and fill in the placeholders.
GREENTRACE_STATUS = {
    # labels of the APX table
    ('Active', 'Listed'): 'listed',
    ('Active', 'Registered'): 'registered',
    ('Active', 'Listed - Active ARB Project'): 'active',
    ('Active', 'Listed - Proposed Project'): 'listed',
    ('Active', 'Listed - Active Registry Project'): 'listed',
    ('Transferred', 'Transferred ARB Project'): 'active',
    ('Completed', None): 'completed',
    ('Canceled', None): 'canceled',
    ('Terminated', None): 'completed',
    # the status most of these projects had in the June 2026 release
    ('Inactive', None): 'completed',
    # placeholders
    ('Active', 'Listed - Active ECY Project'): 'unknown',
    ('Active', 'Listed - Active Registry Renewal'): 'unknown',
    ('Canceled', 'Registered'): 'unknown',
    ('Completed', 'Registered'): 'unknown',
}


@pf.register_dataframe_method
def harmonize_greentrace_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set 'status' from Greentrace's 'Project Status' and 'Project Listing Status' columns through
    GREENTRACE_STATUS. A pair that isn't in the table is 'unknown'.
    """
    listings = (
        df['Project Listing Status']
        .astype(object)
        .where(df['Project Listing Status'].notna(), None)
    )
    df['status'] = [
        GREENTRACE_STATUS.get((status, listing), 'unknown')
        for status, listing in zip(df['Project Status'], listings)
    ]
    return df


# TODO: update project url
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
    inverted_protocol_mapping = load_inverted_protocol_mapping()
    protocol_mapping = load_protocol_mapping()
    type_category_mapping = load_type_category_mapping()
    data = df.rename(columns=inverted_column_mapping).strip_project_id_padding()
    if registry_name == 'art-trees':
        data['protocol'] = [['art-trees']] * len(data)

    else:
        data = data.map_protocol(inverted_protocol_mapping=inverted_protocol_mapping)

    if registry_name in GREENTRACE_REGISTRIES:
        data = data.harmonize_greentrace_status()
    else:
        data = data.harmonize_status_codes()

    data = (
        data.set_registry(registry_name=registry_name)
        .add_project_url(registry_name=registry_name)
        .harmonize_country_names()
        .infer_project_type()
        .override_project_types(
            override_data_path=BERKELEY_PROJECT_TYPE_UPATH, source_str='berkeley'
        )
        .add_category(
            type_category_mapping=type_category_mapping,
            protocol_mapping=protocol_mapping,
        )  # category derived from protocol; project_type is independent
        .map_project_type_to_display_name(type_category_mapping=type_category_mapping)
        .add_is_compliance_flag()
        .add_retired_and_issued_totals(credits=credits)
        .add_first_issuance_and_retirement_dates(credits=credits)
        .add_missing_columns(schema=project_schema)
        .convert_to_datetime(columns=['listed_at'])
        .validate(schema=project_schema)
    )
    return data
