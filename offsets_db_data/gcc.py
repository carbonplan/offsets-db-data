import datetime

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
def modify_gcc_project_id(df: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    """
    Modify the project IDs in the DataFrame by replacing 'S' with a specified prefix.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing GCC project data.
    prefix : str
        Prefix to replace 'S' in the project ID.

    Returns
    -------
    pd.DataFrame
        DataFrame with modified 'project_id' column.
    """

    df['project_id'] = df['project_id'].str.replace('S', prefix, regex=False)
    return df


@pf.register_dataframe_method
def set_gcc_vintage_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the vintage year for GCC projects by extracting the year from a 'vintage' column containing a date range or a single year.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing GCC project data.

    Returns
    -------
    pd.DataFrame
        DataFrame with the 'vintage' column updated to only contain the year.
    """

    df['vintage'] = df['vintage'].apply(
        lambda vintage: vintage.split(' - ')[-1] if ' - ' in vintage else vintage
    )
    return df


@pf.register_dataframe_method
def set_gcc_issuance_transaction_type_and_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the transaction type as 'issuance' for all records and set the transaction date to None for GCC issuance transactions.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing GCC issuance data.

    Returns
    -------
    pd.DataFrame
        DataFrame with 'transaction_type' set to 'issuance' and 'transaction_date' set to None.
    """

    df['transaction_type'] = 'issuance'
    df['transaction_date'] = None  # We don't have a date for issuance transactions
    return df


@pf.register_dataframe_method
def add_gcc_project_id(df: pd.DataFrame, *, projects: pd.DataFrame) -> pd.DataFrame:
    """
    Map project names to project IDs using an external DataFrame containing project details.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with GCC credits data containing 'project_name' column.
    projects : pd.DataFrame
        DataFrame containing 'project_id' and 'name' columns for mapping.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'project_id' column mapped from project names.
    """

    projects_dict_list = projects[['project_id', 'name']].to_dict(orient='records')
    result_dict = {d['name']: d['project_id'] for d in projects_dict_list}

    # rename the project_id column to project_name
    # df = df.rename(columns={'project_id': 'original_project_id'})
    df['project_id'] = df['project_name'].map(result_dict)
    return df


@pf.register_dataframe_method
def set_gcc_retirement_transaction_type_and_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the transaction type as 'retirement' for all records and convert UNIX timestamps in 'retirement_date' to formatted date strings.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing GCC retirement data.

    Returns
    -------
    pd.DataFrame
        DataFrame with 'transaction_type' set to 'retirement' and 'transaction_date' converted from UNIX timestamp.
    """

    df['transaction_type'] = 'retirement'
    df['transaction_date'] = df['retirement_date'].apply(
        lambda unix_time: datetime.datetime.fromtimestamp(unix_time / 1000).strftime(
            '%Y-%m-%d %H:%M:%S'
        )
        if pd.notnull(unix_time)
        else None
    )
    return df


@pf.register_dataframe_method
def process_gcc_credits(
    df: pd.DataFrame,
    *,
    raw_projects: pd.DataFrame,
    download_type: str,
    registry_name: str = 'global-carbon-council',
    prefix: str = 'GCC',
    arb: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Process GCC credits data by setting vintage year, transaction types, dates, registry, and validating the schema.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with raw GCC credits data.
    raw_projects : pd.DataFrame
        DataFrame containing project names for mapping.
    download_type : str
        Type of download ('issuances' or 'retirements').
    registry_name : str, optional
        Name of the registry for setting and mapping columns (default is 'global-carbon-council').
    prefix : str, optional
        Prefix for modifying project IDs (default is 'GCC').
    arb : pd.DataFrame | None, optional
        Additional DataFrame for data merging (default is None).

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with GCC credits data.
    """

    df = df.copy()
    data = df.set_gcc_vintage_year()
    if download_type == 'issuances':
        data = data.set_gcc_issuance_transaction_type_and_date()
    elif download_type == 'retirements':
        data = data.set_gcc_retirement_transaction_type_and_date()

    data = data.set_registry(registry_name=registry_name).convert_to_datetime(
        columns=['transaction_date']
    )

    results = raw_projects.add_gcc_project_name()
    projects_dict_list = results[['project_submission_number', 'name']].to_dict(orient='records')
    result_dict = {d['name']: d['project_submission_number'] for d in projects_dict_list}

    data['project_id'] = data['project_name'].map(result_dict)

    column_mapping = load_column_mapping(
        registry_name=registry_name, download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )

    columns = {v: k for k, v in column_mapping.items()}

    data = (
        data.rename(columns=columns)
        .modify_gcc_project_id(prefix=prefix)
        .convert_to_datetime(columns=['transaction_date'])
        .validate(schema=credit_without_id_schema)
    )

    if arb is not None and not arb.empty:
        data = data.merge_with_arb(arb=arb)
    return data


@pf.register_dataframe_method
def add_gcc_project_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract project names from 'project_url' column in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing GCC project data.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'name' column containing extracted project names.
    """

    name_pattern = r'>(.*)<'
    df['name'] = df['project_url'].str.extract(name_pattern)
    return df


@pf.register_dataframe_method
def add_gcc_project_url(df: pd.DataFrame) -> pd.DataFrame:
    """
    Construct project URLs for GCC projects based on internal project IDs extracted from 'project_url' column.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing GCC project data.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'project_url' column containing constructed URLs for each project.
    """

    internal_id_pattern = r'\/(\d+)<*'
    base_proj_url = 'https://projects.globalcarboncouncil.com/project/'
    df['project_url'] = base_proj_url + df['project_url'].str.extract(internal_id_pattern)
    return df


@pf.register_dataframe_method
def process_gcc_projects(
    df: pd.DataFrame,
    *,
    credits: pd.DataFrame,
    registry_name='global-carbon-council',
    prefix: str = 'GCC',
) -> pd.DataFrame:
    """
    Process GCC projects data, including renaming, adding, and validating columns, harmonizing statuses, and merging with credits data.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with raw GCC projects data.
    credits : pd.DataFrame
        DataFrame containing credits data for merging.
    registry_name : str, optional
        Name of the registry for specific processing steps (default is 'global-carbon-council').
    prefix : str, optional
        Prefix for modifying project IDs (default is 'GCC').

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with harmonized and validated GCC projects data.
    """

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
        .add_gcc_project_name()
        .add_gcc_project_url()
        .modify_gcc_project_id(prefix=prefix)
        .harmonize_country_names()
        .harmonize_status_codes()
        .map_protocol(inverted_protocol_mapping=inverted_protocol_mapping)
        .add_category(protocol_mapping=protocol_mapping)
        .add_is_compliance_flag()
        .add_retired_and_issued_totals(credits=credits)
        .add_first_issuance_and_retirement_dates(credits=credits)
        .add_missing_columns(schema=project_schema)
        .convert_to_datetime(columns=['listed_at'])
        .validate(schema=project_schema)
    )
    return data
