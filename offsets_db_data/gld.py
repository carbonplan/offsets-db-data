import numpy as np  # noqa: F401, I001
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
from offsets_db_data.credits import (
    aggregate_issuance_transactions,  # noqa: F401
    filter_and_merge_transactions,  # noqa: F401
    merge_with_arb,  # noqa: F401
)
from offsets_db_data.models import credit_without_id_schema, project_schema
from offsets_db_data.projects import (
    harmonize_country_names,  # noqa: F401
    add_category,  # noqa: F401
    add_is_compliance_flag,  # noqa: F401
    map_protocol,  # noqa: F401
    harmonize_status_codes,  # noqa: F401
    add_first_issuance_and_retirement_dates,  # noqa: F401
    add_retired_and_issued_totals,  # noqa: F401
)


@pf.register_dataframe_method
def determine_gld_transaction_type(df: pd.DataFrame, *, download_type: str) -> pd.DataFrame:
    """
    Assign a transaction type to each record in the DataFrame based on the download type for Gold Standard transactions.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing transaction data.
    download_type : str
        Type of transaction ('issuances', 'retirements') to determine the transaction type.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'transaction_type' column, containing assigned transaction types based on download_type.
    """

    transaction_type_mapping = {'issuances': 'issuance', 'retirements': 'retirement'}
    df['transaction_type'] = transaction_type_mapping[download_type]
    return df


@pf.register_dataframe_method
def add_gld_project_id(df: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    """
    Add Gold Standard project IDs to the DataFrame

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing credits data.
    prefix : str
        Prefix string to prepend to each project ID.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'project_id' column, containing the generated project IDs.
    """

    df['project_id'] = prefix + df['project_id'].astype(str)
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
    """
    Process Gold Standard credits data by renaming columns, setting registry, determining transaction types,
    adding project IDs, converting date columns, aggregating issuances (if applicable), and validating the schema.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with raw Gold Standard credits data.
    download_type : str
        Type of download ('issuances' or 'retirements').
    registry_name : str, optional
        Name of the registry for setting and mapping columns (default is 'gold-standard').
    prefix : str, optional
        Prefix for generating project IDs (default is 'GLD').
    arb : pd.DataFrame | None, optional
        Additional DataFrame for data merging (default is None).

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with Gold Standard credits data.
    """

    column_mapping = load_column_mapping(
        registry_name=registry_name, download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )

    columns = {v: k for k, v in column_mapping.items()}

    df = df.copy()

    if not df.empty:
        data = (
            df.rename(columns=columns)
            .set_registry(registry_name=registry_name)
            .determine_gld_transaction_type(download_type=download_type)
            .add_gld_project_id(prefix=prefix)
        )
        # split on T and discard the microseconds for consistency
        data['transaction_date'] = data['transaction_date'].str.split('T').str[0]
        data = data.convert_to_datetime(columns=['transaction_date'], format='%Y-%m-%d')

        if download_type == 'issuances':
            data = data.aggregate_issuance_transactions()

        data = data.add_missing_columns(schema=credit_without_id_schema).validate(
            schema=credit_without_id_schema
        )

        if arb is not None and not arb.empty:
            data = data.merge_with_arb(arb=arb)

    else:
        data = (
            pd.DataFrame(columns=credit_without_id_schema.columns.keys())
            .add_missing_columns(schema=credit_without_id_schema)
            .convert_to_datetime(columns=['transaction_date'], format='%Y-%m-%d')
            .add_missing_columns(schema=credit_without_id_schema)
            .validate(schema=credit_without_id_schema)
        )

    return data


@pf.register_dataframe_method
def add_gld_project_url(df: pd.DataFrame) -> pd.DataFrame:
    """Add url for gold standard projects

    gs project ids are different from the id used in gold standard urls.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing Gold Standard project data.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'project_url' column, containing URLs for each project.
    """
    df['project_url'] = 'https://registry.goldstandard.org/projects?q=gs' + df['project_id'].apply(
        str
    )
    return df


@pf.register_dataframe_method
def process_gld_projects(
    df: pd.DataFrame,
    *,
    credits: pd.DataFrame,
    registry_name: str = 'gold-standard',
    prefix: str = 'GLD',
) -> pd.DataFrame:
    """
    Process Gold Standard projects data, including renaming, adding, and validating columns, harmonizing statuses,
    and merging with credits data.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with raw Gold Standard projects data.
    credits : pd.DataFrame
        DataFrame containing credits data for merging.
    registry_name : str, optional
        Name of the registry for specific processing steps (default is 'gold-standard').
    prefix : str, optional
        Prefix for generating project IDs (default is 'GLD').

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with harmonized and validated Gold Standard projects data.
    """

    registry_project_column_mapping = load_registry_project_column_mapping(
        registry_name=registry_name, file_path=PROJECT_SCHEMA_UPATH
    )
    inverted_column_mapping = {value: key for key, value in registry_project_column_mapping.items()}
    protocol_mapping = load_protocol_mapping()
    inverted_protocol_mapping = load_inverted_protocol_mapping()

    df = df.copy()
    credits = credits.copy()

    if not df.empty and not credits.empty:
        data = (
            df.rename(columns=inverted_column_mapping)
            .set_registry(registry_name=registry_name)
            .add_gld_project_url()
            .add_gld_project_id(prefix=prefix)
            .harmonize_country_names()
            .harmonize_status_codes()
            .map_protocol(inverted_protocol_mapping=inverted_protocol_mapping)
            .add_category(protocol_mapping=protocol_mapping)
            .add_is_compliance_flag()
            .add_retired_and_issued_totals(credits=credits)
            .add_first_issuance_and_retirement_dates(credits=credits)
            .add_missing_columns(schema=project_schema)
            .convert_to_datetime(columns=['listed_at', 'first_issuance_at', 'first_retirement_at'])
            .validate(schema=project_schema)
        )
        return data

    elif not df.empty and credits.empty:
        data = (
            df.rename(columns=inverted_column_mapping)
            .set_registry(registry_name=registry_name)
            .add_gld_project_url()
            .add_gld_project_id(prefix=prefix)
            .harmonize_country_names()
            .harmonize_status_codes()
            .map_protocol(inverted_protocol_mapping=inverted_protocol_mapping)
            .add_category(protocol_mapping=protocol_mapping)
            .add_is_compliance_flag()
            .add_missing_columns(schema=project_schema)
            .convert_to_datetime(columns=['listed_at', 'first_issuance_at', 'first_retirement_at'])
            .validate(schema=project_schema)
        )
        return data
    elif df.empty:
        data = (
            pd.DataFrame(columns=project_schema.columns.keys())
            .add_missing_columns(schema=project_schema)
            .convert_to_datetime(columns=['listed_at', 'first_issuance_at', 'first_retirement_at'])
        )

        data['is_compliance'] = data['is_compliance'].astype(bool)
        data = data.validate(schema=project_schema)
        return data
