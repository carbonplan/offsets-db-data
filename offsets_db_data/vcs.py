import re

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
def set_vcs_transaction_type(df: pd.DataFrame, *, download_type: str) -> pd.DataFrame:
    """
    Set the transaction type from the Verra report the rows come from.

    Verra publishes issuances, retirements and cancellations as separate reports, so every row of
    one report has the same transaction type.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with the rows of one Verra report.
    download_type : str
        The report: 'issuances', 'retirements' or 'cancellations'.

    Returns
    -------
    pd.DataFrame
        DataFrame with a 'transaction_type' column.
    """

    transaction_types = {
        'issuances': 'issuance',
        'retirements': 'retirement',
        'cancellations': 'cancellation',
    }
    df['transaction_type'] = transaction_types[download_type]
    return df


@pf.register_dataframe_method
def set_vcs_vintage_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Keep the last year of each Verra vintage.

    Verra reports a vintage as one year ('2015') or as a range of years ('2010 - 2011').
    OffsetsDB stores one year per credit, the last year of the range.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with a 'vintage' column.

    Returns
    -------
    pd.DataFrame
        DataFrame with 'vintage' as a year. A missing vintage stays missing.
    """

    last_year = df['vintage'].astype(str).str.findall(r'\d{4}').str[-1]
    df['vintage'] = pd.to_numeric(last_year).astype('Int64')
    return df


@pf.register_dataframe_method
def process_vcs_credits(
    df: pd.DataFrame,
    *,
    download_type: str,
    registry_name: str = 'verra',
    harmonize_beneficiary_info: bool = False,
) -> pd.DataFrame:
    """
    Process one report of Verra credits data: issuances, retirements or cancellations.

    Renames the mapped columns, sets the registry, transaction type, project ID and vintage year,
    keeps only the date of each transaction, sums issuances of the same project and vintage on the
    same day, and validates the result.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with one raw Verra report.
    download_type : str
        The report: 'issuances', 'retirements' or 'cancellations'.
    registry_name : str, optional
        Name of the registry (default is 'verra').
    harmonize_beneficiary_info : bool, optional
        Whether to harmonize retirement beneficiaries with OpenRefine (default is False).

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with Verra credits data.
    """

    column_mapping = load_column_mapping(
        registry_name=registry_name, download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )
    columns = {raw: column for column, raw in column_mapping.items() if raw}

    # Select the mapped columns before renaming, so no raw column can share a name with a new one
    data = (
        df[list(columns)]
        .rename(columns=columns)
        .set_registry(registry_name=registry_name)
        .set_vcs_transaction_type(download_type=download_type)
        .add_vcs_project_id()
        .set_vcs_vintage_year()
        .convert_to_datetime(columns=['transaction_date'], format='ISO8601')
    )

    if download_type == 'issuances':
        data = data.aggregate_issuance_transactions()

    data = data.add_missing_columns(schema=credit_without_id_schema).validate(
        schema=credit_without_id_schema
    )

    if harmonize_beneficiary_info:
        data = data.pipe(
            harmonize_beneficiary_data, registry_name=registry_name, download_type=download_type
        )

    return (
        data.add_missing_columns(schema=credit_without_id_schema)
        .convert_to_datetime(columns=['transaction_date'], format='%Y-%m-%d')
        .validate(schema=credit_without_id_schema)
    )


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
            'category': 'ghg-management',
            'project_type': 'mine methane capture',
            'project_type_source': 'carbonplan',
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
            'category': 'forest',
            'project_type': 'improved forest management',
            'project_type_source': 'carbonplan',
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
    """
    Create a URL for each project based on its Verra project ID.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with Verra project data.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'project_url' column, containing the generated URLs for each project.
    """

    df['project_url'] = (
        'https://registry.verra.org/app/projectDetail/VCS/' + df['project_id'].str[3:]
    )
    return df


@pf.register_dataframe_method
def add_vcs_project_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a prefix 'VCS' to each project ID in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with Verra project data.

    Returns
    -------
    pd.DataFrame
        DataFrame with updated 'project_id' column, containing the prefixed project IDs.
    """

    df['project_id'] = df['project_id'].apply(lambda x: f'VCS{str(x)}')
    return df


# TODO(Grayson): fill in the placeholders and confirm the mapping
VERRA_STATE_CODES = {
    # old table
    'REGISTERED': 'registered',
    'REGISTRATION_REQUESTED': 'listed',
    'REGISTRATION_VERIFICATION_APPROVAL_REQ': 'listed',
    'CREDIT_PERIOD_RENEWAL_REQUESTED': 'registered',
    'CREDIT_PERIOD_RENEWAL_VERIFICATION_REQUESTED': 'registered',
    'CREDIT_PERIOD_RENEWAL_VERIFICATION_APP_REQUESTED': 'registered',
    'INACTIVE': 'completed',
    'REJECTED': 'completed',
    'WITHDRAWN': 'completed',
    'UNITS_TRANSFER_APPROVE_GHG_PROG': 'unknown',
    'UNDER_VAL_COMMENT_CLOSED': 'listed',
    'UNDER_VAL_COMMENT_PERIOD_ON': 'listed',
    'PIPELINE_LISTING_UNDER_DEVELOPMENT_APPROVED': 'listed',
    'PIPELINE_LISTING_UNDER_VALIDATION': 'listed',
    'REQUANTIFICATION_REQUESTED': 'registered',
    'LATE_TO_VERIFY': 'unknown',
    'VERIFICATION_APPROVAL_REQUESTED': 'unknown',
    'ON_HOLD': 'unknown',
    'PIPELINE_LISTING_UNDER_DEVELOPMENT': 'unknown',
    'REGISTRATION_REQ_DENIED': 'unknown',
    'VERIFICATION_APPROVAL_REQUEST_DENIED': 'unknown',
    'REGISTRATION_VERIFICATION_REQ_DENIED': 'unknown',
}


@pf.register_dataframe_method
def harmonize_vcs_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map Verra state codes to OffsetsDB project statuses with ``VERRA_STATE_CODES``.

    A state code missing from ``VERRA_STATE_CODES`` becomes 'unknown' and is printed, so it can be
    added.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with Verra state codes in a 'status' column.

    Returns
    -------
    pd.DataFrame
        DataFrame with OffsetsDB statuses in the 'status' column.
    """

    if unmapped := set(df['status'].dropna()) - set(VERRA_STATE_CODES):
        print(f'Verra state codes with no status, set to unknown: {sorted(unmapped)}')
    df['status'] = df['status'].map(VERRA_STATE_CODES).fillna('unknown')
    return df


@pf.register_dataframe_method
def clean_vcs_proponents(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove Verra's '(PP)' role from proponent names, and join several proponents with '; '.

    Verra lists proponents as 'A (PP),B (PP)'. Names are split only after '(PP)', because names
    like 'Blue Carbon Projects, LLC' have commas of their own.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with a 'proponent' column.

    Returns
    -------
    pd.DataFrame
        DataFrame with cleaned 'proponent' values.
    """

    df['proponent'] = (
        df['proponent']
        .str.replace(r'\s*\(PP\)\s*,\s*', '; ', regex=True)
        .str.replace(r'\s*\(PP\)\s*$', '', regex=True)
        .str.strip()
    )
    return df


def _split_vcs_methodologies(value) -> list[str]:
    """'VM0050 (Version 1), VMR0006 (Version 1.1)' -> ['VM0050', 'VMR0006']"""
    if pd.isna(value):
        return []
    without_versions = re.sub(r'\s*\(Version[^)]*\)', '', str(value))
    return [code for code in re.split(r'\s*,\s*', without_versions.strip()) if code]


def _find_vcs_methodology(code: str, inverted_protocol_mapping: dict) -> list[str] | None:
    # The known strings have 'AMS-III.AU' where Verra writes 'AMS-III.AU.', and the other way round
    for candidate in (code, code.rstrip('.'), code + '.'):
        if inverted_protocol_mapping.get(candidate, ['unknown']) != ['unknown']:
            return inverted_protocol_mapping[candidate]
    return None


# TODO(Grayson): Verra lists some afforestation methodologies without their 'AR-' prefix, such as
# 'ACM0003' for 'AR-ACM0003' ('ACM0003' is fuel switching in cement), so these projects get the
# wrong protocol and category. Projects with land use as their only scope, November 2026:
#   'ACM0003': VCS1332, VCS1361, VCS1930, VCS3318, VCS3896, VCS3976, VCS4632, VCS4801, VCS4935
#   'AM0014' (perhaps 'AR-AM0014', mangrove planting): VCS2395
# Decide how to remap them.
#
# TODO(Grayson): these codes have no protocol in all-protocol-mapping.json, so they end up in
# 'protocol_unassigned'. Add a protocol, with a category, for each. Projects per code, November
# 2026: VMR0017 (29), VMR0016 (10), AMS-III.AK. (3), VMR0009 (3), AMS-II.S. (2), CAR - USLP (2),
# VMR0012 (2), VMR0015 (2), and 1 each for ACM0004, ACM0021, AM0047, AM0057, AM0086, AR-AM0010,
# VM0024, VM0035, VM0040, VMD0049, VMD0051, VMD0054, VMD0055, VMR0004, VT0008.
# Also give a category to 'am0123' (none, 5 projects) and 'vm0033' ('unknown', 62 projects).
# 'Methodology Under Development' (27 projects) is not a code and should stay unassigned.
@pf.register_dataframe_method
def map_vcs_protocol(df: pd.DataFrame, *, inverted_protocol_mapping: dict) -> pd.DataFrame:
    """
    Map each methodology code of a Verra project to OffsetsDB protocols.

    Verra lists several methodologies as one string, such as
    'VM0042 (Version 2.0), VMR0017 (Version 1.0)'. Each code is matched on its own against the
    known strings, so known strings need one entry per code, not one per combination.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with the raw methodologies in an 'original_protocol' column.
    inverted_protocol_mapping : dict
        Known protocol strings, from ``load_inverted_protocol_mapping``.

    Returns
    -------
    pd.DataFrame
        DataFrame with 'protocol', the matched protocols, and 'protocol_unassigned', the codes
        that matched none. Each is None when empty.
    """

    protocols, unassigned = [], []
    for value in df['original_protocol']:
        matched, unmatched = [], []
        for code in _split_vcs_methodologies(value):
            if found := _find_vcs_methodology(code, inverted_protocol_mapping):
                matched += [protocol for protocol in found if protocol not in matched]
            else:
                unmatched.append(code)
        protocols.append(matched or None)
        unassigned.append(unmatched or None)
    df['protocol'] = protocols
    df['protocol_unassigned'] = unassigned
    return df


@pf.register_dataframe_method
def process_vcs_projects(
    df: pd.DataFrame,
    *,
    credits: pd.DataFrame,
    registry_name: str = 'verra',
) -> pd.DataFrame:
    """
    Process Verra projects data, including renaming, adding, and validating columns, and merging with credits data.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with raw projects data.
    credits : pd.DataFrame
        DataFrame containing credits data for merging.
    registry_name : str, optional
        Name of the registry (default is 'verra').

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with harmonized and validated Verra projects data.
    """

    df = df.copy()
    credits = credits.copy()
    registry_project_column_mapping = load_registry_project_column_mapping(
        registry_name=registry_name, file_path=PROJECT_SCHEMA_UPATH
    )
    inverted_column_mapping = {value: key for key, value in registry_project_column_mapping.items()}
    type_category_mapping = load_type_category_mapping()
    inverted_protocol_mapping = load_inverted_protocol_mapping()
    protocol_mapping = load_protocol_mapping()

    # Select the mapped columns before renaming: the raw report has its own 'status' column,
    # which would clash with 'stateCode' renamed to 'status'
    data = (
        df[list(inverted_column_mapping)]
        .rename(columns=inverted_column_mapping)
        .set_registry(registry_name=registry_name)
        .add_vcs_project_id()
        .add_vcs_project_url()
        .clean_vcs_proponents()
        .harmonize_country_names()
        .harmonize_vcs_status()
        .map_vcs_protocol(inverted_protocol_mapping=inverted_protocol_mapping)
        .infer_project_type()
        .override_project_types(
            override_data_path=BERKELEY_PROJECT_TYPE_UPATH, source_str='berkeley'
        )
        .add_category(
            type_category_mapping=type_category_mapping,
            protocol_mapping=protocol_mapping,
        )  # category derived from protocol; project_type is independent
        .add_is_compliance_flag()
        .add_vcs_compliance_projects()
        .map_project_type_to_display_name(type_category_mapping=type_category_mapping)
        .add_retired_and_issued_totals(credits=credits)
        .add_first_issuance_and_retirement_dates(credits=credits)
        .add_missing_columns(schema=project_schema)
        .convert_to_datetime(columns=['listed_at'], dayfirst=True)
        .validate(schema=project_schema)
    )

    return data
