import contextlib
import json
import re

import country_converter as coco
import janitor  # noqa: F401
import numpy as np
import pandas as pd
import pandas_flavor as pf


def extract_protocol_version_pairs(protocol_string: str) -> list[tuple[str, str | None]]:
    """
    Extract protocol name and version pairs from a raw protocol string.

    Handles multi-protocol strings separated by: ; & , and

    Parameters
    ----------
    protocol_string : str
        Raw protocol string like "ACM0001 v19.0; ACM0022 v3.0"

    Returns
    -------
    list[tuple[str, str | None]]
        List of (protocol_name, version) tuples
        Example: [("ACM0001", "19.0"), ("ACM0022", "3.0")]

    Examples
    --------
    >>> extract_protocol_version_pairs("ACM0001 v19.0")
    [('ACM0001', '19.0')]

    >>> extract_protocol_version_pairs("ACM0001: Version 19.0; ACM0022: Version 3.0")
    [('ACM0001', '19.0'), ('ACM0022', '3.0')]

    >>> extract_protocol_version_pairs("VM0007 REDD+ Framework")
    [('VM0007', None)]
    """
    if pd.isna(protocol_string) or not str(protocol_string).strip():
        return []

    # Regex patterns for version and protocol names
    version_pattern = r'(?:[vV](?:ersion|er)?\.?\s*|&\s*)(\d+(?:\.\d+)?)'
    protocol_name_pattern = r'\b([A-Z]+[A-Z0-9\-\.]*[A-Z0-9]+)\b'

    # Normalize version format (remove prefixes, ensure decimal)
    def normalize_version(version: str) -> str:
        version = re.sub(r'^[vV](?:ersion|er)?\.?\s*', '', version)
        if '.' not in version:
            version = f'{version}.0'
        return version

    # Split by common separators
    segments = re.split(r'[;&]|\s+and\s+', protocol_string, flags=re.IGNORECASE)

    pairs = []
    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue

        # Extract protocol name
        protocol_match = re.search(protocol_name_pattern, segment, re.IGNORECASE)
        if not protocol_match:
            continue

        protocol_name = protocol_match.group(1).upper()

        # Extract version from this segment
        version_match = re.search(version_pattern, segment, re.IGNORECASE)
        version = normalize_version(version_match.group(1)) if version_match else None

        pairs.append((protocol_name, version))

    return pairs


@pf.register_dataframe_method
def extract_protocol_versions(
    df: pd.DataFrame, *, original_protocol_column: str = 'original_protocol'
) -> pd.DataFrame:
    """
    Extract protocol version information from raw protocol strings BEFORE harmonization.

    Creates a 'protocol_version_raw' column with protocol-version mappings that will be
    aligned to normalized protocol names after map_protocol() runs.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with raw protocol data
    original_protocol_column : str, optional
        Column containing raw protocol strings from registry (default is 'original_protocol')

    Returns
    -------
    pd.DataFrame
        DataFrame with new 'protocol_version_raw' column containing dict mapping
        protocol names to versions

    Examples
    --------
    Input: "ACM0001: Version 19.0; ACM0022: Version 3.0"
    Output: protocol_version_raw = {"ACM0001": "19.0", "ACM0022": "3.0"}

    Input: "VM0007 REDD+ Framework"
    Output: protocol_version_raw = {"VM0007": None}
    """
    print('Extracting protocol versions from raw strings...')

    def extract_to_dict(protocol_string):
        """Convert protocol string to dict mapping name to version"""
        if pd.isna(protocol_string):
            return {}

        pairs = extract_protocol_version_pairs(protocol_string)
        return {name: version for name, version in pairs}

    try:
        df['protocol_version_raw'] = df[original_protocol_column].apply(extract_to_dict)
    except KeyError:
        # Some registries (like art-trees) don't have protocol column
        df['protocol_version_raw'] = [{}] * len(df)

    projects_with_versions = sum(df['protocol_version_raw'].apply(bool))
    print(f'Extracted versions for {projects_with_versions} of {len(df)} projects')

    return df


@pf.register_dataframe_method
def align_protocol_versions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Align extracted protocol versions with harmonized protocol names.

    This runs AFTER map_protocol() to create a parallel array where
    protocol[i] corresponds to protocol_version[i].

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with both 'protocol' (normalized) and 'protocol_version_raw' (raw mapping)

    Returns
    -------
    pd.DataFrame
        DataFrame with 'protocol_version' array aligned to 'protocol' array

    Examples
    --------
    protocol = ['acm0001', 'acm0022']
    protocol_version_raw = {'ACM0001': '19.0', 'ACM0022': '3.0'}
    → protocol_version = ['19.0', '3.0']

    protocol = ['vm0007']
    protocol_version_raw = {}
    → protocol_version = [None]
    """
    print('Aligning protocol versions with normalized protocol names...')

    def align_versions(row):
        """Align versions to normalized protocol array"""
        protocols = row.get('protocol', [])
        if not isinstance(protocols, list) or not protocols:
            return [None]

        version_map = row.get('protocol_version_raw', {})
        if not version_map:
            return [None] * len(protocols)

        # Match normalized protocol names to raw protocol names
        aligned_versions = []
        for normalized_proto in protocols:
            # Try to find matching raw protocol name (case-insensitive, ignore punctuation)
            version = None
            normalized_clean = normalized_proto.lower().replace('-', '').replace('.', '')

            for raw_proto, raw_version in version_map.items():
                raw_clean = raw_proto.lower().replace('-', '').replace('.', '')
                if raw_clean == normalized_clean:
                    version = raw_version
                    break

            aligned_versions.append(version)

        return aligned_versions

    df['protocol_version'] = df.apply(align_versions, axis=1)

    # Clean up temporary column
    df = df.drop(columns=['protocol_version_raw'], errors='ignore')

    # Stats
    versions_found = sum(
        any(v is not None for v in versions)
        for versions in df['protocol_version']
        if isinstance(versions, list)
    )
    print(f'Aligned versions for {versions_found} of {len(df)} projects')

    return df


@pf.register_dataframe_method
def harmonize_country_names(df: pd.DataFrame, *, country_column: str = 'country') -> pd.DataFrame:
    """
    Harmonize country names in the DataFrame to standardized country names.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with country data.
    country_column : str, optional
        The name of the column containing country names to be harmonized (default is 'country').

    Returns
    -------
    pd.DataFrame
        DataFrame with harmonized country names in the specified column.
    """

    print('Harmonizing country names...')
    cc = coco.CountryConverter()
    df[country_column] = cc.pandas_convert(df[country_column], to='name')
    print('Done converting country names...')
    return df


@pf.register_dataframe_method
def add_category(df: pd.DataFrame, *, type_category_mapping: dict) -> pd.DataFrame:
    """
    Add a category to each record in the DataFrame based on its protocol.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing protocol data.
    type_category_mapping : dict
        Dictionary mapping types to categories.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'category' column, derived from the protocol information.
    """

    print('Adding category based on protocol...')
    df['category'] = (
        df['project_type']
        .str.lower()
        .map({key.lower(): value['category'] for key, value in type_category_mapping.items()})
        .fillna('unknown')
    )
    return df


@pf.register_dataframe_method
def override_project_types(df: pd.DataFrame, *, override_data_path: str, source_str: str):
    """
    Override project types to the DataFrame based on project characteristics
    We treat Berkeley data as source of truth for most project types

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing project data.
    override_data_path: str
        Path to where json of override data lives
    source: str
        Value to write to `type_source` when applying override values

    Returns
    -------
    pd.DataFrame
        DataFrame with a 'project_type' column overridden by all values in override_data.
    """

    override_d = json.load(open(override_data_path))
    df['project_type'] = df['project_id'].map(override_d).fillna(df['project_type'])
    df.loc[df['project_id'].isin(list(override_d.keys())), 'project_type_source'] = source_str

    return df


@pf.register_dataframe_method
def infer_project_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add project types to the DataFrame based on project characteristics

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing project data.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'project_type' column, indicating the project's type. Defaults to None
    """
    df.loc[:, 'project_type'] = 'unknown'
    df.loc[:, 'project_type_source'] = 'carbonplan'
    df.loc[df.apply(lambda x: 'art-trees' in x['protocol'], axis=1), 'project_type'] = 'redd+'

    df.loc[df.apply(lambda x: 'acr-ifm-nonfed' in x['protocol'], axis=1), 'project_type'] = (
        'improved forest management'
    )
    df.loc[df.apply(lambda x: 'acr-abandoned-wells' in x['protocol'], axis=1), 'project_type'] = (
        'plugging oil & gas wells'
    )

    df.loc[df.apply(lambda x: 'arb-mine-methane' in x['protocol'], axis=1), 'project_type'] = (
        'mine methane capture'
    )

    df.loc[df.apply(lambda x: 'vm0048' in x['protocol'], axis=1), 'project_type'] = 'redd+'
    df.loc[df.apply(lambda x: 'vm0047' in x['protocol'], axis=1), 'project_type'] = (
        'afforestation/reforestation'
    )
    df.loc[df.apply(lambda x: 'vm0045' in x['protocol'], axis=1), 'project_type'] = (
        'improved forest management'
    )
    df.loc[df.apply(lambda x: 'vm0042' in x['protocol'], axis=1), 'project_type'] = 'agriculture'
    df.loc[df.apply(lambda x: 'vm0007' in x['protocol'], axis=1), 'project_type'] = 'redd+'

    return df


@pf.register_dataframe_method
def map_project_type_to_display_name(
    df: pd.DataFrame, *, type_category_mapping: dict
) -> pd.DataFrame:
    """
    Map project types in the DataFrame to display names based on a mapping dictionary.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing project data.
    type_category_mapping : dict
        Dictionary mapping project type strings to display names.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'project_type' column, containing mapped display names.
    """

    print('Mapping project types to display names...')
    df['project_type'] = (
        df['project_type']
        .map(
            {
                key.lower(): value['project-type-display-name']
                for key, value in type_category_mapping.items()
            }
        )
        .fillna('Unknown')
    )
    return df


@pf.register_dataframe_method
def add_is_compliance_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a compliance flag to the DataFrame based on the protocol.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing protocol data.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'is_compliance' column, indicating if the protocol starts with 'arb-'.
    """

    print('Adding is_compliance flag...')
    df['is_compliance'] = df.apply(
        lambda row: np.any([protocol_str.startswith('arb-') for protocol_str in row['protocol']]),
        axis=1,
    )
    return df


@pf.register_dataframe_method
def map_protocol(
    df: pd.DataFrame,
    *,
    inverted_protocol_mapping: dict,
    original_protocol_column: str = 'original_protocol',
) -> pd.DataFrame:
    """
    Map protocols in the DataFrame to standardized names based on an inverted protocol mapping.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing protocol data.
    inverted_protocol_mapping : dict
        Dictionary mapping protocol strings to standardized protocol names.
    original_protocol_column : str, optional
        Name of the column containing original protocol information (default is 'original_protocol').

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'protocol' column, containing mapped protocol names.
    """

    print('Mapping protocol based on known string...')
    try:
        df['protocol'] = df[original_protocol_column].apply(
            lambda item: find_protocol(
                search_string=item, inverted_protocol_mapping=inverted_protocol_mapping
            )
        )
    except KeyError:
        # art-trees doesnt have protocol column
        df['protocol'] = [['unknown']] * len(df)  # protocol column is nested list

    return df


@pf.register_dataframe_method
def harmonize_status_codes(df: pd.DataFrame, *, status_column: str = 'status') -> pd.DataFrame:
    """Harmonize project status codes across registries

    Excludes ACR, as it requires special treatment across two columns

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with project status data.
    status_column : str, optional
        Name of the column containing status codes to harmonize (default is 'status').

    Returns
    -------
    pd.DataFrame
        DataFrame with harmonized project status codes.
    """
    print('Harmonizing status codes')
    with contextlib.suppress(KeyError):
        CAR_STATES = {
            'Registered': 'registered',
            'Completed': 'completed',
            'Listed': 'listed',
            'Transitioned': 'unknown',
        }

        VERRA_STATES = {
            'Under validation': 'listed',
            'Under development': 'listed',
            'Registration requested': 'listed',
            'Registration and verification approval requested': 'listed',
            'Withdrawn': 'completed',
            'On Hold': 'registered',
            'Units Transferred from Approved GHG Program': 'unknown',
            'Rejected by Administrator': 'completed',
            'Crediting Period Renewal Requested': 'registered',
            'Inactive': 'completed',
            'Crediting Period Renewal and Verification Approval Requested': 'registered',
        }

        GS_STATES = {
            'GOLD_STANDARD_CERTIFIED_PROJECT': 'registered',
            'LISTED': 'listed',
            'GOLD_STANDARD_CERTIFIED_DESIGN': 'registered',
        }

        state_dict = CAR_STATES | VERRA_STATES | GS_STATES
        df[status_column] = df[status_column].apply(lambda x: state_dict.get(x, 'unknown'))
    return df


def find_protocol(
    *, search_string: str, inverted_protocol_mapping: dict[str, list[str]]
) -> list[str]:
    """Match known strings of project methodologies to internal topology

    Unmatched strings are passed through to the database, until such time that we update mapping data.
    """
    if pd.isna(search_string):  # handle nan case, which crops up in verra data right now
        return ['unknown']
    if known_match := inverted_protocol_mapping.get(search_string.strip()):
        return known_match  # inverted_mapping returns lst
    print(f"'{search_string}' is unmapped in full protocol mapping")
    return [search_string]


def get_protocol_category(*, protocol_strs: list[str] | str, protocol_mapping: dict) -> list[str]:
    """
    Get category based on protocol string

    Parameters
    ----------
    protocol_strs : str or list
        single protocol string or list of protocol strings

    protocol_mapping: dict
        metadata about normalized protocol strings

    Returns
    -------
    categories : list[str]
        list of category strings
    """

    def _get_category(protocol_str, protocol_mapping):
        try:
            return protocol_mapping.get(protocol_str).get('category', 'unknown')
        except AttributeError:
            return 'unknown'

    if isinstance(protocol_strs, str):
        protocol_strs = [protocol_strs]
    categories = [_get_category(protocol_str, protocol_mapping) for protocol_str in protocol_strs]
    return list(
        set(categories)
    )  # if multiple protocols have same category, just return category once


@pf.register_dataframe_method
def add_first_issuance_and_retirement_dates(
    projects: pd.DataFrame, *, credits: pd.DataFrame
) -> pd.DataFrame:
    """
    Add the first issuance date of carbon credits to each project in the projects DataFrame.

    Parameters
    ----------
    credits : pd.DataFrame
        A pandas DataFrame containing credit issuance data with columns 'project_id', 'transaction_date', and 'transaction_type'.
    projects : pd.DataFrame
        A pandas DataFrame containing project data with a 'project_id' column.

    Returns
    -------
    projects : pd.DataFrame
        A pandas DataFrame which is the original projects DataFrame with two additional columns 'first_issuance_at' representing
        the first issuance date of each project and 'first_retirement_at' representing the first retirement date of each project.
    """

    first_issuance = (
        credits[credits['transaction_type'] == 'issuance']
        .groupby('project_id')['transaction_date']
        .min()
        .reset_index()
    )
    first_retirement = (
        credits[credits['transaction_type'].str.contains('retirement')]
        .groupby('project_id')['transaction_date']
        .min()
        .reset_index()
    )

    # Merge the projects DataFrame with the first issuance and retirement dates
    projects_with_dates = pd.merge(projects, first_issuance, on='project_id', how='left')
    projects_with_dates = pd.merge(
        projects_with_dates, first_retirement, on='project_id', how='left'
    )

    # Rename the merged columns for clarity
    projects_with_dates = projects_with_dates.rename(
        columns={
            'transaction_date_x': 'first_issuance_at',
            'transaction_date_y': 'first_retirement_at',
        }
    )

    return projects_with_dates


@pf.register_dataframe_method
def add_retired_and_issued_totals(projects: pd.DataFrame, *, credits: pd.DataFrame) -> pd.DataFrame:
    """
    Add total quantities of issued and retired credits to each project.

    Parameters
    ----------
    projects : pd.DataFrame
        DataFrame containing project data.
    credits : pd.DataFrame
        DataFrame containing credit transaction data.

    Returns
    -------
    pd.DataFrame
        DataFrame with two new columns: 'issued' and 'retired', representing the total quantities of issued and retired credits.
    """

    # Drop conflicting columns if they exist
    projects = projects.drop(columns=['issued', 'retired'], errors='ignore')

    # # filter out the projects that are not in the credits data
    # credits = credits[credits['project_id'].isin(projects['project_id'].unique())]
    # groupd and sum
    credit_totals = (
        credits.groupby(['project_id', 'transaction_type'])['quantity'].sum().reset_index()
    )
    # pivot the table
    credit_totals_pivot = credit_totals.pivot(
        index='project_id', columns='transaction_type', values='quantity'
    ).reset_index()

    # merge with projects
    projects_combined = pd.merge(
        projects,
        credit_totals_pivot[['project_id', 'issuance', 'retirement']],
        left_on='project_id',
        right_on='project_id',
        how='left',
    )

    # rename columns for clarity
    projects_combined = projects_combined.rename(
        columns={'issuance': 'issued', 'retirement': 'retired'}
    )

    # replace Nans with 0 if any
    projects_combined[['issued', 'retired']] = projects_combined[['issued', 'retired']].fillna(0)

    return projects_combined
