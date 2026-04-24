import contextlib
import json
from decimal import Decimal

import country_converter as coco
import janitor  # noqa: F401
import numpy as np
import pandas as pd
import pandas_flavor as pf

_REGISTRY_PROJECT_URLS = {
    'verra': 'https://registry.verra.org/app/projectDetail/VCS/',
    'gold-standard': 'https://registry.goldstandard.org/projects?q=gs',
    'american-carbon-registry': 'https://acr2.apx.com/mymodule/reg/prjView.asp?id1=',
    'climate-action-reserve': 'https://thereserve2.apx.com/mymodule/reg/prjView.asp?id1=',
    'art-trees': 'https://art.apx.com/mymodule/reg/prjView.asp?id1=',
    'cercarbono': 'https://www.ecoregistry.io/projects/CDC-',
}


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
def add_category(
    df: pd.DataFrame, *, type_category_mapping: dict, protocol_mapping: dict | None = None
) -> pd.DataFrame:
    """
    Add a category to each record in the DataFrame based on its protocol.

    Category is derived directly from the protocol via protocol_mapping when available,
    falling back to type_category_mapping via project_type. This keeps category and
    project_type independent of each other.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing protocol data.
    type_category_mapping : dict
        Dictionary mapping project_type strings to categories (fallback).
    protocol_mapping : dict, optional
        The full protocol mapping (from all-protocol-mapping.json). When provided,
        category is read directly from the matched protocol's 'category' field,
        which decouples it from project_type.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'category' column, derived from the protocol information.
    """

    print('Adding category based on protocol...')

    if protocol_mapping is not None:

        def _category_from_protocol(protocol_list: list | None) -> str:
            if not protocol_list:
                return 'unknown'
            for p in protocol_list:
                cat = protocol_mapping.get(p, {}).get('category')
                if cat and cat != 'unknown':
                    return cat
            return 'unknown'

        df['category'] = df['protocol'].apply(_category_from_protocol)
    else:
        # Legacy fallback: derive category from project_type via type_category_mapping
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

    def _has_protocol(pid: str):
        return lambda x: x['protocol'] is not None and pid in x['protocol']

    df.loc[:, 'project_type'] = 'unknown'
    df.loc[:, 'project_type_source'] = 'carbonplan'
    df.loc[df.apply(_has_protocol('art-trees'), axis=1), 'project_type'] = 'redd+'

    df.loc[df.apply(_has_protocol('acr-ifm-nonfed'), axis=1), 'project_type'] = (
        'improved forest management'
    )
    df.loc[df.apply(_has_protocol('acr-refridge'), axis=1), 'project_type'] = (
        'advanced refrigerants'
    )
    df.loc[df.apply(_has_protocol('acr-abandoned-wells'), axis=1), 'project_type'] = (
        'plugging oil & gas wells'
    )

    df.loc[df.apply(_has_protocol('arb-mine-methane'), axis=1), 'project_type'] = (
        'mine methane capture'
    )

    df.loc[df.apply(_has_protocol('vm0048'), axis=1), 'project_type'] = 'redd+'
    df.loc[df.apply(_has_protocol('vm0047'), axis=1), 'project_type'] = (
        'afforestation/reforestation'
    )
    df.loc[df.apply(_has_protocol('vm0045'), axis=1), 'project_type'] = 'improved forest management'
    df.loc[df.apply(_has_protocol('vm0042'), axis=1), 'project_type'] = 'sustainable agriculture'
    df.loc[df.apply(_has_protocol('vm0007'), axis=1), 'project_type'] = 'redd+'

    df.loc[df.apply(_has_protocol('acm0001'), axis=1), 'project_type'] = 'landfill methane'
    df.loc[df.apply(_has_protocol('acm0002'), axis=1), 'project_type'] = 're bundled'

    df.loc[df.apply(_has_protocol('iso-refor'), axis=1), 'project_type'] = (
        'afforestation/reforestation'
    )
    df.loc[df.apply(_has_protocol('iso-biochar'), axis=1), 'project_type'] = 'biochar'
    df.loc[df.apply(_has_protocol('iso-bio-burial'), axis=1), 'project_type'] = 'biomass burial'
    df.loc[df.apply(_has_protocol('iso-bio-geo'), axis=1), 'project_type'] = 'biomass injection'
    df.loc[df.apply(_has_protocol('iso-bio-oil'), axis=1), 'project_type'] = 'biomass injection'
    df.loc[df.apply(_has_protocol('iso-dac'), axis=1), 'project_type'] = 'direct air capture'
    df.loc[df.apply(_has_protocol('iso-erw'), axis=1), 'project_type'] = 'enhanced rock weathering'

    df.loc[df.apply(_has_protocol('ccb-refor'), axis=1), 'project_type'] = (
        'afforestation/reforestation'
    )
    df.loc[df.apply(_has_protocol('ccb-redd'), axis=1), 'project_type'] = 'redd+'

    df.loc[df.apply(_has_protocol('car-forest-mx'), axis=1), 'project_type'] = (
        'improved forest management'
    )
    df.loc[df.apply(_has_protocol('gs-reforest'), axis=1), 'project_type'] = (
        'afforestation/reforestation'
    )
    df.loc[df.apply(_has_protocol('gs-drinking-water'), axis=1), 'project_type'] = 'clean water'

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
        lambda row: (
            row['protocol'] is not None
            and np.any([protocol_str.startswith('arb-') for protocol_str in row['protocol']])
        ),
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
        results = df[original_protocol_column].apply(
            lambda item: find_protocol(
                search_string=item, inverted_protocol_mapping=inverted_protocol_mapping
            )
        )
        df['protocol'] = [r[0] for r in results]
        df['protocol_unassigned'] = [r[1] for r in results]
    except KeyError:
        df['protocol'] = [None] * len(df)
        df['protocol_unassigned'] = [None] * len(df)

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
) -> tuple[list[str] | None, list[str] | None]:
    """Match known strings of project methodologies to internal topology.

    Returns a ``(mapped, unmatched)`` tuple:

    * ``mapped`` — list of normalised protocol IDs when the string is recognised, else ``None``.
    * ``unmatched`` — list containing the raw string when it is present but unrecognised, else ``None``.

    NaN, empty, and whitespace-only strings yield ``(None, None)``.
    """
    if pd.isna(search_string) or not str(search_string).strip():
        return None, None
    stripped = search_string.strip()
    if known_match := inverted_protocol_mapping.get(stripped):
        return known_match, None
    print(f"'{search_string}' is unmapped in full protocol mapping")
    return None, [search_string]


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
    # infer source precision and round after sum to avoid float64 representation errors
    # (e.g. 153.89 + 235.53 = 389.41999... in binary float)
    source_precision = int(
        credits['quantity']
        .dropna()
        .apply(lambda x: max(0, -Decimal(repr(x)).as_tuple().exponent))
        .max()
    )
    credit_totals = (
        credits.groupby(['project_id', 'transaction_type'])['quantity']
        .sum()
        .round(source_precision)
        .reset_index()
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


def add_placeholder_projects(
    *,
    credits: pd.DataFrame,
    projects: pd.DataFrame,
) -> pd.DataFrame:
    """
    Append placeholder project rows for any project IDs found in credits but absent from projects.

    Computes issued/retired totals and first issuance/retirement dates from credit data so that
    placeholder rows arrive in the database with correct summary stats rather than zeroed-out
    defaults.  Call this on the combined (all-registry) DataFrames before writing to parquet.

    Parameters
    ----------
    credits : pd.DataFrame
        Combined credits DataFrame (all registries).
    projects : pd.DataFrame
        Combined projects DataFrame (all registries).

    Returns
    -------
    pd.DataFrame
        Projects DataFrame with placeholder rows appended for orphan project IDs.
    """
    from offsets_db_data.registry import get_registry_from_project_id

    orphan_ids = set(credits['project_id'].unique()) - set(projects['project_id'].unique())

    if not orphan_ids:
        return projects

    print(f'Found {len(orphan_ids)} project IDs in credits with no project record: {orphan_ids}')

    orphan_credits = credits[credits['project_id'].isin(orphan_ids)]

    issued = (
        orphan_credits[orphan_credits['transaction_type'] == 'issuance']
        .groupby('project_id')['quantity']
        .sum()
        .rename('issued')
    )
    retired = (
        orphan_credits[orphan_credits['transaction_type'].str.contains('retirement', na=False)]
        .groupby('project_id')['quantity']
        .sum()
        .rename('retired')
    )
    first_issuance_at = (
        orphan_credits[orphan_credits['transaction_type'] == 'issuance']
        .groupby('project_id')['transaction_date']
        .min()
        .rename('first_issuance_at')
        .dt.as_unit('ns')
    )
    first_retirement_at = (
        orphan_credits[orphan_credits['transaction_type'].str.contains('retirement', na=False)]
        .groupby('project_id')['transaction_date']
        .min()
        .rename('first_retirement_at')
        .dt.as_unit('ns')
    )

    stats = pd.concat(
        [issued, retired, first_issuance_at, first_retirement_at], axis=1
    ).reset_index()
    stats[['issued', 'retired']] = stats[['issued', 'retired']].fillna(0)

    rows = []
    for _, row in stats.iterrows():
        project_id = row['project_id']
        try:
            registry = get_registry_from_project_id(project_id)
        except KeyError:
            registry = 'unknown'
        base_url = _REGISTRY_PROJECT_URLS.get(registry)
        project_url = f'{base_url}{project_id[3:]}' if base_url else None
        rows.append(
            {
                'project_id': project_id,
                'registry': registry,
                'project_type': 'Unknown',
                'project_type_source': 'carbonplan',
                'category': 'unknown',
                'protocol': None,
                'protocol_unassigned': None,
                'issued': row.get('issued', 0.0),
                'retired': row.get('retired', 0.0),
                'first_issuance_at': row.get('first_issuance_at'),
                'first_retirement_at': row.get('first_retirement_at'),
                'is_compliance': False,
                'project_url': project_url,
                'name': None,
                'proponent': None,
                'status': None,
                'country': None,
                'listed_at': None,
            }
        )

    return pd.concat([projects, pd.DataFrame(rows)], ignore_index=True)
