import contextlib
import json
from collections import defaultdict

import country_converter as coco
import numpy as np
import pandas as pd
import pandas_flavor as pf
import upath

PROTOCOL_MAPPING_UPATH = upath.UPath(__file__).parents[0] / 'configs' / 'all-protocol-mapping.json'
PROJECT_SCHEMA_UPATH = (
    upath.UPath(__file__).parents[0] / 'configs' / 'projects-raw-columns-mapping.json'
)


def add_retired_and_issued_totals(
    *, credits_data: pd.DataFrame, projects_data: pd.DataFrame
) -> pd.DataFrame:
    credits_data = credits_data.copy()
    projects_data = projects_data.copy()

    # Drop conflicting columns if they exist
    projects_data = projects_data.drop(columns=['issued', 'retired'], errors='ignore')

    credits_data['transaction_type_mapped'] = credits_data['transaction_type'].apply(
        lambda x: 'retirement' if x == 'retirement/cancellation' else x
    )
    # # filter out the projects that are not in the credits data
    # credits_data = credits_data[credits_data['project_id'].isin(projects_data['project_id'].unique())]
    # groupd and sum
    credit_totals = (
        credits_data.groupby(['project_id', 'transaction_type_mapped'])['quantity']
        .sum()
        .reset_index()
    )
    # pivot the table
    credit_totals_pivot = credit_totals.pivot(
        index='project_id', columns='transaction_type_mapped', values='quantity'
    ).reset_index()

    # merge with projects
    projects_combined = pd.merge(
        projects_data,
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


def transform_raw_registry_data(raw_data: pd.DataFrame, registry_name: str) -> pd.DataFrame:
    """Transform raw downloaded data to conform to `projects` data model"""

    # load a bunch of static files that map things like raw protocol strings and column names to a common data model
    registry_project_column_mapping = load_registry_project_column_mapping(
        registry_name=registry_name
    )
    inverted_column_mapping = {v: k for k, v in registry_project_column_mapping.items()}

    # map raw column strings to cross-registry consistent schema
    raw_data = raw_data.rename(columns=inverted_column_mapping)

    protocol_mapping = load_protocol_mapping()
    inverted_protocol_mapping = load_inverted_protocol_mapping()

    transformed_project_data = (
        raw_data.harmonize_country_names()
        .harmonize_status_codes()
        .map_protocol(
            inverted_protocol_mapping=inverted_protocol_mapping,
        )
        .add_category(protocol_mapping=protocol_mapping)
        .add_is_compliance_flag()
    )

    transformed_project_data['registry'] = registry_name

    for column in ['listed_at']:
        if column in transformed_project_data.columns:
            transformed_project_data = transformed_project_data.to_datetime(
                column, format='mixed', utc=True
            )

    return transformed_project_data


def filter_project_data(data: pd.DataFrame) -> pd.DataFrame:
    # TODO this needs to run through pydantic
    filtered_columns_dtypes = {
        'project_id': str,
        'name': str,
        'protocol': 'object',
        'category': 'object',
        'proponent': str,
        'country': str,
        'status': str,
        'is_compliance': bool,
        'registry': str,
        'project_url': str,
        'retired': float,
        'issued': float,
        'listed_at': pd.DatetimeTZDtype(tz='UTC'),
    }

    for filtered_column in filtered_columns_dtypes:
        if filtered_column not in data:
            data.loc[:, filtered_column] = None
    return data[list(filtered_columns_dtypes.keys())].astype(filtered_columns_dtypes)


@pf.register_dataframe_method
def harmonize_country_names(df: pd.DataFrame, country_column: str = 'country') -> pd.DataFrame:
    print('Harmonizing country names...')
    cc = coco.CountryConverter()
    df[country_column] = cc.pandas_convert(df[country_column], to='name')
    print('Done converting country names...')
    return df


@pf.register_dataframe_method
def add_category(df, protocol_mapping) -> pd.DataFrame:
    """Add category based on protocol"""
    print('Adding category based on protocol...')
    df['category'] = df['protocol'].apply(
        lambda item: get_protocol_category(item, protocol_mapping)
    )
    return df


@pf.register_dataframe_method
def add_is_compliance_flag(df: pd.DataFrame) -> pd.DataFrame:
    """Add is_arb flag"""
    print('Adding is_compliance flag...')
    df['is_compliance'] = df.apply(
        lambda row: np.any([protocol_str.startswith('arb-') for protocol_str in row['protocol']]),
        axis=1,
    )
    return df


@pf.register_dataframe_method
def map_protocol(
    df: pd.DataFrame,
    inverted_protocol_mapping: dict,
    original_protocol_column: str = 'original_protocol',
) -> pd.DataFrame:
    """Map protocol based on known string"""
    print('Mapping protocol based on known string...')
    try:
        df['protocol'] = df[original_protocol_column].apply(
            lambda item: find_protocol(item, inverted_protocol_mapping)
        )
    except KeyError:
        # art-trees doesnt have protocol column
        df['protocol'] = [['unknown']] * len(df)  # protocol column is nested list

    return df


@pf.register_dataframe_method
def harmonize_status_codes(df: pd.DataFrame, status_column: str = 'status') -> pd.DataFrame:
    """Harmonize project status codes across registries

    Excludes ACR, as it requires special treatment across two columns
    """
    print('Harmonizing status codes')
    with contextlib.suppress(KeyError):
        GCC_STATES = {
            'VERIFICATION': 'listed',
            'RFR CC INCOMPLETE': 'unknown',
            'GCC ASSESMENT': 'listed',
            'REGISTERED': 'registered',
            'REQUEST FOR REGISTRATION': 'listed',
        }

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

        state_dict = GCC_STATES | CAR_STATES | VERRA_STATES | GS_STATES
        df[status_column] = df[status_column].apply(lambda x: state_dict.get(x, 'unknown'))
    return df


def find_protocol(search_string: str, inverted_protocol_mapping: dict[str, list[str]]) -> list[str]:
    """Match known strings of project methodologies to internal topology

    Unmatched strings are passed through to the database, until such time that we update mapping data.
    """
    if pd.isna(search_string):  # handle nan case, which crops up in verra data right now
        return ['unknown']
    if known_match := inverted_protocol_mapping.get(search_string.strip()):
        return known_match  # inverted_mapping returns lst
    print(f"'{search_string}' is unmapped in full protocol mapping")
    return [search_string]


def get_protocol_category(protocol_strs: list[str] | str, protocol_mapping: dict) -> list[str]:
    """
    Parameters
    ----------
    protocol_strs : str or list
    normalized protocol strings
    protocol_mapping  dict
    metadata about normalized protocols

    Returns
    -------
    list[str] :
    list of category strings
    """

    def _get_category(protocol_str, protocol_mapping):
        try:
            return protocol_mapping.get(protocol_str).get('category', 'unknown')
        except AttributeError:
            return 'unknown'

    if isinstance(protocol_strs, str):
        protocol_strs = [protocol_strs]
    return [_get_category(protocol_str, protocol_mapping) for protocol_str in protocol_strs]


def harmonize_acr_status(row: pd.Series) -> str:
    """Derive single project status for CAR and ACR projects

    Raw CAR and ACR data has two status columns -- one for compliance status, one for voluntary.
    Handle and harmonize.
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


def load_registry_project_column_mapping(
    *, registry_name: str, file_path: upath.UPath = PROJECT_SCHEMA_UPATH
) -> dict:
    with open(file_path) as file:
        data = json.load(file)

    mapping = {}
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
