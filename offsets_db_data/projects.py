import contextlib

import country_converter as coco
import janitor  # noqa: F401
import numpy as np
import pandas as pd
import pandas_flavor as pf


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
def add_category(df: pd.DataFrame, *, protocol_mapping: dict) -> pd.DataFrame:
    """
    Add a category to each record in the DataFrame based on its protocol.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing protocol data.
    protocol_mapping : dict
        Dictionary mapping protocol strings to categories.

    Returns
    -------
    pd.DataFrame
        DataFrame with a new 'category' column, derived from the protocol information.
    """

    print('Adding category based on protocol...')
    df['category'] = df['protocol'].apply(
        lambda item: get_protocol_category(protocol_strs=item, protocol_mapping=protocol_mapping)
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
