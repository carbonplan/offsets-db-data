import pandas as pd
import pandas_flavor as pf

from offsets_db_data.common import (
    BERKELEY_PROJECT_TYPE_UPATH,
    CREDIT_SCHEMA_UPATH,
    PROJECT_SCHEMA_UPATH,
    load_column_mapping,
    load_inverted_protocol_mapping,
    load_registry_project_column_mapping,
    load_type_category_mapping,
)
from offsets_db_data.credits import (
    aggregate_issuance_transactions,  # noqa: F401
    filter_and_merge_transactions,  # noqa: F401
    merge_with_arb,  # noqa: F401
)
from offsets_db_data.models import credit_without_id_schema, project_schema
from offsets_db_data.projects import (
    add_category,  # noqa: F401
    add_first_issuance_and_retirement_dates,  # noqa: F401
    add_is_compliance_flag,  # noqa: F401
    add_retired_and_issued_totals,  # noqa: F401
    harmonize_country_names,  # noqa: F401
    harmonize_status_codes,  # noqa: F401
    map_protocol,  # noqa: F401
)


@pf.register_dataframe_method
def add_cercarbono_project_url(df: pd.DataFrame) -> pd.DataFrame:
    """Add project URL column for Cercarbono projects.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing Cercarbono project data.

    Returns
    -------
    pd.DataFrame
        Dataframe with added project URL column.
    """
    base_url = 'https://www.ecoregistry.io/projects'
    df['project_url'] = df['project_id'].apply(lambda x: f'{base_url}/{x}')
    return df


@pf.register_dataframe_method
def process_cercarbono_transactions(
    projects: pd.DataFrame,
    retirements: pd.DataFrame,
    download_type: str = 'retirements',
    registry_name: str = 'cercarbono',
) -> pd.DataFrame:
    """Process Cercarbono transactions dataframe to conform to offsets-db schema.

    Parameters
    ----------
    projects : pd.DataFrame
        Input dataframe containing Cercarbono project data.
    retirements : pd.DataFrame
        Input dataframe containing Cercarbono retirement data.
    download_type : str, optional
        Type of data to download, by default "retirements"
    registry_name : str, optional
        Name of the registry to be added to the dataframe, by default "cercarbono"

    Returns
    -------
    pd.DataFrame
        Processed dataframe conforming to offsets-db schema.
    """
    all_issuances = []
    for idx, row in projects.iterrows():
        issuances = row['serials']
        for issuance in issuances:
            issuance['project_id'] = row['code']
            issuance['name'] = row['name']
        all_issuances.extend(issuances)

    issuances = pd.json_normalize(all_issuances).rename(
        columns={'issued_quantity': 'quantity', 'issuance_date': 'date'}
    )
    # Extract vintage year from the last date in vintage_of_credits (format: "YYYY-MM-DD / YYYY-MM-DD")
    # TODO: @badgley, please confirm this is the correct way to extract vintage year for issuances
    issuances['vintage'] = (
        issuances['vintage_of_credits'].str.split(' / ').str[-1].str[:4].astype(int)
    )
    issuances['transaction_type'] = 'issuance'
    # add CDC- prefix to project IDs
    retirements['project_id'] = retirements['project_id'].apply(lambda x: f'CDC-{x}')
    retirements['transaction_type'] = 'retirement'

    column_mapping = load_column_mapping(
        registry_name=registry_name, download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )

    columns = {v: k for k, v in column_mapping.items()}

    df = pd.concat([issuances, retirements]).reset_index(drop=True).rename(columns=columns)
    data = (
        df.set_registry(registry_name=registry_name)
        .convert_to_datetime(columns=['transaction_date'], format='ISO8601')
        .add_missing_columns(schema=credit_without_id_schema)
        .validate(schema=credit_without_id_schema)
    )
    return data


@pf.register_dataframe_method
def process_cercarbono_projects(
    df: pd.DataFrame, registry_name: str = 'cercarbono', prefix: str = 'CDC'
) -> pd.DataFrame:
    """Process Cercarbono projects dataframe to conform to offsets-db schema.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing Cercarbono project data.
    registry_name : str, optional
        Name of the registry to be added to the dataframe, by default "cercarbon
    prefix : str, optional
        Prefix to standardize project IDs, by default 'CDC'

    Returns
    -------
    pd.DataFrame
        Processed dataframe conforming to offsets-db schema.
    """

    registry_project_column_mapping = load_registry_project_column_mapping(
        registry_name=registry_name, file_path=PROJECT_SCHEMA_UPATH
    )
    inverted_column_mapping = {value: key for key, value in registry_project_column_mapping.items()}
    type_category_mapping = load_type_category_mapping()
    inverted_protocol_mapping = load_inverted_protocol_mapping()
    df = df.copy()
    df['country'] = df.locations.map(
        lambda x: x[0]['country']
    )  # extract country from locations by taking first entry

    data = (
        df.rename(columns=inverted_column_mapping)
        .set_registry(registry_name=registry_name)
        .add_cercarbono_project_url()
        .harmonize_country_names()
        .harmonize_status_codes()
        .map_protocol(inverted_protocol_mapping=inverted_protocol_mapping)
        .infer_project_type()
        .override_project_types(
            override_data_path=BERKELEY_PROJECT_TYPE_UPATH, source_str='berkeley'
        )
        .add_category(
            type_category_mapping=type_category_mapping
        )  # must come after types; type -> category
        .map_project_type_to_display_name(type_category_mapping=type_category_mapping)
        .add_is_compliance_flag()
        .add_missing_columns(schema=project_schema)
        .convert_to_datetime(columns=['listed_at', 'first_issuance_at', 'first_retirement_at'])
        .validate(schema=project_schema)
    )

    return data
