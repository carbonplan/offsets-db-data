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
def add_isometric_project_url(df: pd.DataFrame) -> pd.DataFrame:
    """Add project URL column for Isometric projects.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing Isometric project data.

    Returns
    -------
    pd.DataFrame
        Dataframe with added project URL column.
    """
    df['project_url'] = df['url']
    return df


@pf.register_dataframe_method
def process_isometric_credits(
    df: pd.DataFrame, *, download_type: str, registry_name: str = 'isometric'
) -> pd.DataFrame:
    """Process Isometric credits dataframe to conform to offsets-db schema.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing Isometric credit transactions data.

    Returns
    -------
    pd.DataFrame
        Dataframe conforming to offsets-db credit schema.
    """
    column_mapping = load_column_mapping(
        registry_name=registry_name, download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )

    columns = {v: k for k, v in column_mapping.items()}
    df = df.copy()

    if not df.empty:
        data = df.rename(columns=columns).set_registry(registry_name=registry_name)

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
def process_isometric_projects(
    df: pd.DataFrame,
    *,
    credits: pd.DataFrame,
    registry_name: str = 'isometric',
) -> pd.DataFrame:
    """Process Isometric projects dataframe to conform to offsets-db schema.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe containing Isometric project data.
    credits : pd.DataFrame
        Dataframe containing credit transactions data.
    registry_name : str, optional
        Name of the registry to be added to the dataframe, by default "isometric"

    Returns
    -------
    pd.DataFrame
        Dataframe conforming to offsets-db project schema.
    """

    registry_project_column_mapping = load_registry_project_column_mapping(
        registry_name=registry_name, file_path=PROJECT_SCHEMA_UPATH
    )
    inverted_column_mapping = {value: key for key, value in registry_project_column_mapping.items()}
    type_category_mapping = load_type_category_mapping()
    inverted_protocol_mapping = load_inverted_protocol_mapping()

    df = df.copy()
    credits = credits.copy()

    data = (
        df.rename(columns=inverted_column_mapping)
        .set_registry(registry_name=registry_name)
        .add_isometric_project_url()
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
        # .add_retired_and_issued_totals(credits=credits)
        # .add_first_issuance_and_retirement_dates(credits=credits)
        .add_missing_columns(schema=project_schema)
        .convert_to_datetime(columns=['listed_at', 'first_issuance_at', 'first_retirement_at'])
        .validate(schema=project_schema)
    )
    return data
