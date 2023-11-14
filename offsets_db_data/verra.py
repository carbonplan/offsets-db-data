import janitor  # noqa: F401
import pandas as pd
import pandas_flavor as pf
import upath

from offsets_db_data.common import (
    aggregate_issuance_transactions,  # noqa: F401
    convert_to_datetime,  # noqa: F401
    load_column_mapping,
    rename_columns,  # noqa: F401
    set_registry,  # noqa: F401
)
from offsets_db_data.models import credit_schema

CREDIT_SCHEMA_UPATH = (
    upath.UPath(__file__).parents[0] / 'configs' / 'credits-raw-columns-mapping.json'
)


@pf.register_dataframe_method
def generate_project_ids(df: pd.DataFrame, *, prefix: str) -> pd.DataFrame:
    df['project_id'] = prefix + df['ID'].astype(str)
    return df


@pf.register_dataframe_method
def determine_transaction_type(df: pd.DataFrame, *, date_column: str) -> pd.DataFrame:
    df['transaction_type'] = df[date_column].apply(
        lambda x: 'retirement/cancellation' if pd.notnull(x) else 'issuance'
    )
    return df


@pf.register_dataframe_method
def set_transaction_dates(
    df: pd.DataFrame, *, date_column: str, fallback_column: str
) -> pd.DataFrame:
    df['transaction_date'] = df[date_column].where(df[date_column].notnull(), df[fallback_column])
    return df


@pf.register_dataframe_method
def clean_and_convert_numeric_columns(df: pd.DataFrame, *, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        df[column] = df[column].str.replace(',', '', regex=True)
        df[column] = pd.to_numeric(df[column], errors='coerce')
    return df


@pf.register_dataframe_method
def set_vintage_year(df: pd.DataFrame, *, date_column: str) -> pd.DataFrame:
    df[date_column] = pd.to_datetime(df[date_column], format='%d/%m/%Y', utc=True)
    df['vintage'] = df[date_column].dt.year
    return df


@pf.register_dataframe_method
def calculate_verra_issuances(df: pd.DataFrame) -> pd.DataFrame:
    """Logic to calculate verra transactions from prepocessed transaction data

    Verra allows rolling/partial issuances. This requires inferring vintage issuance from `Total Vintage Quantity`
    """

    df_issuance = df.sort_values('transaction_date').drop_duplicates(
        ['vintage', 'project_id', 'Total Vintage Quantity'], keep='first'
    )

    df_issuance = df_issuance.rename(columns={'Total Vintage Quantity': 'quantity'})

    df_issuance['transaction_type'] = 'issuance'

    return df_issuance


@pf.register_dataframe_method
def calculate_verra_retirements(df: pd.DataFrame) -> pd.DataFrame:
    """retirements + cancelations, but data doesnt allow us to distinguish the two"""
    retirements = df[df['transaction_type'] != 'issuance']
    retirements = retirements.rename(columns={'Quantity Issued': 'quantity'})
    return retirements


@pf.register_dataframe_method
def process_verra_transactions(
    df: pd.DataFrame, *, download_type: str, registry_name: str = 'verra', prefix: str = 'VCS'
) -> pd.DataFrame:
    df = df.copy()
    data = (
        df.set_registry(registry_name=registry_name)
        .generate_project_ids(prefix=prefix)
        .determine_transaction_type(date_column='Retirement/Cancellation Date')
        .set_transaction_dates(
            date_column='Retirement/Cancellation Date', fallback_column='Issuance Date'
        )
        .clean_and_convert_numeric_columns(columns=['Total Vintage Quantity', 'Quantity Issued'])
        .set_vintage_year(date_column='Vintage End')
        .convert_to_datetime(columns=['transaction_date'])
    )

    issuances = data.calculate_verra_issuances()
    retirements = data.calculate_verra_retirements()

    column_mapping = load_column_mapping(
        registry_name='verra', download_type=download_type, mapping_path=CREDIT_SCHEMA_UPATH
    )
    merged_df = (
        pd.concat([issuances, retirements])
        .reset_index(drop=True)
        .rename_columns(column_mapping=column_mapping)
    )

    issuances = merged_df.aggregate_issuance_transactions()
    retirements = merged_df[merged_df['transaction_type'] != 'issuance']
    results = pd.concat([issuances, retirements]).reset_index().rename(columns={'index': 'id'})

    results = credit_schema.validate(results)

    keys = sorted(list(credit_schema.columns.keys()))
    results = results[keys]

    return results
