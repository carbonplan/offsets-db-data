import json

import pandas as pd
import pandas_flavor as pf


@pf.register_dataframe_method
def set_registry(df: pd.DataFrame, registry_name: str) -> pd.DataFrame:
    df['registry'] = registry_name
    return df


def load_column_mapping(*, registry_name: str, download_type: str, mapping_path: str) -> dict:
    with open(mapping_path) as f:
        registry_credit_column_mapping = json.load(f)
    return registry_credit_column_mapping[registry_name][download_type]


@pf.register_dataframe_method
def rename_columns(df: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    inverted_column_mapping = {v: k for k, v in column_mapping.items()}
    return df.rename(columns=inverted_column_mapping)


@pf.register_dataframe_method
def convert_to_datetime(
    df: pd.DataFrame, *, columns: list, date_format: str = 'mixed', utc: bool = True
) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], format=date_format, utc=utc)
    return df


@pf.register_dataframe_method
def aggregate_issuance_transactions(df: pd.DataFrame) -> pd.DataFrame:
    # Check if 'transaction_type' exists in DataFrame columns
    if 'transaction_type' not in df.columns:
        raise KeyError("The column 'transaction_type' is missing.")

    # Initialize df_issuance_agg to an empty DataFrame
    df_issuance_agg = pd.DataFrame()
    df_issuance = df[df['transaction_type'] == 'issuance']

    if not df_issuance.empty:
        df_issuance_agg = (
            df_issuance.groupby(['project_id', 'transaction_date', 'vintage'])
            .agg(
                {
                    'quantity': 'sum',
                    'registry': 'first',
                    'transaction_type': 'first',
                }
            )
            .reset_index()
        )
        df_issuance_agg = df_issuance_agg[df_issuance_agg['quantity'] > 0]
    return df_issuance_agg
