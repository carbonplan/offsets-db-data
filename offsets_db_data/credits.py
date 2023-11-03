import ast
import datetime
import json

import janitor  # noqa: F401
import numpy as np
import pandas as pd
import pandas_flavor as pf
import upath

CREDIT_SCHEMA_UPATH = (
    upath.UPath(__file__).parents[0] / 'configs' / 'credits-raw-columns-mapping.json'
)


@pf.register_dataframe_method
def filter_and_merge_transactions(
    df: pd.DataFrame, arb_data: pd.DataFrame, project_id_column: str = 'project_id'
) -> pd.DataFrame:
    if intersection_values := list(
        set(df[project_id_column]).intersection(set(arb_data[project_id_column]))
    ):
        df = df[~df[project_id_column].isin(intersection_values)]
        df = pd.concat(
            [df, arb_data[arb_data[project_id_column].isin(intersection_values)]], ignore_index=True
        )
    return df


@pf.register_dataframe_method
def aggregate_issuance_transactions(df: pd.DataFrame) -> pd.DataFrame:
    # Check if 'transaction_type' exists in DataFrame columns
    if 'transaction_type' not in df.columns:
        raise KeyError("The column 'transaction_type' is missing.")

    # Initialize df_issuance_agg to an empty DataFrame
    df_issuance_agg = pd.DataFrame()
    df = df.copy()
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


@pf.register_dataframe_method
def handle_non_issuance_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df_non_issuance = df[df['transaction_type'] != 'issuance']
    return df_non_issuance


def calculate_verra_issuances(*, df: pd.DataFrame) -> pd.DataFrame:
    """Logic to calculate verra transactions from prepocessed transaction data

    Verra allows rolling/partial issuances. This requires inferring vintage issuance from `Total Vintage Quantity`
    """

    df_issuance = (
        df.sort_values('transaction_date')
        .drop_duplicates(['vintage', 'project_id', 'Total Vintage Quantity'], keep='first')
        .copy()
    )

    df_issuance = df_issuance.rename(columns={'Total Vintage Quantity': 'quantity'})

    df_issuance['transaction_type'] = 'issuance'

    return df_issuance


def calculate_verra_retirements(*, df: pd.DataFrame) -> pd.DataFrame:
    """retirements + cancelations, but data doesnt allow us to distinguish the two"""
    retirements = df[df['transaction_type'] != 'issuance']
    retirements = retirements.rename(columns={'Quantity Issued': 'quantity'})
    return retirements


def preprocess_verra_transactions(*, df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess Verra transactions data"""

    df = df.copy()
    df['registry'] = 'verra'
    df['project_id'] = 'VCS' + df['ID'].astype(str)
    df['transaction_type'] = df['Retirement/Cancellation Date'].apply(
        lambda x: 'retirement/cancellation' if pd.notnull(x) else 'issuance'
    )
    df['transaction_date'] = df['Retirement/Cancellation Date'].where(
        df['Retirement/Cancellation Date'].notnull(), df['Issuance Date']
    )

    # Remove commas from 'Total Vintage Quantity' and 'Quantity Issued' columns
    df['Total Vintage Quantity'] = df['Total Vintage Quantity'].str.replace(',', '', regex=True)
    df['Quantity Issued'] = df['Quantity Issued'].str.replace(',', '', regex=True)

    # Convert the columns to numeric (float)
    df['Total Vintage Quantity'] = pd.to_numeric(df['Total Vintage Quantity'], errors='coerce')
    df['Quantity Issued'] = pd.to_numeric(df['Quantity Issued'], errors='coerce')

    df.to_datetime('Vintage End', format='%d/%m/%Y')  # from janitor, changes inplace
    df['vintage'] = df['Vintage End'].dt.year
    df.to_datetime('transaction_date', format='%d/%m/%Y')  # from janitor, changes inplace
    return df


def preprocess_gold_standard_transactions(*, df: pd.DataFrame, download_type: str) -> pd.DataFrame:
    """Preprocess Gold Standard transactions data"""
    df = df.copy()
    df['project'] = df['project'].apply(lambda x: x if isinstance(x, dict) else ast.literal_eval(x))
    transaction_type_mapping = {'issuances': 'issuance', 'retirements': 'retirement'}
    df['transaction_type'] = transaction_type_mapping[download_type]
    df['registry'] = 'gold-standard'

    df['project_id'] = 'GS' + df['project'].apply(lambda x: x.get('sustaincert_id', np.nan)).astype(
        str
    )

    return df


def add_gcc_project_id(*, transactions, projects):
    projects_dict_list = projects[['project_id', 'name']].to_dict(orient='records')
    result_dict = {d['name']: d['project_id'] for d in projects_dict_list}

    # rename the project_id column to project_name
    # df = df.rename(columns={'project_id': 'original_project_id'})
    transactions['project_id'] = transactions['project_name'].map(result_dict)
    return transactions


def preprocess_gcc_transactions(*, df: pd.DataFrame, download_type: str) -> pd.DataFrame:
    """Preprocess GCC transactions data"""
    df = df.copy()

    # Apply the function to the DataFrame column
    df['vintage'] = df['vintage'].apply(
        lambda vintage: vintage.split(' - ')[-1] if ' - ' in vintage else vintage
    )

    # if retirement_date is null, then it's an issuance
    if download_type == 'issuances':
        df['transaction_type'] = 'issuance'
        # TODO: Figure out how to get the proper issuance date
        df['transaction_date'] = None
    elif download_type == 'retirements':
        df['transaction_type'] = 'retirement'
        # if retirement_date is set, then transaction_date is retirement_date else None
        df['transaction_date'] = df['retirement_date'].apply(
            lambda unix_time: datetime.datetime.fromtimestamp(unix_time / 1000).strftime(
                '%Y-%m-%d %H:%M:%S'
            )
            if pd.notnull(unix_time)
            else None
        )

    df['registry'] = 'global-carbon-council'
    return df


def preprocess_apx_transactions(
    *, df: pd.DataFrame, download_type: str, registry_name: str
) -> pd.DataFrame:
    transaction_type_mapping = {
        'issuances': 'issuance',
        'retirements': 'retirement',
        'cancellations': 'cancellation',
    }
    df['transaction_type'] = transaction_type_mapping[download_type]
    df['registry'] = registry_name
    return df


def filter_credit_data(data: pd.DataFrame) -> pd.DataFrame:
    filtered_columns_dtypes = {
        'project_id': str,
        'vintage': int,
        'quantity': int,
        'transaction_type': str,
        'transaction_date': pd.DatetimeTZDtype(tz='UTC'),
        'registry': str,
    }

    for filtered_column in filtered_columns_dtypes:
        if filtered_column not in data:
            data.loc[:, filtered_column] = None
    return data.astype(filtered_columns_dtypes)[
        sorted(list(filtered_columns_dtypes.keys()))
    ].sort_values(by=['project_id', 'vintage'])


def transform_raw_registry_data(
    *,
    raw_data: pd.DataFrame,
    registry_name: str,
    download_type: str,
) -> pd.DataFrame:
    with open(CREDIT_SCHEMA_UPATH) as f:
        registry_credit_column_mapping = json.load(f)

    column_mapping = registry_credit_column_mapping[registry_name][download_type]

    inverted_column_mapping = {v: k for k, v in column_mapping.items()}
    # map raw column strings to cross-registry consistent schema
    df = raw_data.rename(columns=inverted_column_mapping)

    for column in ['transaction_date']:
        if column in df.columns:
            df = df.to_datetime(column, format='mixed', utc=True)

    return filter_credit_data(df)


def filter_and_merge_credits_and_arb(
    *, credits_data: pd.DataFrame, arb_data: pd.DataFrame
) -> pd.DataFrame:
    df = credits_data.copy()
    project_id_column = 'project_id'
    if intersection_values := list(
        set(df[project_id_column]).intersection(set(arb_data[project_id_column]))
    ):
        df = df[~df[project_id_column].isin(intersection_values)]
        df = pd.concat(
            [df, arb_data[arb_data[project_id_column].isin(intersection_values)]], ignore_index=True
        )
    return filter_credit_data(df)
