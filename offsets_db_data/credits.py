import ast
import datetime

import janitor  # noqa: F401
import numpy as np
import pandas as pd
import pandas_flavor as pf


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
def handle_non_issuance_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df_non_issuance = df[df['transaction_type'] != 'issuance']
    return df_non_issuance


@pf.register_dataframe_method
def preprocess_gold_standard_transactions(df: pd.DataFrame, *, download_type: str) -> pd.DataFrame:
    """Preprocess Gold Standard transactions data"""
    df['project'] = df['project'].apply(lambda x: x if isinstance(x, dict) else ast.literal_eval(x))
    transaction_type_mapping = {'issuances': 'issuance', 'retirements': 'retirement'}
    df['transaction_type'] = transaction_type_mapping[download_type]
    df['registry'] = 'gold-standard'

    df['project_id'] = 'GS' + df['project'].apply(lambda x: x.get('sustaincert_id', np.nan)).astype(
        str
    )

    return df


@pf.register_dataframe_method
def preprocess_gcc_transactions(df: pd.DataFrame, *, download_type: str) -> pd.DataFrame:
    """Preprocess GCC transactions data"""

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


@pf.register_dataframe_method
def preprocess_apx_transactions(
    df: pd.DataFrame, *, download_type: str, registry_name: str
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


def filter_and_merge_credits_and_arb(
    *, credits_data: pd.DataFrame, arb_data: pd.DataFrame
) -> pd.DataFrame:
    """
    ARB issuance table contains the authorative version of all credit transactions for ARB projects.
    This function drops all registry crediting data and, isntead, patches in data from the ARB issuance table.

    Parameters
    ----------
    credits_data: pd.DataFrame
        Pandas dataframe containing registry credit data
    arb_data: pd.DataFrame
        Pandas dataframe containing ARB issuance data

    Returns
    -------
    pd.DataFrame
        Pandas dataframe containing merged credit and ARB data
    """
    df = credits_data
    project_id_column = 'project_id'
    if intersection_values := list(
        set(df[project_id_column]).intersection(set(arb_data[project_id_column]))
    ):
        df = df[~df[project_id_column].isin(intersection_values)]

    df = pd.concat([df, arb_data], ignore_index=True)
    return filter_credit_data(df)
