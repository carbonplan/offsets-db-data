import janitor  # noqa: F401
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
def merge_with_arb(credits: pd.DataFrame, *, arb: pd.DataFrame) -> pd.DataFrame:
    """
    ARB issuance table contains the authorative version of all credit transactions for ARB projects.
    This function drops all registry crediting data and, isntead, patches in data from the ARB issuance table.

    Parameters
    ----------
    credits: pd.DataFrame
        Pandas dataframe containing registry credit data
    arb: pd.DataFrame
        Pandas dataframe containing ARB issuance data

    Returns
    -------
    pd.DataFrame
        Pandas dataframe containing merged credit and ARB data
    """
    df = credits
    project_id_column = 'project_id'
    if intersection_values := list(
        set(df[project_id_column]).intersection(set(arb[project_id_column]))
    ):
        df = df[~df[project_id_column].isin(intersection_values)]

    df = pd.concat([df, arb], ignore_index=True)
    return df
