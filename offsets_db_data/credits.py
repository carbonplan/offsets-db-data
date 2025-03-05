import datetime
import pathlib
import subprocess
import tempfile
import uuid

import janitor  # noqa: F401
import numpy as np
import pandas as pd
import pandas_flavor as pf
import upath

BENEFICIARY_MAPPING_UPATH = (
    upath.UPath(__file__).parents[0] / 'configs' / 'beneficiary-mappings.json'
)


@pf.register_dataframe_method
def aggregate_issuance_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate issuance transactions by summing the quantity for each combination of project ID, transaction date, and vintage.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing issuance transaction data.

    Returns
    -------
    pd.DataFrame
        DataFrame with aggregated issuance transactions, filtered to include only those with a positive quantity.
    """

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
    """
    Filter transactions based on project ID intersection with ARB data and merge the filtered transactions.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with transaction data.
    arb_data : pd.DataFrame
        DataFrame containing ARB issuance data.
    project_id_column : str, optional
        The name of the column containing project IDs (default is 'project_id').

    Returns
    -------
    pd.DataFrame
        DataFrame with transactions from the input DataFrame, excluding those present in ARB data, merged with relevant ARB transactions.
    """

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
    """
    Filter the DataFrame to include only non-issuance transactions.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing transaction data.

    Returns
    -------
    pd.DataFrame
        DataFrame containing only transactions where 'transaction_type' is not 'issuance'.
    """

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


def harmonize_beneficiary_data(
    credits: pd.DataFrame, registry_name: str, download_type: str
) -> pd.DataFrame:
    """
    Harmonize the beneficiary information by removing the 'beneficiary_id' column and renaming the 'beneficiary_name' column to 'beneficiary'.

    Parameters
    ----------
    credits : pd.DataFrame
        Input DataFrame containing credit data.
    """

    tempdir = tempfile.gettempdir()
    temp_path = pathlib.Path(tempdir) / f'{registry_name}-{download_type}-credits.csv'

    if len(credits) == 0:
        print(
            f'Empty dataframe with shape={credits.shape} - columns:{credits.columns.tolist()}. No credits to harmonize'
        )
        data = credits.copy()
        data['retirement_beneficiary_harmonized'] = pd.Series(dtype='str')
        return data
    credits.to_csv(temp_path, index=False)

    project_name = f'{registry_name}-{download_type}-beneficiary-harmonization-{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}-{uuid.uuid4()}'
    output_path = pathlib.Path(tempdir) / f'{project_name}.csv'

    try:
        return _extract_harmonized_beneficiary_data_via_openrefine(
            temp_path, project_name, str(BENEFICIARY_MAPPING_UPATH), str(output_path)
        )

    except subprocess.CalledProcessError as e:
        raise ValueError(
            f'Commad failed with return code: {e.returncode}\nOutput: {e.output}\nError output: {e.stderr}'
        ) from e


def _extract_harmonized_beneficiary_data_via_openrefine(
    temp_path, project_name, beneficiary_mapping_path, output_path
):
    result = subprocess.run(
        [
            'offsets-db-data-orcli',
            'run',
            '--',
            'import',
            'csv',
            str(temp_path),
            '--projectName',
            f'{project_name}',
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    result = subprocess.run(
        ['offsets-db-data-orcli', 'run', '--', 'info', project_name],
        capture_output=True,
        text=True,
        check=True,
    )

    result = subprocess.run(
        [
            'offsets-db-data-orcli',
            'run',
            '--',
            'transform',
            project_name,
            beneficiary_mapping_path,
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    result = subprocess.run(
        [
            'offsets-db-data-orcli',
            'run',
            '--',
            'export',
            'csv',
            project_name,
            '--output',
            output_path,
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    result = subprocess.run(
        ['offsets-db-data-orcli', 'run', '--', 'delete', project_name],
        capture_output=True,
        text=True,
        check=True,
    )

    print(result.stdout)

    data = pd.read_csv(output_path)
    data['merged_beneficiary'] = data['merged_beneficiary'].fillna('').astype(str)
    data['retirement_beneficiary_harmonized'] = np.where(
        data['merged_beneficiary'].notnull() & (~data['merged_beneficiary'].str.contains(';%')),
        data['merged_beneficiary'],
        '',
    )
    return data
