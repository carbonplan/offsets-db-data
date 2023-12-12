import janitor  # noqa: F401
import numpy as np
import pandas as pd
import pandas_flavor as pf

from offsets_db_data.common import convert_to_datetime  # noqa: F401
from offsets_db_data.models import credit_without_id_schema


def _get_registry(item):
    registry_map = {
        'CAR': 'climate-action-reserve',
        'ACR': 'american-carbon-registry',
        'VCS': 'verra',
        'ART': 'art-trees',
    }
    prefix = item[:3]
    return registry_map.get(prefix)


@pf.register_dataframe_method
def process_arb(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process ARB (Air Resources Board) data by renaming columns, handling nulls, interpolating vintages,
    and transforming the data structure for transactions.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame containing raw ARB data.

    Returns
    -------
    data : pd.DataFrame
        Processed DataFrame with ARB data. Columns include 'opr_id', 'vintage', 'issued_at' (interpolated),
        various credit transaction types, and quantities. The DataFrame is also validated against
        a predefined schema for credit data.

    Notes
    -----
    - The function renames columns for readability and standardization.
    - It interpolates missing vintage values and handles NaNs in 'issuance' column.
    - Retirement transactions are derived based on compliance period dates.
    - The DataFrame is melted to restructure credit data.
    - Zero retirement events are dropped as they are considered artifacts.
    - A prefix is added to 'project_id' to indicate the source.
    - The 'registry' column is derived based on the project_id prefix.
    - The 'vintage' column is converted to integer type.
    - Finally, the data is converted to datetime where necessary and validated against a predefined schema.
    """

    df = df.copy()

    rename_d = {
        'OPR Project ID': 'opr_id',
        'ARB Offset Credits Issued': 'issuance',
        'Project Type': 'project_type',
        'Issuance Date': 'issued_at',
        'Vintage': 'vintage',
        'Retired Voluntarily': 'vcm_retirement',
        'Retired 1st Compliance Period (CA)': 'first_compliance_ca',
        'Retired 2nd Compliance Period (CA)': 'second_compliance_ca',
        'Retired 3rd Compliance Period (CA)': 'third_compliance_ca',
        'Retired 4th Compliance Period (CA)': 'fourth_compliance_ca',
        'Retired for Compliance in Quebec': 'qc_compliance',
    }

    df = df.rename(columns=rename_d)
    df['vintage'] = df[
        'vintage'
    ].interpolate()  # data is ordered; fills na vintage for zero issuance reporting periods

    df['project_type'] = df['project_type'].str.lower()

    # can be multiple issuance in single RP -- grab issuance ID so can aggregate later

    df = df.replace('reforest defer', np.nan)
    df.loc[pd.isna(df['issuance']), 'issuance'] = 0

    print(f'Loaded {len(df)} rows from ARB issuance table')
    df = df[rename_d.values()]

    compliance_period_dates = {
        'vcm_retirement': np.datetime64('NaT'),
        'qc_compliance': np.datetime64('NaT'),
        'first_compliance_ca': np.datetime64('2016-03-21'),
        'second_compliance_ca': np.datetime64('2018-11-01'),
        'third_compliance_ca': np.datetime64('2021-11-01'),
        'fourth_compliance_ca': np.datetime64('2022-11-01'),
    }
    # rename columns to what we want `transaction_type` to be in the end. then call melt
    # which casts to (opr_id, vintage, issued_at, transaction_type, quantity)
    credit_cols = [
        'issuance',
        'vcm_retirement',
        'first_compliance_ca',
        'second_compliance_ca',
        'third_compliance_ca',
        'fourth_compliance_ca',
        'qc_compliance',
    ]
    melted = df.melt(
        id_vars=['opr_id', 'vintage', 'issued_at'],
        value_vars=credit_cols,
        var_name='transaction_type',
        value_name='quantity',
    )
    melted.loc[
        melted['transaction_type'].isin(compliance_period_dates.keys()), 'issued_at'
    ] = melted['transaction_type'].map(compliance_period_dates)
    melted = melted.rename(columns={'issued_at': 'transaction_date'}).to_datetime(
        'transaction_date', format='mixed', utc=True
    )
    melted['transaction_type'] = melted.transaction_type.apply(
        lambda x: 'retirement' if x in compliance_period_dates else x
    )

    # handle missing in retirement cols (i.e. ACR570 2022)
    melted.loc[pd.isna(melted['quantity']), 'quantity'] = 0

    # drop all th zero retirement events, as they're artifacts of processing steps
    data = melted[
        ~((melted['transaction_type'] == 'retirement') & (melted['quantity'] == 0))
    ].copy()
    # add a prefix to the project_id to indicate the source
    data['project_id'] = data.opr_id.apply(
        lambda item: item
        if isinstance(item, str)
        and (item.startswith('CAR') or item.startswith('ACR') or item.startswith('VCS'))
        else f'VCS{item}'
    )
    data['registry'] = data.project_id.apply(_get_registry)
    data['vintage'] = data['vintage'].astype(int)

    data = data.convert_to_datetime(columns=['transaction_date']).validate(
        schema=credit_without_id_schema
    )

    return data
