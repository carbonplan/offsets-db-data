import fsspec
import pandas as pd


def main():
    print('Checking beneficiary coverage against latest production release on S3')

    with fsspec.open(
        'zip://credits.parquet::s3://carbonplan-offsets-db/production/latest/offsets-db.parquet.zip'
    ) as f:
        credits = pd.read_parquet(f)
    retirement_credits = credits[credits['transaction_type'] == 'retirement']

    beneficiary_cols = [
        'retirement_beneficiary',
        'retirement_account',
        'retirement_note',
        'retirement_reason',
    ]
    no_user_data = pd.isna(retirement_credits[beneficiary_cols]).sum(axis=1) == 4

    mapped_stats = (
        retirement_credits[(~no_user_data)]
        .groupby(pd.isna(retirement_credits['retirement_beneficiary_harmonized']))
        .quantity.sum()
    )
    tot_mapped = mapped_stats.sum()
    frac_mapped = mapped_stats[False] / tot_mapped
    nlarge_unmapped = (
        retirement_credits[
            (~no_user_data) & pd.isna(retirement_credits['retirement_beneficiary_harmonized'])
        ].quantity
        > 50_000
    ).sum()

    print(f'A total of {mapped_stats[False] / 1_000_000:.2f} million credits have been mapped')
    print(f'which represents {frac_mapped * 100:.1f} percent of mappable credit')
    print(f'There are {nlarge_unmapped} mappable transactions that 50,000 credits')


if __name__ == '__main__':
    main()
