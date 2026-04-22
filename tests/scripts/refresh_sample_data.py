"""
Refresh local test sample data from S3.

Usage:
    pixi run python tests/scripts/refresh_sample_data.py
    pixi run python tests/scripts/refresh_sample_data.py --date 2026-04-14

The script downloads small slices of each registry's raw files and writes
them as plain CSV into tests/data/. Requires S3 read access.

Note: Cercarbono data lives in the scratch bucket
  (s3://carbonplan-scratch/offsets-db-test/raw/) until promoted
  to the production bucket (s3://carbonplan-offsets-db/raw/).
"""

import argparse
import io
from pathlib import Path

import pandas as pd
import s3fs

BASE_S3 = 's3://carbonplan-offsets-db/raw'
SCRATCH_S3 = 's3://carbonplan-scratch/offsets-db-test/raw'
OUT_DIR = Path(__file__).parent.parent / 'data'


def main(date: str) -> None:
    fs = s3fs.S3FileSystem(anon=False)
    base = f'{BASE_S3}/{date}'
    scratch_base = f'{SCRATCH_S3}/{date}'
    print(f'Refreshing sample data from {base}')

    # ── Verra ─────────────────────────────────────────────────────────────────
    _csv(f'{base}/verra/projects.csv.gz', OUT_DIR / 'verra' / 'projects.csv', nrows=50)

    # transactions: balanced mix of issuances and retirements
    txn = pd.read_csv(f'{base}/verra/transactions.csv.gz', nrows=5000)
    has_ret = txn[txn['Retirement/Cancellation Date'].notna()].head(100)
    no_ret = txn[txn['Retirement/Cancellation Date'].isna()].head(100)
    out = OUT_DIR / 'verra' / 'transactions.csv'
    pd.concat([has_ret, no_ret]).to_csv(out, index=False)
    print(
        f'  {out.relative_to(OUT_DIR.parent)}  ({len(has_ret)} retirements + {len(no_ret)} issuances)'
    )

    # ── American Carbon Registry ───────────────────────────────────────────────
    for key in ('projects', 'issuances', 'retirements', 'cancellations'):
        _csv(
            f'{base}/american-carbon-registry/{key}.csv.gz',
            OUT_DIR / 'american-carbon-registry' / f'{key}.csv',
        )

    # ── ART-Trees ─────────────────────────────────────────────────────────────
    for key in ('projects', 'issuances', 'retirements', 'cancellations'):
        _csv(
            f'{base}/art-trees/{key}.csv.gz',
            OUT_DIR / 'art-trees' / f'{key}.csv',
        )

    # ── Climate Action Reserve ─────────────────────────────────────────────────
    for key in ('projects', 'issuances', 'retirements', 'cancellations'):
        _csv(
            f'{base}/climate-action-reserve/{key}.csv.gz',
            OUT_DIR / 'climate-action-reserve' / f'{key}.csv',
        )

    # ── Gold Standard ──────────────────────────────────────────────────────────
    for key in ('projects', 'issuances', 'retirements'):
        _csv(
            f'{base}/gold-standard/{key}.csv.gz',
            OUT_DIR / 'gold-standard' / f'{key}.csv',
        )

    # ── ARB (xlsx, sheet index 3) ──────────────────────────────────────────────
    out = OUT_DIR / 'arb' / 'nc-arboc_issuance_sheet3.csv'
    with fs.open(f'{base}/arb/nc-arboc_issuance.xlsx', 'rb') as f:
        content = f.read()
    df = pd.read_excel(io.BytesIO(content), sheet_name=3, nrows=100)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f'  {out.relative_to(OUT_DIR.parent)}  ({len(df)} rows)')

    # ── Cercarbono (scratch bucket) ────────────────────────────────────────────
    print(f'Refreshing Cercarbono from {scratch_base}')
    for key in ('projects', 'issuances', 'retirements'):
        _csv(
            f'{scratch_base}/cercarbono/{key}.csv.gz',
            OUT_DIR / 'cercarbono' / f'{key}.csv',
        )

    print('Done.')


def _csv(src: str, dst: Path, nrows: int = 100) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(src, nrows=nrows)
    df.to_csv(dst, index=False)
    print(f'  {dst.relative_to(OUT_DIR.parent)}  ({len(df)} rows)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default='2026-04-14', help='S3 date partition (YYYY-MM-DD)')
    args = parser.parse_args()
    main(args.date)
