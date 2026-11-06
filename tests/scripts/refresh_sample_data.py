"""
Refresh local test sample data from S3.

Usage:
    pixi run python tests/scripts/refresh_sample_data.py
    pixi run python tests/scripts/refresh_sample_data.py --date 2026-04-14

The script downloads small slices of each registry's raw files and writes
them as plain CSV into tests/data/. Requires S3 read access.

Note: Cercarbono and Isometric data live in the scratch bucket
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
    credit_samples = []
    for key in ('issuances', 'retirements', 'cancellations'):
        report = pd.read_csv(f'{base}/verra/{key}.csv.gz', low_memory=False)
        out = OUT_DIR / 'verra' / f'{key}.csv'
        sample = verra_credit_sample(report, key)
        sample.to_csv(out, index=False)
        credit_samples.append(sample)
        print(f'  {out.relative_to(OUT_DIR.parent)}  ({len(sample)} rows)')

    report = pd.read_csv(f'{base}/verra/projects.csv.gz', low_memory=False)
    out = OUT_DIR / 'verra' / 'projects.csv'
    sample = verra_project_sample(report, credit_samples)
    sample.to_csv(out, index=False)
    print(f'  {out.relative_to(OUT_DIR.parent)}  ({len(sample)} rows)')

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

    # ── Isometric (scratch bucket) ─────────────────────────────────────────────
    print(f'Refreshing Isometric from {scratch_base}')
    for key in ('projects', 'issuances', 'retirements'):
        _csv(
            f'{scratch_base}/isometric/{key}.csv.gz',
            OUT_DIR / 'isometric' / f'{key}.csv',
        )

    print('Done.')


# Projects the Verra tests in tests/test_vcs.py look up by ID
VERRA_SAMPLE_PROJECTS = [75, 101, 1223, 2498, 3057, 3408]
VERRA_DATE_COLUMNS = {
    'issuances': 'issueDate',
    'retirements': 'retiredDate',
    'cancellations': 'cancellation_date',
}


def verra_credit_sample(report: pd.DataFrame, key: str) -> pd.DataFrame:
    """Every row of the sample projects, plus rows that exercise the transform's edge cases."""
    dates = pd.to_datetime(report[VERRA_DATE_COLUMNS[key]], format='ISO8601')
    picks = [
        report[report['vcsProjectId'].isin(VERRA_SAMPLE_PROJECTS)],
        report[dates != dates.dt.normalize()].head(1),  # a timestamp with a time of day
        report.head(5),
    ]
    if key == 'issuances':  # issuances of one project and vintage on one day, which get summed
        day = dates.dt.normalize().rename('day')
        group_size = report.groupby(['vcsProjectId', 'vintage', day])['id'].transform('size')
        first = report[group_size > 1].head(1)
        picks.append(
            report[
                report['vcsProjectId'].isin(first['vcsProjectId'])
                & report['vintage'].isin(first['vintage'])
                & day.isin(day[first.index])
            ]
        )
    return pd.concat(picks).drop_duplicates('id').sort_values('id')


def verra_project_sample(report: pd.DataFrame, credit_samples: list[pd.DataFrame]) -> pd.DataFrame:
    """The sample projects, plus every project in the credit samples."""
    ids = set(VERRA_SAMPLE_PROJECTS).union(*(sample['vcsProjectId'] for sample in credit_samples))
    return report[report['vcsProjectId'].isin(ids)].sort_values('vcsProjectId')


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
