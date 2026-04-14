"""Shared test fixtures for offsets-db-data tests.

Sample data in tests/data/ was extracted from:
  s3://carbonplan-offsets-db/raw/2026-04-14/

To refresh samples, run:
  python tests/scripts/refresh_sample_data.py
"""

from pathlib import Path

import pandas as pd
import pytest

DATA_DIR = Path(__file__).parent / 'data'

# S3 coordinates that match the local sample data
RAW_DATE = '2026-04-14'
RAW_BUCKET = 's3://carbonplan-offsets-db/raw'


def pytest_collection_modifyitems(items):
    """Auto-mark any test in test_integration.py with the 'integration' marker."""
    for item in items:
        if 'test_integration' in str(item.fspath):
            item.add_marker(pytest.mark.integration)


# ── S3 coordinate fixtures (used by integration tests) ────────────────────────


@pytest.fixture
def date() -> str:
    return RAW_DATE


@pytest.fixture
def bucket() -> str:
    return RAW_BUCKET


# ── Raw data fixtures (load from local sample files) ──────────────────────────
# These fixtures load small CSV samples extracted from S3. Use them as the base
# for unit tests so tests don't need S3 access.


@pytest.fixture
def raw_vcs_projects() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'verra' / 'projects.csv')


@pytest.fixture
def raw_vcs_transactions() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'verra' / 'transactions.csv')


@pytest.fixture
def raw_arb() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'arb' / 'nc-arboc_issuance_sheet3.csv')


@pytest.fixture
def raw_acr_projects() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'american-carbon-registry' / 'projects.csv')


@pytest.fixture
def raw_acr_issuances() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'american-carbon-registry' / 'issuances.csv')


@pytest.fixture
def raw_acr_retirements() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'american-carbon-registry' / 'retirements.csv')


@pytest.fixture
def raw_acr_cancellations() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'american-carbon-registry' / 'cancellations.csv')


@pytest.fixture
def raw_art_projects() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'art-trees' / 'projects.csv')


@pytest.fixture
def raw_art_issuances() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'art-trees' / 'issuances.csv')


@pytest.fixture
def raw_art_retirements() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'art-trees' / 'retirements.csv')


@pytest.fixture
def raw_art_cancellations() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'art-trees' / 'cancellations.csv')


@pytest.fixture
def raw_car_projects() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'climate-action-reserve' / 'projects.csv')


@pytest.fixture
def raw_car_issuances() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'climate-action-reserve' / 'issuances.csv')


@pytest.fixture
def raw_car_retirements() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'climate-action-reserve' / 'retirements.csv')


@pytest.fixture
def raw_car_cancellations() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'climate-action-reserve' / 'cancellations.csv')


@pytest.fixture
def raw_gld_projects() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'gold-standard' / 'projects.csv')


@pytest.fixture
def raw_gld_issuances() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'gold-standard' / 'issuances.csv')


@pytest.fixture
def raw_gld_retirements() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / 'gold-standard' / 'retirements.csv')


# ── Processed fixtures ─────────────────────────────────────────────────────────
# These derive processed DataFrames from the raw fixtures above.
# Import side-effects register all pandas_flavor methods on pd.DataFrame.


@pytest.fixture
def arb(raw_arb) -> pd.DataFrame:
    from offsets_db_data.arb import process_arb  # noqa: F401

    return process_arb(raw_arb)
