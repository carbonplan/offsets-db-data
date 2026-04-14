"""Unit tests for ARB (California Air Resources Board) transformation functions.

All tests use real sample data from tests/data/ via conftest fixtures.
The `arb` fixture (from conftest) runs process_arb on the raw sample CSV.
"""

import pandas as pd

from offsets_db_data.models import credit_without_id_schema

# Valid project-ID prefixes produced by process_arb
_VALID_PREFIXES = frozenset({'ACR', 'CAR', 'VCS', 'ART'})
# Valid registry values derived from those prefixes
_VALID_REGISTRIES = frozenset(
    {
        'american-carbon-registry',
        'climate-action-reserve',
        'verra',
        'art-trees',
    }
)


# ── process_arb ────────────────────────────────────────────────────────────────


def test_process_arb_schema(arb):
    """Output must conform to credit_without_id_schema."""
    credit_without_id_schema.validate(arb)
    assert set(arb.columns) == set(credit_without_id_schema.columns.keys())


def test_process_arb_project_ids(arb):
    assert arb['project_id'].str[:3].isin(_VALID_PREFIXES).all()


def test_process_arb_transaction_types(subtests, arb):
    types = set(arb['transaction_type'].unique())

    with subtests.test('only_known_types'):
        assert types <= {'issuance', 'retirement'}
    with subtests.test('both_types_present'):
        assert 'issuance' in types
        assert 'retirement' in types


def test_process_arb_quantities(subtests, arb):
    with subtests.test('non_negative'):
        assert (arb['quantity'] >= 0).all()
    with subtests.test('no_nulls'):
        assert arb['quantity'].notna().all()


def test_process_arb_vintage(arb):
    assert pd.api.types.is_integer_dtype(arb['vintage'])
    assert arb['vintage'].between(1990, 2100).all()


def test_process_arb_transaction_date(arb):
    assert pd.api.types.is_datetime64_any_dtype(arb['transaction_date'])
