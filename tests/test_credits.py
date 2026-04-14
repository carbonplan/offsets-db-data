"""Unit tests for credits transformation functions.

Covers: aggregate_issuance_transactions, filter_and_merge_transactions,
        handle_non_issuance_transactions, merge_with_arb, harmonize_beneficiary_data.
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from offsets_db_data.credits import (
    _extract_harmonized_beneficiary_data_via_openrefine,
    aggregate_issuance_transactions,
    filter_and_merge_transactions,
    handle_non_issuance_transactions,
    harmonize_beneficiary_data,
    merge_with_arb,
)


@pytest.fixture
def mixed_credits():
    return pd.DataFrame(
        {
            'project_id': ['VCS1', 'VCS1', 'VCS2', 'VCS2'],
            'transaction_date': pd.to_datetime(
                ['2021-01-01', '2021-01-01', '2022-06-01', '2022-06-01']
            ),
            'vintage': [2020, 2020, 2021, 2021],
            'quantity': [100, 50, 200, 0],
            'registry': ['verra', 'verra', 'verra', 'verra'],
            'transaction_type': ['issuance', 'issuance', 'retirement', 'issuance'],
        }
    )


# ── aggregate_issuance_transactions ───────────────────────────────────────────


def test_aggregate_issuance_transactions_missing_column():
    df = pd.DataFrame({'quantity': [100], 'project_id': ['VCS1']})
    with pytest.raises(KeyError, match='transaction_type'):
        aggregate_issuance_transactions(df)


def test_aggregate_issuance_transactions_no_issuances():
    """When no rows have transaction_type=='issuance', returns empty DataFrame."""
    df = pd.DataFrame(
        {
            'project_id': ['VCS1'],
            'transaction_date': pd.to_datetime(['2021-01-01']),
            'vintage': [2020],
            'quantity': [100],
            'registry': ['verra'],
            'transaction_type': ['retirement'],
        }
    )
    result = aggregate_issuance_transactions(df)
    assert result.empty


def test_aggregate_issuance_transactions_sums_quantities(subtests, mixed_credits):
    """Issuances for the same (project_id, date, vintage) are summed; zero rows dropped."""
    result = aggregate_issuance_transactions(mixed_credits)
    with subtests.test('one_row_returned'):
        assert len(result) == 1
    with subtests.test('project_id'):
        assert result.iloc[0]['project_id'] == 'VCS1'
    with subtests.test('quantity_summed'):
        assert result.iloc[0]['quantity'] == 150


# ── filter_and_merge_transactions ─────────────────────────────────────────────


def test_filter_and_merge_transactions_no_intersection(subtests):
    """No overlapping IDs → registry rows unchanged, ARB rows not added."""
    registry = pd.DataFrame({'project_id': ['VCS1'], 'quantity': [100]})
    arb = pd.DataFrame({'project_id': ['ACR999'], 'quantity': [50]})
    result = filter_and_merge_transactions(registry.copy(), arb_data=arb)
    with subtests.test('registry_rows_preserved'):
        assert 'VCS1' in result['project_id'].values
    with subtests.test('arb_rows_not_added'):
        assert 'ACR999' not in result['project_id'].values


def test_filter_and_merge_transactions_with_intersection(subtests):
    """Overlapping IDs: registry rows removed, ARB rows substituted."""
    registry = pd.DataFrame({'project_id': ['VCS1', 'VCS2'], 'quantity': [100, 200]})
    arb = pd.DataFrame({'project_id': ['VCS1'], 'quantity': [999]})
    result = filter_and_merge_transactions(registry.copy(), arb_data=arb)
    vcs1 = result[result['project_id'] == 'VCS1']
    with subtests.test('arb_quantity_used'):
        assert vcs1.iloc[0]['quantity'] == 999
    with subtests.test('no_duplicate'):
        assert len(vcs1) == 1
    with subtests.test('non_overlap_preserved'):
        assert 'VCS2' in result['project_id'].values


# ── handle_non_issuance_transactions ─────────────────────────────────────────


def test_handle_non_issuance_transactions(subtests, mixed_credits):
    result = handle_non_issuance_transactions(mixed_credits)
    with subtests.test('no_issuances'):
        assert (result['transaction_type'] != 'issuance').all()
    with subtests.test('retirements_kept'):
        assert 'retirement' in result['transaction_type'].values


# ── merge_with_arb ────────────────────────────────────────────────────────────


def test_merge_with_arb_no_overlap(subtests):
    credits = pd.DataFrame({'project_id': ['VCS1'], 'quantity': [100]})
    arb = pd.DataFrame({'project_id': ['ACR999'], 'quantity': [50]})
    result = merge_with_arb(credits, arb=arb)
    with subtests.test('both_rows_present'):
        assert len(result) == 2
    with subtests.test('vcs1_present'):
        assert 'VCS1' in result['project_id'].values
    with subtests.test('acr_present'):
        assert 'ACR999' in result['project_id'].values


def test_merge_with_arb_with_overlap(subtests):
    """Credits for overlapping project IDs are dropped; ARB rows substituted."""
    credits = pd.DataFrame({'project_id': ['VCS1', 'VCS2'], 'quantity': [100, 200]})
    arb = pd.DataFrame({'project_id': ['VCS1'], 'quantity': [999]})
    result = merge_with_arb(credits, arb=arb)
    vcs1 = result[result['project_id'] == 'VCS1']
    with subtests.test('arb_quantity_used'):
        assert vcs1.iloc[0]['quantity'] == 999
    with subtests.test('no_duplicate_vcs1'):
        assert len(vcs1) == 1
    with subtests.test('non_overlap_preserved'):
        assert 'VCS2' in result['project_id'].values


# ── harmonize_beneficiary_data ────────────────────────────────────────────────


def test_harmonize_beneficiary_data_empty_input():
    """Empty DataFrame returns early with harmonized column added."""
    empty = pd.DataFrame(columns=['project_id', 'quantity', 'retirement_beneficiary'])
    result = harmonize_beneficiary_data(empty, registry_name='verra', download_type='retirements')
    assert 'retirement_beneficiary_harmonized' in result.columns
    assert result.empty


@patch('offsets_db_data.credits._extract_harmonized_beneficiary_data_via_openrefine')
def test_harmonize_beneficiary_data_non_empty(mock_openrefine):
    """Non-empty input calls OpenRefine extraction (mocked)."""
    credits = pd.DataFrame(
        {
            'project_id': ['VCS1'],
            'quantity': [100],
            'retirement_beneficiary': ['Some Org'],
        }
    )
    expected = credits.copy()
    expected['retirement_beneficiary_harmonized'] = 'Some Org'
    mock_openrefine.return_value = expected

    result = harmonize_beneficiary_data(credits, registry_name='verra', download_type='retirements')
    mock_openrefine.assert_called_once()
    assert 'retirement_beneficiary_harmonized' in result.columns


@patch('subprocess.run')
@patch('pandas.read_csv')
def test_extract_harmonized_beneficiary_data_via_openrefine(mock_read_csv, mock_run):
    """All 5 subprocess.run calls fire; read_csv result is transformed."""
    mock_run.return_value = MagicMock(stdout='', returncode=0)
    mock_read_csv.return_value = pd.DataFrame({'merged_beneficiary': ['Org A', None, 'Org;%Bad']})

    result = _extract_harmonized_beneficiary_data_via_openrefine(
        temp_path='/tmp/credits.csv',
        project_name='test-project',
        beneficiary_mapping_path='/tmp/mapping.json',
        output_path='/tmp/output.csv',
    )

    assert mock_run.call_count == 5
    assert 'retirement_beneficiary_harmonized' in result.columns
    # 'Org A' maps to 'Org A'; None → nan; 'Org;%Bad' → nan
    assert result['retirement_beneficiary_harmonized'].iloc[0] == 'Org A'


@patch('offsets_db_data.credits._extract_harmonized_beneficiary_data_via_openrefine')
def test_harmonize_beneficiary_data_openrefine_failure(mock_openrefine):
    """CalledProcessError from OpenRefine is re-raised as ValueError."""
    import subprocess

    mock_openrefine.side_effect = subprocess.CalledProcessError(
        returncode=1, cmd='orcli', output='stdout', stderr='stderr'
    )
    credits = pd.DataFrame({'project_id': ['VCS1'], 'quantity': [100]})

    with pytest.raises(ValueError, match='Commad failed with return code'):
        harmonize_beneficiary_data(credits, registry_name='verra', download_type='retirements')
