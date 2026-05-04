"""Unit tests for Isometric transformation functions.

All tests use real sample data from tests/data/ via conftest fixtures.
"""

from unittest.mock import patch

import pandas as pd

from offsets_db_data.isometric import (
    add_isometric_project_id,
    add_isometric_project_url,
    process_isometric_credits,
    process_isometric_projects,
)
from offsets_db_data.models import credit_without_id_schema, project_schema

# ── add_isometric_project_id ───────────────────────────────────────────────────


def test_add_isometric_project_id(subtests, raw_isometric_projects):
    # Mirror pipeline state: rename 'short_code' -> 'project_id' before this runs
    df = raw_isometric_projects.rename(columns={'short_code': 'project_id'}).copy()
    result = add_isometric_project_id(df, prefix='ISO')

    with subtests.test('prefix_added'):
        assert result['project_id'].str.startswith('ISO').all()
    with subtests.test('original_codes_preserved'):
        original = raw_isometric_projects['short_code'].astype(str)
        assert (result['project_id'].str[3:] == original).all()


# ── add_isometric_project_url ──────────────────────────────────────────────────


def test_add_isometric_project_url(subtests, raw_isometric_projects):
    result = add_isometric_project_url(raw_isometric_projects)

    with subtests.test('url_column_exists'):
        assert 'project_url' in result.columns
    with subtests.test('url_matches_source'):
        assert (result['project_url'] == raw_isometric_projects['url']).all()


# ── process_isometric_credits ──────────────────────────────────────────────────


def test_process_isometric_credits(
    subtests, raw_isometric_issuances, raw_isometric_retirements, isometric_prj_id_to_short_code
):
    cases = [
        (raw_isometric_issuances, 'issuances', 'issuance'),
        (raw_isometric_retirements, 'retirements', 'retirement'),
    ]
    for df, download_type, expected_type in cases:
        with subtests.test(download_type=download_type):
            result = process_isometric_credits(
                df,
                download_type=download_type,
                prj_id_to_short_code=isometric_prj_id_to_short_code,
                harmonize_beneficiary_info=False,
            )
            credit_without_id_schema.validate(result)
            assert result['project_id'].str.startswith('ISO').all()
            assert (result['transaction_type'] == expected_type).all()
            assert (result['quantity'] >= 0).all()
            assert pd.api.types.is_datetime64_any_dtype(result['transaction_date'])


def test_process_isometric_credits_empty_input():
    result = process_isometric_credits(
        pd.DataFrame(), download_type='issuances', harmonize_beneficiary_info=False
    )
    credit_without_id_schema.validate(result)
    assert result.empty


# ── process_isometric_projects ─────────────────────────────────────────────────


def test_process_isometric_projects(
    subtests,
    raw_isometric_projects,
    raw_isometric_issuances,
    raw_isometric_retirements,
    isometric_prj_id_to_short_code,
):
    credits = pd.concat(
        [
            process_isometric_credits(
                raw_isometric_issuances,
                download_type='issuances',
                prj_id_to_short_code=isometric_prj_id_to_short_code,
                harmonize_beneficiary_info=False,
            ),
            process_isometric_credits(
                raw_isometric_retirements,
                download_type='retirements',
                prj_id_to_short_code=isometric_prj_id_to_short_code,
                harmonize_beneficiary_info=False,
            ),
        ]
    )
    result = process_isometric_projects(raw_isometric_projects, credits=credits)

    with subtests.test('schema'):
        project_schema.validate(result)
    with subtests.test('project_id_prefix'):
        assert result['project_id'].str.startswith('ISO').all()
    with subtests.test('registry'):
        assert (result['registry'] == 'isometric').all()
    with subtests.test('project_url_format'):
        assert result['project_url'].str.startswith('https://').all()


# ── harmonize_beneficiary_data mock ───────────────────────────────────────────


@patch('offsets_db_data.isometric.harmonize_beneficiary_data')
def test_process_isometric_credits_harmonize_beneficiary(
    mock_harmonize, raw_isometric_retirements, isometric_prj_id_to_short_code
):
    """harmonize_beneficiary_info=True invokes harmonize_beneficiary_data."""
    mock_harmonize.side_effect = lambda df, **_: df
    process_isometric_credits(
        raw_isometric_retirements,
        download_type='retirements',
        prj_id_to_short_code=isometric_prj_id_to_short_code,
        harmonize_beneficiary_info=True,
    )
    mock_harmonize.assert_called_once()
