"""Unit tests for Cercarbono transformation functions.

All tests use real sample data from tests/data/ via conftest fixtures.
"""

from unittest.mock import patch

import pandas as pd

from offsets_db_data.cercarbono import (
    add_cercarbono_project_id,
    add_cercarbono_project_url,
    process_cercarbono_credits,
    process_cercarbono_projects,
)
from offsets_db_data.models import credit_without_id_schema, project_schema

# ── add_cercarbono_project_id ──────────────────────────────────────────────────


def test_add_cercarbono_project_id(subtests, raw_cercarbono_projects):
    result = add_cercarbono_project_id(raw_cercarbono_projects, prefix='CCB')

    with subtests.test('prefix_added'):
        assert result['project_id'].str.startswith('CCB').all()
    with subtests.test('numeric_suffix'):
        assert result['project_id'].str[3:].str.isnumeric().all()
    with subtests.test('original_ids_preserved'):
        original = raw_cercarbono_projects['id'].astype(str)
        assert (result['project_id'].str[3:] == original).all()


# ── add_cercarbono_project_url ─────────────────────────────────────────────────


def test_add_cercarbono_project_url(subtests, raw_cercarbono_projects):
    # Mirror pipeline state: rename 'code' -> 'project_id' before URL is built
    df = raw_cercarbono_projects.rename(columns={'code': 'project_id'}).copy()
    result = add_cercarbono_project_url(df)

    base = 'https://www.ecoregistry.io/projects/'
    with subtests.test('url_column_exists'):
        assert 'project_url' in result.columns
    with subtests.test('url_base'):
        assert result['project_url'].str.startswith(base).all()
    with subtests.test('url_contains_code'):
        suffix = result['project_url'].str.removeprefix(base)
        assert (suffix == raw_cercarbono_projects['code']).all()


# ── process_cercarbono_credits ─────────────────────────────────────────────────


def test_process_cercarbono_credits(subtests, raw_cercarbono_issuances, raw_cercarbono_retirements):
    cases = [
        (raw_cercarbono_issuances, 'issuances', 'issuance'),
        (raw_cercarbono_retirements, 'retirements', 'retirement'),
    ]
    for df, download_type, expected_type in cases:
        with subtests.test(download_type=download_type):
            result = process_cercarbono_credits(
                df, download_type=download_type, harmonize_beneficiary_info=False
            )
            credit_without_id_schema.validate(result)
            assert result['project_id'].str.startswith('CCB').all()
            assert (result['transaction_type'] == expected_type).all()
            assert (result['quantity'] >= 0).all()
            assert pd.api.types.is_datetime64_any_dtype(result['transaction_date'])


# ── process_cercarbono_projects ────────────────────────────────────────────────


def test_process_cercarbono_projects(
    subtests, raw_cercarbono_projects, raw_cercarbono_issuances, raw_cercarbono_retirements
):
    credits = pd.concat(
        [
            process_cercarbono_credits(
                raw_cercarbono_issuances,
                download_type='issuances',
                harmonize_beneficiary_info=False,
            ),
            process_cercarbono_credits(
                raw_cercarbono_retirements,
                download_type='retirements',
                harmonize_beneficiary_info=False,
            ),
        ]
    )
    result = process_cercarbono_projects(raw_cercarbono_projects, credits=credits)

    with subtests.test('schema'):
        project_schema.validate(result)
    with subtests.test('project_id_prefix'):
        assert result['project_id'].str.startswith('CCB').all()
    with subtests.test('registry'):
        assert (result['registry'] == 'cercarbono').all()
    with subtests.test('project_url_format'):
        assert result['project_url'].str.startswith('https://www.ecoregistry.io/projects/').all()
    with subtests.test('is_compliance_dtype'):
        assert result['is_compliance'].dtype == bool


# ── harmonize_beneficiary_data mock ───────────────────────────────────────────


@patch('offsets_db_data.cercarbono.harmonize_beneficiary_data')
def test_process_cercarbono_credits_harmonize_beneficiary(
    mock_harmonize, raw_cercarbono_retirements
):
    """harmonize_beneficiary_info=True invokes harmonize_beneficiary_data."""
    mock_harmonize.side_effect = lambda df, **_: df
    process_cercarbono_credits(
        raw_cercarbono_retirements,
        download_type='retirements',
        harmonize_beneficiary_info=True,
    )
    mock_harmonize.assert_called_once()
