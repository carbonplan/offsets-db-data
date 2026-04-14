"""Unit tests for Gold Standard transformation functions.

All tests use real sample data from tests/data/ via conftest fixtures.
"""

import pandas as pd

from offsets_db_data.gld import (
    add_gld_project_id,
    add_gld_project_url,
    determine_gld_transaction_type,
    process_gld_credits,
    process_gld_projects,
)
from offsets_db_data.models import credit_without_id_schema, project_schema

# ── determine_gld_transaction_type ─────────────────────────────────────────────


def test_determine_gld_transaction_type(subtests, raw_gld_issuances, raw_gld_retirements):
    cases = [
        (raw_gld_issuances, 'issuances', 'issuance'),
        (raw_gld_retirements, 'retirements', 'retirement'),
    ]
    for df, download_type, expected_type in cases:
        with subtests.test(download_type=download_type):
            result = determine_gld_transaction_type(df.copy(), download_type=download_type)
            assert 'transaction_type' in result.columns
            assert (result['transaction_type'] == expected_type).all()


# ── add_gld_project_id ─────────────────────────────────────────────────────────


def test_add_gld_project_id(subtests, raw_gld_issuances):
    # Mirror the pipeline: GSID is renamed to project_id before this function runs
    df = raw_gld_issuances.rename(columns={'GSID': 'project_id'}).copy()
    result = add_gld_project_id(df, prefix='GLD')

    with subtests.test('prefix_added'):
        assert result['project_id'].str.startswith('GLD').all()
    with subtests.test('original_ids_preserved'):
        original = raw_gld_issuances['GSID'].astype(str)
        assert (result['project_id'].str[3:] == original).all()


# ── add_gld_project_url ────────────────────────────────────────────────────────


def test_add_gld_project_url(subtests, raw_gld_projects):
    # Mirror pipeline state after rename + add_gld_project_id
    df = raw_gld_projects.rename(columns={'GSID': 'project_id'}).copy()
    df = add_gld_project_id(df, prefix='GLD')
    result = add_gld_project_url(df)

    base = 'https://registry.goldstandard.org/projects?q=gs'
    with subtests.test('url_column_exists'):
        assert 'project_url' in result.columns
    with subtests.test('url_base'):
        assert result['project_url'].str.startswith(base).all()
    with subtests.test('url_contains_project_id'):
        # URL is base + full project_id (e.g. "...?q=gsGLD23574")
        assert result.apply(lambda row: str(row['project_id']) in row['project_url'], axis=1).all()


# ── process_gld_credits ────────────────────────────────────────────────────────


def test_process_gld_credits(subtests, raw_gld_issuances, raw_gld_retirements):
    cases = [
        (raw_gld_issuances, 'issuances', 'issuance'),
        (raw_gld_retirements, 'retirements', 'retirement'),
    ]
    for df, download_type, expected_type in cases:
        with subtests.test(download_type=download_type):
            result = process_gld_credits(
                df, download_type=download_type, harmonize_beneficiary_info=False
            )
            credit_without_id_schema.validate(result)
            assert result['project_id'].str.startswith('GLD').all()
            assert (result['transaction_type'] == expected_type).all()
            assert (result['quantity'] >= 0).all()
            assert pd.api.types.is_datetime64_any_dtype(result['transaction_date'])


def test_process_gld_credits_empty_input():
    result = process_gld_credits(
        pd.DataFrame(), download_type='issuances', harmonize_beneficiary_info=False
    )
    credit_without_id_schema.validate(result)
    assert result.empty


# ── process_gld_projects ───────────────────────────────────────────────────────


def test_process_gld_projects(subtests, raw_gld_projects, raw_gld_issuances, raw_gld_retirements):
    credits = pd.concat(
        [
            process_gld_credits(
                raw_gld_issuances, download_type='issuances', harmonize_beneficiary_info=False
            ),
            process_gld_credits(
                raw_gld_retirements, download_type='retirements', harmonize_beneficiary_info=False
            ),
        ]
    )
    result = process_gld_projects(raw_gld_projects, credits=credits)

    with subtests.test('schema'):
        project_schema.validate(result)
    with subtests.test('project_id_prefix'):
        assert result['project_id'].str.startswith('GLD').all()
    with subtests.test('registry'):
        assert (result['registry'] == 'gold-standard').all()
    with subtests.test('project_url_format'):
        assert result['project_url'].str.startswith('https://registry.goldstandard.org').all()
    with subtests.test('is_compliance_dtype'):
        assert result['is_compliance'].dtype == bool


def test_process_gld_projects_empty_credits(raw_gld_projects):
    result = process_gld_projects(raw_gld_projects, credits=pd.DataFrame())
    project_schema.validate(result)
    assert result['project_id'].str.startswith('GLD').all()


def test_process_gld_projects_empty_projects():
    result = process_gld_projects(pd.DataFrame(), credits=pd.DataFrame())
    project_schema.validate(result)
    assert result.empty
