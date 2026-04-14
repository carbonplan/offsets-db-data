"""Unit tests for APX registry transformation functions.

Covers: american-carbon-registry (ACR), art-trees (ART),
        climate-action-reserve (CAR).
All tests use real sample data loaded from tests/data/ via conftest fixtures.
"""

import pandas as pd

from offsets_db_data.apx import (
    add_project_url,
    determine_transaction_type,
    harmonize_acr_status,
    process_apx_credits,
    process_apx_projects,
)
from offsets_db_data.models import credit_without_id_schema, project_schema

# ── determine_transaction_type ─────────────────────────────────────────────────

DOWNLOAD_TYPE_MAP = {
    'issuances': 'issuance',
    'retirements': 'retirement',
    'cancellations': 'cancellation',
}


def test_determine_transaction_type(
    subtests, raw_acr_issuances, raw_acr_retirements, raw_acr_cancellations
):
    cases = [
        (raw_acr_issuances, 'issuances'),
        (raw_acr_retirements, 'retirements'),
        (raw_acr_cancellations, 'cancellations'),
    ]
    for df, download_type in cases:
        with subtests.test(download_type=download_type):
            result = determine_transaction_type(df.copy(), download_type=download_type)
            assert 'transaction_type' in result.columns
            assert (result['transaction_type'] == DOWNLOAD_TYPE_MAP[download_type]).all()


# ── harmonize_acr_status ───────────────────────────────────────────────────────

# All ACR compliance-program-status values and their expected mapped output.
ACR_STATUS_CASES = [
    # Not ARB eligible → lowercase voluntary status
    ('Not ARB or Ecology Eligible', 'Active', 'active'),
    ('Not ARB or Ecology Eligible', 'Completed', 'completed'),
    # ARB-eligible mapped values
    ('Listed - Active ARB Project', 'Active', 'active'),
    ('Listed – Active ARB Project', 'Active', 'active'),  # en-dash variant
    ('Transferred ARB or Ecology Project', 'Active', 'active'),
    ('ARB Completed', 'Active', 'completed'),
    ('ARB Inactive', 'Active', 'completed'),
    ('ARB Terminated', 'Active', 'completed'),
    ('Listed - Proposed Project', 'Active', 'listed'),
    ('Listed - Active Registry Project', 'Active', 'listed'),
    ('Submitted', 'Active', 'listed'),
    # Unrecognised value falls back to 'unknown'
    ('Some Unknown Status', 'Active', 'unknown'),
]


def test_harmonize_acr_status_logic(subtests):
    for compliance_status, voluntary_status, expected in ACR_STATUS_CASES:
        row = pd.Series(
            {
                'Compliance Program Status (ARB or Ecology)': compliance_status,
                'Voluntary Status': voluntary_status,
            }
        )
        with subtests.test(compliance_status=compliance_status):
            assert harmonize_acr_status(row) == expected


def test_harmonize_acr_status_real_data(subtests, raw_acr_projects):
    results = raw_acr_projects.apply(harmonize_acr_status, axis=1)

    with subtests.test('returns_strings'):
        assert results.apply(lambda x: isinstance(x, str)).all()
    with subtests.test('no_nulls'):
        assert results.notna().all()


# ── add_project_url ────────────────────────────────────────────────────────────


def test_add_project_url(subtests, raw_acr_projects, raw_art_projects, raw_car_projects):
    cases = [
        (
            'american-carbon-registry',
            raw_acr_projects.rename(columns={'Project ID': 'project_id'}),
            'https://acr2.apx.com/mymodule/reg/prjView.asp?id1=',
        ),
        (
            'art-trees',
            raw_art_projects.rename(columns={'Program ID': 'project_id'}),
            'https://art.apx.com/mymodule/reg/prjView.asp?id1=',
        ),
        (
            'climate-action-reserve',
            raw_car_projects.rename(columns={'Project ID': 'project_id'}),
            'https://thereserve2.apx.com/mymodule/reg/prjView.asp?id1=',
        ),
    ]
    for registry, df, expected_base in cases:
        with subtests.test(registry=registry):
            result = add_project_url(df.copy(), registry_name=registry)
            assert 'project_url' in result.columns
            assert result['project_url'].str.startswith(expected_base).all()
            # Suffix is the numeric portion after the 3-char registry prefix
            suffix = result['project_url'].str.removeprefix(expected_base)
            assert suffix.str.isnumeric().all()


# ── process_apx_credits ────────────────────────────────────────────────────────


def test_process_apx_credits(
    subtests,
    raw_acr_issuances,
    raw_acr_retirements,
    raw_acr_cancellations,
    raw_art_issuances,
    raw_art_retirements,
    raw_art_cancellations,
    raw_car_issuances,
    raw_car_retirements,
    raw_car_cancellations,
):
    cases = [
        ('american-carbon-registry', 'ACR', 'issuances', raw_acr_issuances),
        ('american-carbon-registry', 'ACR', 'retirements', raw_acr_retirements),
        ('american-carbon-registry', 'ACR', 'cancellations', raw_acr_cancellations),
        ('art-trees', 'ART', 'issuances', raw_art_issuances),
        ('art-trees', 'ART', 'retirements', raw_art_retirements),
        ('art-trees', 'ART', 'cancellations', raw_art_cancellations),
        ('climate-action-reserve', 'CAR', 'issuances', raw_car_issuances),
        ('climate-action-reserve', 'CAR', 'retirements', raw_car_retirements),
        ('climate-action-reserve', 'CAR', 'cancellations', raw_car_cancellations),
    ]
    for registry, prefix, download_type, raw_df in cases:
        with subtests.test(registry=registry, download_type=download_type):
            result = process_apx_credits(
                raw_df,
                download_type=download_type,
                registry_name=registry,
                harmonize_beneficiary_info=False,
            )
            credit_without_id_schema.validate(result)
            assert result['project_id'].str.startswith(prefix).all()
            assert (result['transaction_type'] == DOWNLOAD_TYPE_MAP[download_type]).all()
            assert (result['quantity'] >= 0).all()
            assert pd.api.types.is_datetime64_any_dtype(result['transaction_date'])


# ── process_apx_projects ───────────────────────────────────────────────────────


def test_process_apx_projects(
    subtests,
    raw_acr_projects,
    raw_acr_issuances,
    raw_acr_retirements,
    raw_acr_cancellations,
    raw_art_projects,
    raw_art_issuances,
    raw_art_retirements,
    raw_art_cancellations,
    raw_car_projects,
    raw_car_issuances,
    raw_car_retirements,
    raw_car_cancellations,
):
    registries = [
        (
            'american-carbon-registry',
            'ACR',
            raw_acr_projects,
            [raw_acr_issuances, raw_acr_retirements, raw_acr_cancellations],
        ),
        (
            'art-trees',
            'ART',
            raw_art_projects,
            [raw_art_issuances, raw_art_retirements, raw_art_cancellations],
        ),
        (
            'climate-action-reserve',
            'CAR',
            raw_car_projects,
            [raw_car_issuances, raw_car_retirements, raw_car_cancellations],
        ),
    ]
    for registry, prefix, raw_projects, credit_dfs in registries:
        with subtests.test(registry=registry):
            download_types = ['issuances', 'retirements', 'cancellations']
            credits = pd.concat(
                [
                    process_apx_credits(
                        df,
                        download_type=dt,
                        registry_name=registry,
                        harmonize_beneficiary_info=False,
                    )
                    for df, dt in zip(credit_dfs, download_types)
                ]
            )
            result = process_apx_projects(raw_projects, credits=credits, registry_name=registry)
            project_schema.validate(result)
            assert result['project_id'].str.startswith(prefix).all()
            assert (result['registry'] == registry).all()
            assert result['project_url'].str.startswith('https://').all()
