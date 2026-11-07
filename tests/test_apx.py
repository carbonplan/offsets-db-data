"""Unit tests for APX registry transformation functions.

Covers: american-carbon-registry (ACR), art-trees (ART),
        climate-action-reserve (CAR).
All tests use real sample data loaded from tests/data/ via conftest fixtures.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from offsets_db_data.apx import (
    add_project_url,
    determine_transaction_type,
    harmonize_acr_status,
    process_apx_credits,
    process_apx_projects,
    strip_project_id_padding,
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


def test_add_project_url_invalid_registry(raw_acr_projects):
    df = raw_acr_projects.rename(columns={'Project ID': 'project_id'})
    with pytest.raises(ValueError, match='Unknown registry name'):
        add_project_url(df, registry_name='unknown-registry')


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


GREENTRACE_RETIREMENT_COLUMNS = {
    'retirement_account': 'Credit Holder',
    'retirement_beneficiary': 'Retired On Behalf Of',
    'retirement_note': 'Purpose of Retirement',
    'retirement_reason': 'Retirement Reason',
}


def test_greentrace_retirement_columns_fill_offsetsdb_columns(
    subtests, raw_acr_retirements, raw_art_retirements
):
    for registry, raw in [
        ('american-carbon-registry', raw_acr_retirements),
        ('art-trees', raw_art_retirements),
    ]:
        with subtests.test(registry=registry):
            result = process_apx_credits(raw, download_type='retirements', registry_name=registry)
            expected = raw[list(GREENTRACE_RETIREMENT_COLUMNS.values())].set_axis(
                list(GREENTRACE_RETIREMENT_COLUMNS), axis=1
            )
            pd.testing.assert_frame_equal(
                result[list(GREENTRACE_RETIREMENT_COLUMNS)].reset_index(drop=True),
                expected.reset_index(drop=True),
                check_dtype=False,
            )


def test_greentrace_issuances_count_the_buffer_pool_as_the_apx_pipeline_did(
    subtests, raw_acr_issuances, raw_art_issuances
):
    for registry, raw, quantity in [
        ('american-carbon-registry', raw_acr_issuances, 'Total Credits Issued'),
        ('art-trees', raw_art_issuances, 'Total Credits Issued to Project Developer'),
    ]:
        with subtests.test(registry=registry):
            result = process_apx_credits(raw, download_type='issuances', registry_name=registry)
            assert result['quantity'].sum() == raw[quantity].sum(), (
                f'{registry} issuances should be counted from {quantity!r}'
            )


def test_strip_project_id_padding_removes_only_leading_zeros(subtests):
    cases = {
        'three_digits': ('ACR0731', 'ACR731'),
        'two_digits': ('ACR0049', 'ACR49'),
        'art': ('ART0102', 'ART102'),
        'four_digits_unpadded': ('ACR1129', 'ACR1129'),
        'zeros_inside_the_number': ('ACR1001', 'ACR1001'),
        'car_unpadded': ('CAR1234', 'CAR1234'),
    }
    raw_ids = [raw for raw, _ in cases.values()]
    result = strip_project_id_padding(pd.DataFrame({'project_id': raw_ids}))
    for (case, (_, expected)), actual in zip(cases.items(), result['project_id']):
        with subtests.test(case=case):
            assert actual == expected


def test_greentrace_timestamps_keep_their_utc_day(subtests, raw_acr_retirements):
    cases = {
        'midnight': ('2026-04-08T00:00:00Z', '2026-04-08'),
        'early_morning_utc': ('2026-04-08T02:00:00Z', '2026-04-08'),
        'late_evening_utc': ('2026-04-13T23:59:59Z', '2026-04-13'),
        'new_years_eve': ('2025-12-31T23:30:00Z', '2025-12-31'),
    }
    raw = raw_acr_retirements.head(len(cases)).assign(
        **{'Status Effective (GMT)': [timestamp for timestamp, _ in cases.values()]}
    )
    result = process_apx_credits(
        raw, download_type='retirements', registry_name='american-carbon-registry'
    )
    days = result['transaction_date'].dt.strftime('%Y-%m-%d')
    for (case, (_, expected)), actual in zip(cases.items(), days):
        with subtests.test(case=case):
            assert actual == expected


def test_a_repeated_row_counts_once_only_when_it_has_serial_numbers(
    subtests, raw_acr_cancellations, raw_car_issuances
):
    cases = {
        'greentrace_cancellation': (
            'american-carbon-registry',
            'cancellations',
            raw_acr_cancellations,
            'Quantity of Credits',
            1,
        ),
        'apx_issuance_without_serial_numbers': (
            'climate-action-reserve',
            'issuances',
            raw_car_issuances,
            'Total Offset Credits Issued',
            2,
        ),
    }
    for case, (registry, download_type, raw, quantity, times_counted) in cases.items():
        with subtests.test(case=case):
            repeated = pd.concat([raw, raw.head(1)], ignore_index=True)
            result = process_apx_credits(
                repeated, download_type=download_type, registry_name=registry
            )
            expected = raw[quantity].sum() + (times_counted - 1) * raw[quantity].iloc[0]
            assert result['quantity'].sum() == expected


def test_greentrace_art_files_give_the_apx_project_ids(
    raw_art_projects, raw_art_issuances, raw_art_retirements, raw_art_cancellations
):
    apx_ids = set(raw_art_projects['Program ID'])
    credits = pd.concat(
        process_apx_credits(raw, download_type=download_type, registry_name='art-trees')
        for raw, download_type in [
            (raw_art_issuances, 'issuances'),
            (raw_art_retirements, 'retirements'),
            (raw_art_cancellations, 'cancellations'),
        ]
    )
    padded_projects = raw_art_projects.assign(
        **{'Program ID': raw_art_projects['Program ID'].str.replace('ART', 'ART0')}
    )
    projects = process_apx_projects(padded_projects, credits=credits, registry_name='art-trees')

    assert set(credits['project_id']) <= apx_ids
    assert set(projects['project_id']) == apx_ids
    assert not projects['project_url'].str.contains('id1=0').any()


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


@patch('offsets_db_data.apx.harmonize_beneficiary_data')
def test_process_apx_credits_harmonize_beneficiary(mock_harmonize, raw_acr_retirements):
    """harmonize_beneficiary_info=True invokes harmonize_beneficiary_data."""
    mock_harmonize.side_effect = lambda df, **_: df
    process_apx_credits(
        raw_acr_retirements,
        download_type='retirements',
        registry_name='american-carbon-registry',
        harmonize_beneficiary_info=True,
    )
    mock_harmonize.assert_called_once()


def test_process_apx_credits_with_arb(subtests, raw_acr_issuances, arb):
    """process_apx_credits with non-empty arb triggers the merge_with_arb branch."""
    acr_arb = arb[arb['project_id'].str.startswith('ACR')]
    result = process_apx_credits(
        raw_acr_issuances,
        download_type='issuances',
        registry_name='american-carbon-registry',
        harmonize_beneficiary_info=False,
        arb=acr_arb,
    )
    credit_without_id_schema.validate(result)
    with subtests.test('schema_valid'):
        assert set(result.columns) == set(credit_without_id_schema.columns.keys())
    with subtests.test('non_negative_quantities'):
        assert (result['quantity'] >= 0).all()
