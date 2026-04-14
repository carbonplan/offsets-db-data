"""
Smoke tests for all registry pipelines using local sample data.

These tests run without S3 access and validate that each registry's
processing functions produce output that conforms to the expected schema.
They use the raw_* fixtures defined in conftest.py which load from tests/data/.
"""

import pandas as pd
import pytest

from offsets_db_data.apx import *  # noqa: F403
from offsets_db_data.arb import *  # noqa: F403
from offsets_db_data.gld import *  # noqa: F403
from offsets_db_data.models import credit_without_id_schema, project_schema
from offsets_db_data.vcs import *  # noqa: F403

# ── VCS / Verra ────────────────────────────────────────────────────────────────


def test_vcs_credits_schema(raw_vcs_transactions, arb):
    df = raw_vcs_transactions.process_vcs_credits(
        arb=arb[arb.project_id.str.startswith('VCS')],
        harmonize_beneficiary_info=False,
    )
    credit_without_id_schema.validate(df)
    assert set(df.columns) == set(credit_without_id_schema.columns.keys())
    assert df['project_id'].str.startswith('VCS').all()


def test_vcs_projects_schema(raw_vcs_projects, raw_vcs_transactions, arb):
    credits = raw_vcs_transactions.process_vcs_credits(
        arb=arb[arb.project_id.str.startswith('VCS')],
        harmonize_beneficiary_info=False,
    )
    df = raw_vcs_projects.process_vcs_projects(credits=credits)
    project_schema.validate(df)
    assert df['project_id'].str.startswith('VCS').all()


def test_vcs_credits_has_both_transaction_types(raw_vcs_transactions, arb):
    df = raw_vcs_transactions.process_vcs_credits(
        arb=arb[arb.project_id.str.startswith('VCS')],
        harmonize_beneficiary_info=False,
    )
    types = df['transaction_type'].unique()
    assert 'issuance' in types
    assert 'retirement' in types


# ── APX registries (ACR / ART / CAR) ──────────────────────────────────────────


@pytest.mark.parametrize(
    'registry, prefix, raw_fixtures',
    [
        (
            'american-carbon-registry',
            'ACR',
            (
                'raw_acr_projects',
                'raw_acr_issuances',
                'raw_acr_retirements',
                'raw_acr_cancellations',
            ),
        ),
        (
            'art-trees',
            'ART',
            (
                'raw_art_projects',
                'raw_art_issuances',
                'raw_art_retirements',
                'raw_art_cancellations',
            ),
        ),
        (
            'climate-action-reserve',
            'CAR',
            (
                'raw_car_projects',
                'raw_car_issuances',
                'raw_car_retirements',
                'raw_car_cancellations',
            ),
        ),
    ],
)
def test_apx_credits_schema(registry, prefix, raw_fixtures, request, arb):
    projects_fix, issuances_fix, retirements_fix, cancellations_fix = raw_fixtures
    raw_projects = request.getfixturevalue(projects_fix)
    raw_issuances = request.getfixturevalue(issuances_fix)
    raw_retirements = request.getfixturevalue(retirements_fix)
    raw_cancellations = request.getfixturevalue(cancellations_fix)

    dfs = [
        raw_issuances.process_apx_credits(
            download_type='issuances', registry_name=registry, harmonize_beneficiary_info=False
        ),
        raw_retirements.process_apx_credits(
            download_type='retirements', registry_name=registry, harmonize_beneficiary_info=False
        ),
        raw_cancellations.process_apx_credits(
            download_type='cancellations', registry_name=registry, harmonize_beneficiary_info=False
        ),
    ]
    df_credits = pd.concat(dfs).merge_with_arb(arb=arb[arb.project_id.str.startswith(prefix)])
    credit_without_id_schema.validate(df_credits)
    assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())
    assert df_credits['project_id'].str.startswith(prefix).all()

    df_projects = raw_projects.process_apx_projects(credits=df_credits, registry_name=registry)
    project_schema.validate(df_projects)
    assert df_projects['project_id'].str.startswith(prefix).all()


# ── Gold Standard ──────────────────────────────────────────────────────────────


def test_gld_credits_schema(raw_gld_issuances, raw_gld_retirements):
    dfs = [
        raw_gld_issuances.process_gld_credits(
            download_type='issuances', harmonize_beneficiary_info=False
        ),
        raw_gld_retirements.process_gld_credits(
            download_type='retirements', harmonize_beneficiary_info=False
        ),
    ]
    df = pd.concat(dfs)
    credit_without_id_schema.validate(df)
    assert set(df.columns) == set(credit_without_id_schema.columns.keys())
    assert df['project_id'].str.startswith('GLD').all()


def test_gld_projects_schema(raw_gld_projects, raw_gld_issuances, raw_gld_retirements):
    dfs = [
        raw_gld_issuances.process_gld_credits(
            download_type='issuances', harmonize_beneficiary_info=False
        ),
        raw_gld_retirements.process_gld_credits(
            download_type='retirements', harmonize_beneficiary_info=False
        ),
    ]
    df_credits = pd.concat(dfs)
    df_projects = raw_gld_projects.process_gld_projects(credits=df_credits)
    project_schema.validate(df_projects)
    assert df_projects['project_id'].str.startswith('GLD').all()


def test_gld_empty_input():
    df_credits = pd.DataFrame().process_gld_credits(
        download_type='issuances', harmonize_beneficiary_info=False
    )
    credit_without_id_schema.validate(df_credits)

    df_projects = pd.DataFrame().process_gld_projects(credits=df_credits)
    project_schema.validate(df_projects)


# ── ARB ────────────────────────────────────────────────────────────────────────


def test_arb_schema(arb):
    credit_without_id_schema.validate(arb)
    assert set(arb.columns) == set(credit_without_id_schema.columns.keys())


def test_arb_has_both_transaction_types(arb):
    types = arb['transaction_type'].unique()
    assert 'issuance' in types
    assert 'retirement' in types
