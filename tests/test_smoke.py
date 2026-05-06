"""
Smoke tests for all registry pipelines using local sample data.

Tests run without S3 access, validating that each registry's processing
functions produce output conforming to the expected schema.
Raw data fixtures come from conftest.py (tests/data/).
"""

import pandas as pd

from offsets_db_data.apx import *  # noqa: F403
from offsets_db_data.arb import *  # noqa: F403
from offsets_db_data.cercarbono import *  # noqa: F403
from offsets_db_data.gld import *  # noqa: F403
from offsets_db_data.isometric import *  # noqa: F403
from offsets_db_data.models import credit_without_id_schema, project_schema
from offsets_db_data.vcs import *  # noqa: F403


def test_vcs_pipeline(subtests, raw_vcs_projects, raw_vcs_transactions, arb):
    credits = raw_vcs_transactions.process_vcs_credits(
        arb=arb[arb.project_id.str.startswith('VCS')],
        harmonize_beneficiary_info=False,
    )

    with subtests.test('credits_schema'):
        credit_without_id_schema.validate(credits)
        assert set(credits.columns) == set(credit_without_id_schema.columns.keys())
        assert credits['project_id'].str.startswith('VCS').all()

    with subtests.test('credits_transaction_types'):
        types = set(credits['transaction_type'].unique())
        assert {'issuance', 'retirement'} <= types

    with subtests.test('projects_schema'):
        projects = raw_vcs_projects.process_vcs_projects(credits=credits)
        project_schema.validate(projects)
        assert projects['project_id'].str.startswith('VCS').all()


def test_apx_pipeline(
    subtests,
    arb,
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
            raw_acr_issuances,
            raw_acr_retirements,
            raw_acr_cancellations,
        ),
        (
            'art-trees',
            'ART',
            raw_art_projects,
            raw_art_issuances,
            raw_art_retirements,
            raw_art_cancellations,
        ),
        (
            'climate-action-reserve',
            'CAR',
            raw_car_projects,
            raw_car_issuances,
            raw_car_retirements,
            raw_car_cancellations,
        ),
    ]

    for (
        registry,
        prefix,
        raw_projects,
        raw_issuances,
        raw_retirements,
        raw_cancellations,
    ) in registries:
        with subtests.test(registry=registry):
            dfs = [
                raw_issuances.process_apx_credits(
                    download_type='issuances',
                    registry_name=registry,
                    harmonize_beneficiary_info=False,
                ),
                raw_retirements.process_apx_credits(
                    download_type='retirements',
                    registry_name=registry,
                    harmonize_beneficiary_info=False,
                ),
                raw_cancellations.process_apx_credits(
                    download_type='cancellations',
                    registry_name=registry,
                    harmonize_beneficiary_info=False,
                ),
            ]
            credits = pd.concat(dfs).merge_with_arb(arb=arb[arb.project_id.str.startswith(prefix)])
            credit_without_id_schema.validate(credits)
            assert set(credits.columns) == set(credit_without_id_schema.columns.keys())
            assert credits['project_id'].str.startswith(prefix).all()

            projects = raw_projects.process_apx_projects(credits=credits, registry_name=registry)
            project_schema.validate(projects)
            assert projects['project_id'].str.startswith(prefix).all()


def test_gld_pipeline(subtests, raw_gld_projects, raw_gld_issuances, raw_gld_retirements):
    credits = pd.concat(
        [
            raw_gld_issuances.process_gld_credits(
                download_type='issuances', harmonize_beneficiary_info=False
            ),
            raw_gld_retirements.process_gld_credits(
                download_type='retirements', harmonize_beneficiary_info=False
            ),
        ]
    )

    with subtests.test('credits_schema'):
        credit_without_id_schema.validate(credits)
        assert set(credits.columns) == set(credit_without_id_schema.columns.keys())
        assert credits['project_id'].str.startswith('GLD').all()

    with subtests.test('projects_schema'):
        projects = raw_gld_projects.process_gld_projects(credits=credits)
        project_schema.validate(projects)
        assert projects['project_id'].str.startswith('GLD').all()


def test_gld_empty_input(subtests):
    credits = pd.DataFrame().process_gld_credits(
        download_type='issuances', harmonize_beneficiary_info=False
    )
    with subtests.test('empty_credits_schema'):
        credit_without_id_schema.validate(credits)

    with subtests.test('empty_projects_schema'):
        projects = pd.DataFrame().process_gld_projects(credits=credits)
        project_schema.validate(projects)


def test_arb_pipeline(subtests, arb):
    with subtests.test('schema'):
        credit_without_id_schema.validate(arb)
        assert set(arb.columns) == set(credit_without_id_schema.columns.keys())

    with subtests.test('transaction_types'):
        types = set(arb['transaction_type'].unique())
        assert {'issuance', 'retirement'} <= types


def test_cercarbono_pipeline(
    subtests,
    raw_cercarbono_projects,
    raw_cercarbono_issuances,
    raw_cercarbono_retirements,
):
    credits = pd.concat(
        [
            raw_cercarbono_issuances.process_cercarbono_credits(
                download_type='issuances', harmonize_beneficiary_info=False
            ),
            raw_cercarbono_retirements.process_cercarbono_credits(
                download_type='retirements', harmonize_beneficiary_info=False
            ),
        ]
    )

    with subtests.test('credits_schema'):
        credit_without_id_schema.validate(credits)
        assert set(credits.columns) == set(credit_without_id_schema.columns.keys())
        assert credits['project_id'].str.startswith('CCB').all()

    with subtests.test('credits_transaction_types'):
        types = set(credits['transaction_type'].unique())
        assert {'issuance', 'retirement'} <= types

    with subtests.test('projects_schema'):
        projects = raw_cercarbono_projects.process_cercarbono_projects(credits=credits)
        project_schema.validate(projects)
        assert projects['project_id'].str.startswith('CCB').all()


def test_isometric_pipeline(
    subtests,
    raw_isometric_projects,
    raw_isometric_issuances,
    raw_isometric_retirements,
    isometric_prj_id_to_short_code,
):
    credits = pd.concat(
        [
            raw_isometric_issuances.process_isometric_credits(
                download_type='issuances',
                prj_id_to_short_code=isometric_prj_id_to_short_code,
                harmonize_beneficiary_info=False,
            ),
            raw_isometric_retirements.process_isometric_credits(
                download_type='retirements',
                prj_id_to_short_code=isometric_prj_id_to_short_code,
                harmonize_beneficiary_info=False,
            ),
        ]
    )

    with subtests.test('credits_schema'):
        credit_without_id_schema.validate(credits)
        assert set(credits.columns) == set(credit_without_id_schema.columns.keys())
        assert credits['project_id'].str.startswith('ISO').all()

    with subtests.test('credits_transaction_types'):
        types = set(credits['transaction_type'].unique())
        assert {'issuance', 'retirement'} <= types

    with subtests.test('projects_schema'):
        projects = raw_isometric_projects.process_isometric_projects(credits=credits)
        project_schema.validate(projects)
        assert projects['project_id'].str.startswith('ISO').all()
