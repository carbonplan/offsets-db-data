from unittest.mock import patch

import pandas as pd
import pytest

from offsets_db_data.models import credit_without_id_schema
from offsets_db_data.vcs import (
    add_vcs_compliance_projects,
    add_vcs_project_id,
    add_vcs_project_url,
    process_vcs_credits,
    process_vcs_projects,
    set_vcs_vintage_year,
)


@pytest.fixture
def raw_vcs_reports(raw_vcs_issuances, raw_vcs_retirements, raw_vcs_cancellations) -> dict:
    return {
        'issuances': raw_vcs_issuances,
        'retirements': raw_vcs_retirements,
        'cancellations': raw_vcs_cancellations,
    }


@pytest.fixture
def vcs_credits(raw_vcs_reports) -> pd.DataFrame:
    return pd.concat(
        [process_vcs_credits(raw, download_type=key) for key, raw in raw_vcs_reports.items()],
        ignore_index=True,
    )


def test_process_vcs_credits_keeps_every_credit_of_each_report(subtests, raw_vcs_reports):
    transaction_types = {
        'issuances': 'issuance',
        'retirements': 'retirement',
        'cancellations': 'cancellation',
    }
    for download_type, raw in raw_vcs_reports.items():
        credits = process_vcs_credits(raw, download_type=download_type)
        with subtests.test(case=download_type):
            credit_without_id_schema.validate(credits)
            assert credits['quantity'].sum() == raw['holdingQuantity'].sum()
            assert (credits['transaction_type'] == transaction_types[download_type]).all()
            assert credits['project_id'].str.fullmatch(r'VCS\d+').all()
            dates = credits['transaction_date']
            assert (dates == dates.dt.normalize()).all(), 'a time of day is left'


def test_process_vcs_credits_sums_issuances_of_one_project_vintage_and_day(raw_vcs_issuances):
    credits = process_vcs_credits(raw_vcs_issuances, download_type='issuances')
    assert not credits.duplicated(['project_id', 'vintage', 'transaction_date']).any()
    assert len(credits) < len(raw_vcs_issuances), 'the sample has no issuances to sum'


def test_process_vcs_credits_takes_retirement_details_from_public_fields(
    subtests, raw_vcs_retirements
):
    credits = process_vcs_credits(raw_vcs_retirements, download_type='retirements')
    for column, raw_column in [
        ('retirement_beneficiary', 'publicBeneficialOwner'),
        ('retirement_reason', 'publicRetirementReason'),
        ('retirement_note', 'publicRetirementReasonDetails'),
    ]:
        with subtests.test(case=column):
            expected = raw_vcs_retirements[raw_column].fillna('').tolist()
            assert credits[column].fillna('').tolist() == expected


def test_set_vcs_vintage_year_keeps_the_last_year(subtests):
    for case, vintage, expected in [
        ('range', '2010 - 2011', 2011),
        ('year', '2015', 2015),
        ('integer', 2015, 2015),
        ('float', 2015.0, 2015),
    ]:
        with subtests.test(case=case):
            df = set_vcs_vintage_year(pd.DataFrame({'vintage': [vintage]}))
            assert df['vintage'].tolist() == [expected]
    with subtests.test(case='missing'):
        df = set_vcs_vintage_year(pd.DataFrame({'vintage': [None]}))
        assert df['vintage'].isna().all()


@patch('offsets_db_data.vcs.harmonize_beneficiary_data')
def test_process_vcs_credits_harmonizes_beneficiaries_when_asked(
    mock_harmonize, raw_vcs_retirements
):
    mock_harmonize.side_effect = lambda df, **_: df
    process_vcs_credits(
        raw_vcs_retirements, download_type='retirements', harmonize_beneficiary_info=True
    )
    mock_harmonize.assert_called_once()
    assert mock_harmonize.call_args.kwargs == {
        'registry_name': 'verra',
        'download_type': 'retirements',
    }


def test_add_vcs_compliance_projects(subtests, raw_vcs_projects):
    original_len = len(raw_vcs_projects)
    df = add_vcs_compliance_projects(raw_vcs_projects)

    with subtests.test('two_rows_added'):
        assert len(df) == original_len + 2
    with subtests.test('compliance_ids_present'):
        assert {'VCSOPR2', 'VCSOPR10'}.issubset(df['project_id'].values)


def test_process_vcs_projects(subtests, raw_vcs_projects, vcs_credits):
    df = process_vcs_projects(raw_vcs_projects, credits=vcs_credits, registry_name='verra')
    ids = raw_vcs_projects['vcsProjectId'].astype(str).tolist()
    from_report = df.iloc[: len(raw_vcs_projects)]

    with subtests.test('listed_at_column'):
        assert 'listed_at' in df.columns
    with subtests.test('project_ids'):
        assert df['project_id'].tolist() == [f'VCS{id_}' for id_ in ids] + ['VCSOPR2', 'VCSOPR10']
    with subtests.test('project_urls'):
        base = 'https://registry.verra.org/app/projectDetail/VCS/'
        assert df['project_url'].tolist() == [base + id_ for id_ in ids + ['2265', '2271']]
    with subtests.test('names_from_report'):
        assert from_report['name'].tolist() == raw_vcs_projects['projectName'].tolist()


def test_process_vcs_projects_totals(subtests, raw_vcs_projects, vcs_credits):
    df = process_vcs_projects(raw_vcs_projects, credits=vcs_credits, registry_name='verra')
    row = df[df['project_id'] == 'VCS2498'].iloc[0]

    with subtests.test('total_issued'):
        assert row['issued'] == 435078
    with subtests.test('total_retired'):
        assert row['retired'] == 186511
    with subtests.test('first_issuance_at_type'):
        assert isinstance(row['first_issuance_at'], pd.Timestamp)
    with subtests.test('first_retirement_at_type'):
        assert isinstance(row['first_retirement_at'], pd.Timestamp)


# ── add_vcs_project_id / add_vcs_project_url (real sample data) ───────────────


def test_add_vcs_project_id(subtests, raw_vcs_projects):
    # Mirror the pipeline: rename 'vcsProjectId' -> 'project_id' before calling add_vcs_project_id
    df = raw_vcs_projects.rename(columns={'vcsProjectId': 'project_id'}).copy()
    result = add_vcs_project_id(df)

    with subtests.test('prefix_added'):
        assert result['project_id'].str.startswith('VCS').all()
    with subtests.test('numeric_suffix'):
        assert result['project_id'].str[3:].str.isnumeric().all()
    with subtests.test('original_ids_preserved'):
        original = raw_vcs_projects['vcsProjectId'].astype(str)
        assert (result['project_id'].str[3:] == original).all()


def test_add_vcs_project_url(subtests, raw_vcs_projects):
    # Mirror the pipeline: rename then prefix, then build URL
    df = raw_vcs_projects.rename(columns={'vcsProjectId': 'project_id'}).copy()
    df = add_vcs_project_id(df)
    result = add_vcs_project_url(df)

    base = 'https://registry.verra.org/app/projectDetail/VCS/'
    with subtests.test('url_column_exists'):
        assert 'project_url' in result.columns
    with subtests.test('url_base'):
        assert result['project_url'].str.startswith(base).all()
    with subtests.test('url_suffix_matches_id'):
        suffix = result['project_url'].str.removeprefix(base)
        assert (suffix == result['project_id'].str[3:]).all()
