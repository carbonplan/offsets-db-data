from unittest.mock import patch

import numpy as np
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
def vcs_projects() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'ID': 75,
                'Name': '5.4 MW Grouped Wind Power Project in Gujarat & Maharashtra (India) by Rohan Builders (India) Pvt Ltd.',
                'Proponent': 'Rohan Builders (India)',
                'Project Type': 'Energy industries (renewable/non-renewable sources)',
                'AFOLU Activities': np.nan,
                'Methodology': 'AMS-I.D.',
                'Status': 'Registered',
                'Country/Area': 'India',
                'Estimated Annual Emission Reductions': '9,143',
                'Region': 'Asia',
                'Project Registration Date': '2009-06-15',
                'Crediting Period Start Date': np.nan,
                'Crediting Period End Date': np.nan,
            },
            {
                'ID': 2498,
                'Name': 'Afforestation of degraded grasslands in Caazapa and Guairá',
                'Proponent': 'Miller Forest Investment AG',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'AFOLU Activities': 'ARR',
                'Methodology': 'AR-ACM0003',
                'Status': 'Registered',
                'Country/Area': 'Paraguay',
                'Estimated Annual Emission Reductions': '204,819',
                'Region': 'Latin America',
                'Project Registration Date': '2022-01-14',
                'Crediting Period Start Date': '2016-01-13',
                'Crediting Period End Date': '2046-01-12',
            },
            {
                'ID': 101,
                'Name': 'Bagasse based Co-generation Power Project at Khatauli',
                'Proponent': 'Triveni Engineering and Industries Limited (TEIL)',
                'Project Type': 'Energy industries (renewable/non-renewable sources)',
                'AFOLU Activities': np.nan,
                'Methodology': 'ACM0006',
                'Status': 'Registered',
                'Country/Area': 'India',
                'Estimated Annual Emission Reductions': '86,808',
                'Region': 'Asia',
                'Project Registration Date': '2009-07-15',
                'Crediting Period Start Date': np.nan,
                'Crediting Period End Date': np.nan,
            },
            {
                'ID': 3408,
                'Name': 'Mianning1 Water Management with Rice Cultivation',
                'Proponent': 'Yunnan Ruihan Agricultural Technology Development Co., Ltd.',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'AFOLU Activities': 'ALM',
                'Methodology': 'AMS-III.AU',
                'Status': 'Under development',
                'Country/Area': 'China',
                'Estimated Annual Emission Reductions': '55,497',
                'Region': 'Asia',
                'Project Registration Date': np.nan,
                'Crediting Period Start Date': '2018-04-06',
                'Crediting Period End Date': '2025-04-05',
            },
            {
                'ID': 1223,
                'Name': 'Yanhe, Dejiang, and Yinjiang Rural Methane Digesters Project in Guizhou Province, China',
                'Proponent': 'Guizhou Black Carbon Energy Tech Prom & App Co. Lt',
                'Project Type': 'Energy industries (renewable/non-renewable sources)',
                'AFOLU Activities': np.nan,
                'Methodology': 'AMS-I.C.; AMS-III.R.',
                'Status': 'Under validation',
                'Country/Area': 'China',
                'Estimated Annual Emission Reductions': '53,247',
                'Region': 'Asia',
                'Project Registration Date': np.nan,
                'Crediting Period Start Date': np.nan,
                'Crediting Period End Date': np.nan,
            },
        ]
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


def test_add_vcs_compliance_projects(subtests, vcs_projects):
    original_len = len(vcs_projects)
    df = add_vcs_compliance_projects(vcs_projects)

    with subtests.test('two_rows_added'):
        assert len(df) == original_len + 2
    with subtests.test('compliance_ids_present'):
        assert {'VCSOPR2', 'VCSOPR10'}.issubset(df['project_id'].values)


def test_process_vcs_projects(subtests, vcs_projects, vcs_credits):
    df = process_vcs_projects(vcs_projects, credits=vcs_credits, registry_name='verra')

    with subtests.test('listed_at_column'):
        assert 'listed_at' in df.columns
    with subtests.test('project_urls'):
        assert df['project_url'].tolist() == [
            'https://registry.verra.org/app/projectDetail/VCS/75',
            'https://registry.verra.org/app/projectDetail/VCS/2498',
            'https://registry.verra.org/app/projectDetail/VCS/101',
            'https://registry.verra.org/app/projectDetail/VCS/3408',
            'https://registry.verra.org/app/projectDetail/VCS/1223',
            'https://registry.verra.org/app/projectDetail/VCS/2265',
            'https://registry.verra.org/app/projectDetail/VCS/2271',
        ]
    with subtests.test('project_ids'):
        assert df['project_id'].tolist() == [
            'VCS75',
            'VCS2498',
            'VCS101',
            'VCS3408',
            'VCS1223',
            'VCSOPR2',
            'VCSOPR10',
        ]


def test_process_vcs_projects_totals(subtests, vcs_projects, vcs_credits):
    df = process_vcs_projects(vcs_projects, credits=vcs_credits, registry_name='verra')
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
    # Mirror the pipeline: rename 'ID' -> 'project_id' before calling add_vcs_project_id
    df = raw_vcs_projects.rename(columns={'ID': 'project_id'}).copy()
    result = add_vcs_project_id(df)

    with subtests.test('prefix_added'):
        assert result['project_id'].str.startswith('VCS').all()
    with subtests.test('numeric_suffix'):
        assert result['project_id'].str[3:].str.isnumeric().all()
    with subtests.test('original_ids_preserved'):
        original = raw_vcs_projects['ID'].astype(str)
        assert (result['project_id'].str[3:] == original).all()


def test_add_vcs_project_url(subtests, raw_vcs_projects):
    # Mirror the pipeline: rename then prefix, then build URL
    df = raw_vcs_projects.rename(columns={'ID': 'project_id'}).copy()
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
