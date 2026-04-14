import numpy as np
import pandas as pd
import pytest

from offsets_db_data.vcs import (
    add_vcs_compliance_projects,
    add_vcs_project_id,
    add_vcs_project_url,
    calculate_vcs_issuances,
    calculate_vcs_retirements,
    determine_vcs_transaction_type,
    generate_vcs_project_ids,
    process_vcs_credits,
    process_vcs_projects,
    set_vcs_transaction_dates,
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
def vcs_transactions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                'Issuance Date': '08/03/2022',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '01/01/2020',
                'Vintage End': '19/11/2020',
                'ID': 2498,
                'Name': 'Afforestation of degraded grasslands in Caazapa and Guairá',
                'Country/Area': 'Paraguay',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'Methodology': 'AR-ACM0003',
                'Total Vintage Quantity': '99,870',
                'Quantity Issued': '84,773',
                'Serial Number': '12629-421604735-421689507-VCS-VCU-576-VER-PY-14-2498-01012020-19112020-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': np.nan,
                'Retirement Beneficiary': np.nan,
                'Retirement Reason': np.nan,
                'Retirement Details': np.nan,
            },
            {
                'Issuance Date': '29/11/2022',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '01/01/2017',
                'Vintage End': '31/12/2017',
                'ID': 2498,
                'Name': 'Afforestation of degraded grasslands in Caazapa and Guairá',
                'Country/Area': 'Paraguay',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'Methodology': 'AR-ACM0003',
                'Total Vintage Quantity': '82,455',
                'Quantity Issued': '5,000',
                'Serial Number': '14121-556418249-556423248-VCS-VCU-576-VER-PY-14-2498-01012017-31122017-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': '26/12/2022',
                'Retirement Beneficiary': 'DNV AS',
                'Retirement Reason': 'Environmental Benefit',
                'Retirement Details': 'VCUs 2022 for DNV',
            },
            {
                'Issuance Date': '24/06/2022',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '13/01/2016',
                'Vintage End': '31/12/2016',
                'ID': 2498,
                'Name': 'Afforestation of degraded grasslands in Caazapa and Guairá',
                'Country/Area': 'Paraguay',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'Methodology': 'AR-ACM0003',
                'Total Vintage Quantity': '55,805',
                'Quantity Issued': '1,788',
                'Serial Number': '13378-495669005-495670792-VCS-VCU-576-VER-PY-14-2498-13012016-31122016-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': '11/09/2022',
                'Retirement Beneficiary': np.nan,
                'Retirement Reason': np.nan,
                'Retirement Details': np.nan,
            },
            {
                'Issuance Date': '27/07/2022',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '01/01/2020',
                'Vintage End': '19/11/2020',
                'ID': 2498,
                'Name': 'Afforestation of degraded grasslands in Caazapa and Guairá',
                'Country/Area': 'Paraguay',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'Methodology': 'AR-ACM0003',
                'Total Vintage Quantity': '99,870',
                'Quantity Issued': '725',
                'Serial Number': '13488-505972385-505973109-VCS-VCU-576-VER-PY-14-2498-01012020-19112020-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': '27/07/2022',
                'Retirement Beneficiary': 'Jebsen & Jessen (GmbH & Co.) KG',
                'Retirement Reason': 'Environmental Benefit',
                'Retirement Details': 'Retired on behalf of Jebsen & Jessen 2022',
            },
            {
                'Issuance Date': '11/09/2009',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '01/04/2006',
                'Vintage End': '18/03/2007',
                'ID': 101,
                'Name': 'Bagasse based Co-generation Power Project at Khatauli',
                'Country/Area': 'India',
                'Project Type': 'Energy industries (renewable/non-renewable sources)',
                'Methodology': 'ACM0006',
                'Total Vintage Quantity': '62,796',
                'Quantity Issued': '25,433',
                'Serial Number': '240-7863589-7889021-VCU-003-APX-IN-1-101-01042006-18032007-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': '17/06/2015',
                'Retirement Beneficiary': np.nan,
                'Retirement Reason': np.nan,
                'Retirement Details': np.nan,
            },
            {
                'Issuance Date': '04/11/2022',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '01/01/2019',
                'Vintage End': '31/12/2019',
                'ID': 2498,
                'Name': 'Afforestation of degraded grasslands in Caazapa and Guairá',
                'Country/Area': 'Paraguay',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'Methodology': 'AR-ACM0003',
                'Total Vintage Quantity': '99,871',
                'Quantity Issued': '1,413',
                'Serial Number': '13969-543072663-543074075-VCS-VCU-576-VER-PY-14-2498-01012019-31122019-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': '26/12/2022',
                'Retirement Beneficiary': 'DNV AS',
                'Retirement Reason': 'Environmental Benefit',
                'Retirement Details': 'VCUs 2022 for DNV',
            },
            {
                'Issuance Date': '27/07/2022',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '01/01/2020',
                'Vintage End': '19/11/2020',
                'ID': 2498,
                'Name': 'Afforestation of degraded grasslands in Caazapa and Guairá',
                'Country/Area': 'Paraguay',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'Methodology': 'AR-ACM0003',
                'Total Vintage Quantity': '99,870',
                'Quantity Issued': '297',
                'Serial Number': '13488-505982056-505982352-VCS-VCU-576-VER-PY-14-2498-01012020-19112020-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': '26/12/2022',
                'Retirement Beneficiary': 'DNV AS',
                'Retirement Reason': 'Environmental Benefit',
                'Retirement Details': 'VCUs 2022 for DNV',
            },
            {
                'Issuance Date': '27/07/2022',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '01/01/2018',
                'Vintage End': '31/12/2018',
                'ID': 2498,
                'Name': 'Afforestation of degraded grasslands in Caazapa and Guairá',
                'Country/Area': 'Paraguay',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'Methodology': 'AR-ACM0003',
                'Total Vintage Quantity': '97,077',
                'Quantity Issued': '1,380',
                'Serial Number': '13487-505962385-505963764-VCS-VCU-576-VER-PY-14-2498-01012018-31122018-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': '20/10/2022',
                'Retirement Beneficiary': 'Implement Consulting Group',
                'Retirement Reason': 'Environmental Benefit',
                'Retirement Details': 'Retirement of 1380t in the name of Implement Consulting Group, for flights 2021',
            },
            {
                'Issuance Date': '27/07/2022',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '01/01/2020',
                'Vintage End': '19/11/2020',
                'ID': 2498,
                'Name': 'Afforestation of degraded grasslands in Caazapa and Guairá',
                'Country/Area': 'Paraguay',
                'Project Type': 'Agriculture Forestry and Other Land Use',
                'Methodology': 'AR-ACM0003',
                'Total Vintage Quantity': '99,870',
                'Quantity Issued': '8,946',
                'Serial Number': '13488-505973110-505982055-VCS-VCU-576-VER-PY-14-2498-01012020-19112020-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': '01/12/2022',
                'Retirement Beneficiary': np.nan,
                'Retirement Reason': np.nan,
                'Retirement Details': np.nan,
            },
            {
                'Issuance Date': '11/09/2009',
                'Sustainable Development Goals': np.nan,
                'Vintage Start': '01/04/2006',
                'Vintage End': '18/03/2007',
                'ID': 101,
                'Name': 'Bagasse based Co-generation Power Project at Khatauli',
                'Country/Area': 'India',
                'Project Type': 'Energy industries (renewable/non-renewable sources)',
                'Methodology': 'ACM0006',
                'Total Vintage Quantity': '62,796',
                'Quantity Issued': '1,466',
                'Serial Number': '240-7889022-7890487-VCU-003-APX-IN-1-101-01042006-18032007-0',
                'Additional Certifications': np.nan,
                'Retirement/Cancellation Date': '18/06/2015',
                'Retirement Beneficiary': np.nan,
                'Retirement Reason': np.nan,
                'Retirement Details': np.nan,
            },
        ]
    )


@pytest.fixture
def processed_vcs_transactions(vcs_transactions) -> pd.DataFrame:
    """Intermediate processing state shared by issuance and retirement tests."""
    return (
        vcs_transactions.set_registry(registry_name='verra')
        .generate_vcs_project_ids(prefix='VCS')
        .determine_vcs_transaction_type(date_column='Retirement/Cancellation Date')
        .set_vcs_transaction_dates(
            date_column='Retirement/Cancellation Date', fallback_column='Issuance Date'
        )
        .clean_and_convert_numeric_columns(columns=['Total Vintage Quantity', 'Quantity Issued'])
        .set_vcs_vintage_year(date_column='Vintage End')
        .convert_to_datetime(columns=['transaction_date'], dayfirst=True)
    )


def test_determine_vcs_transaction_type(subtests, vcs_transactions):
    df = determine_vcs_transaction_type(
        vcs_transactions, date_column='Retirement/Cancellation Date'
    )
    assert 'transaction_type' in df.columns

    for i, row in df.iterrows():
        expected = 'retirement' if pd.notnull(row['Retirement/Cancellation Date']) else 'issuance'
        with subtests.test(row=i):
            assert row['transaction_type'] == expected


def test_set_vcs_transaction_dates(vcs_transactions):
    df = set_vcs_transaction_dates(
        vcs_transactions,
        date_column='Retirement/Cancellation Date',
        fallback_column='Issuance Date',
    )
    assert 'transaction_date' in df.columns
    expected = (
        vcs_transactions['Retirement/Cancellation Date']
        .where(
            vcs_transactions['Retirement/Cancellation Date'].notnull(),
            vcs_transactions['Issuance Date'],
        )
        .rename('transaction_date')
    )
    pd.testing.assert_series_equal(df['transaction_date'], expected)


def test_set_vcs_vintage_year(vcs_transactions):
    df = set_vcs_vintage_year(vcs_transactions, date_column='Issuance Date')
    assert 'vintage' in df.columns
    expected = pd.to_datetime(
        vcs_transactions['Issuance Date'], dayfirst=True, utc=True
    ).dt.year.rename('vintage')
    pd.testing.assert_series_equal(df['vintage'], expected)


def test_calculate_vcs_issuances(subtests, processed_vcs_transactions):
    issuances = calculate_vcs_issuances(processed_vcs_transactions)

    with subtests.test('no_duplicates'):
        assert issuances.duplicated(subset=['vintage', 'project_id', 'quantity']).sum() == 0
    with subtests.test('quantity_column'):
        assert 'quantity' in issuances.columns
    with subtests.test('all_issuances'):
        assert (issuances['transaction_type'] == 'issuance').all()


def test_calculate_vcs_retirements(subtests, processed_vcs_transactions):
    retirements = calculate_vcs_retirements(processed_vcs_transactions)

    with subtests.test('all_retirements'):
        assert retirements['transaction_type'].str.contains('retirement').all()
    with subtests.test('quantity_column'):
        assert 'quantity' in retirements.columns
    with subtests.test('raw_column_removed'):
        assert 'Quantity Issued' not in retirements.columns


def test_generate_vcs_project_ids(vcs_projects):
    df = generate_vcs_project_ids(vcs_projects, prefix='VCS')
    assert df['project_id'].tolist() == ['VCS75', 'VCS2498', 'VCS101', 'VCS3408', 'VCS1223']


def test_add_vcs_compliance_projects(subtests, vcs_projects):
    original_len = len(vcs_projects)
    df = add_vcs_compliance_projects(vcs_projects)

    with subtests.test('two_rows_added'):
        assert len(df) == original_len + 2
    with subtests.test('compliance_ids_present'):
        assert {'VCSOPR2', 'VCSOPR10'}.issubset(df['project_id'].values)


def test_process_vcs_projects(subtests, vcs_projects, vcs_transactions):
    credits = process_vcs_credits(vcs_transactions, harmonize_beneficiary_info=False)
    df = process_vcs_projects(
        vcs_projects, credits=credits, registry_name='verra', download_type='projects'
    )

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


def test_process_vcs_projects_totals(subtests, vcs_projects, vcs_transactions):
    credits = process_vcs_credits(vcs_transactions, harmonize_beneficiary_info=False)
    df = process_vcs_projects(
        vcs_projects, credits=credits, registry_name='verra', download_type='projects'
    )
    row = df[df['project_id'] == 'VCS2498'].iloc[0]

    with subtests.test('total_issued'):
        assert row['issued'] == 435078
    with subtests.test('total_retired'):
        assert row['retired'] == 19549
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
