import numpy as np
import pandas as pd
import pytest

from offsets_db_data.vcs import (
    add_vcs_compliance_projects,
    calculate_vcs_issuances,
    calculate_vcs_retirements,
    determine_vcs_transaction_type,
    generate_vcs_project_ids,
    process_vcs_credits,
    process_vcs_projects,
    set_vcs_transaction_dates,
    set_vcs_vintage_year,
)


def vcs_projects() -> pd.DataFrame:
    df = pd.DataFrame(
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

    return df


@pytest.fixture(name='vcs_projects')
def fixture_vcs_projects() -> pd.DataFrame:
    return vcs_projects()


def vcs_transactions() -> pd.DataFrame:
    df = pd.DataFrame(
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
    return df


@pytest.fixture(name='vcs_transactions')
def fixture_vcs_transactions() -> pd.DataFrame:
    return vcs_transactions()


def test_determine_vcs_transaction_type(vcs_transactions):
    df = determine_vcs_transaction_type(
        vcs_transactions, date_column='Retirement/Cancellation Date'
    )

    # Check if the 'transaction_type' column is created
    assert 'transaction_type' in df.columns

    # Check that the function correctly assigns 'retirement/cancellation' or 'issuance'
    for i, row in df.iterrows():
        if pd.notnull(row['Retirement/Cancellation Date']):
            assert row['transaction_type'] == 'retirement'
        else:
            assert row['transaction_type'] == 'issuance'


def test_set_vcs_transaction_dates(vcs_transactions):
    df = set_vcs_transaction_dates(
        vcs_transactions,
        date_column='Retirement/Cancellation Date',
        fallback_column='Issuance Date',
    )

    # Check if the 'transaction_date' column is created
    assert 'transaction_date' in df.columns

    # Create a series for expected transaction_date values
    expected_transaction_date = vcs_transactions['Retirement/Cancellation Date'].where(
        vcs_transactions['Retirement/Cancellation Date'].notnull(),
        vcs_transactions['Issuance Date'],
    )

    expected_transaction_date.name = (
        'transaction_date'  # Set the name of the Series to match the DataFrame column
    )

    # Use assert_series_equal to compare the entire series
    pd.testing.assert_series_equal(df['transaction_date'], expected_transaction_date)


def test_set_vcs_vintage_year(vcs_transactions):
    df = set_vcs_vintage_year(vcs_transactions, date_column='Issuance Date')

    # Check if the 'vintage' column is created
    assert 'vintage' in df.columns

    # Convert 'Issuance Date' in the original DataFrame to datetime for comparison
    expected_vintage = pd.to_datetime(
        vcs_transactions['Issuance Date'], dayfirst=True, utc=True
    ).dt.year
    expected_vintage.name = 'vintage'  # Set the name of the Series to match the DataFrame column

    # Use assert_series_equal to compare the 'vintage' column with the expected result
    pd.testing.assert_series_equal(df['vintage'], expected_vintage)


def test_calculate_vcs_issuances(vcs_transactions):
    # Process the vcs_transactions similar to process_vcs_credits
    processed_data = (
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

    # Apply calculate_vcs_issuances
    issuances = calculate_vcs_issuances(processed_data)

    # Assertions
    # Ensure duplicates are removed based on the specified columns
    assert issuances.duplicated(subset=['vintage', 'project_id', 'quantity']).sum() == 0

    # Ensure the 'quantity' column is correctly populated
    assert 'quantity' in issuances.columns

    # Ensure 'transaction_type' is set to 'issuance'
    assert all(issuances['transaction_type'] == 'issuance')


def test_calculate_vcs_retirements(vcs_transactions):
    # Process the vcs_transactions similar to process_vcs_credits
    processed_data = (
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

    # Apply calculate_vcs_retirements
    retirements = calculate_vcs_retirements(processed_data)

    # Assertions
    # Check if 'retirement' and 'cancellation' types are present and 'issuance' types are filtered out
    assert all(retirements['transaction_type'].str.contains('retirement'))

    # Ensure the 'quantity' column is correctly renamed
    assert 'quantity' in retirements.columns
    assert 'Quantity Issued' not in retirements.columns


def test_generate_vcs_project_ids(vcs_projects):
    df = vcs_projects
    df = generate_vcs_project_ids(df, prefix='VCS')
    assert df['project_id'].tolist() == [
        'VCS75',
        'VCS2498',
        'VCS101',
        'VCS3408',
        'VCS1223',
    ]


def test_add_vcs_compliance_projects(vcs_projects):
    original_length = len(vcs_projects)
    df = add_vcs_compliance_projects(vcs_projects)

    # Check if two new rows are added
    assert len(df) == original_length + 2

    # Optionally, check for the presence of specific project details
    assert 'VCSOPR2' in df['project_id'].values
    assert 'VCSOPR10' in df['project_id'].values


def test_process_vcs_projects(vcs_projects, vcs_transactions):
    vcs_credits = process_vcs_credits(vcs_transactions)
    df = process_vcs_projects(
        vcs_projects, credits=vcs_credits, registry_name='verra', download_type='projects'
    )

    assert 'listed_at' in df.columns
    # check project_url series
    assert df['project_url'].tolist() == [
        'https://registry.verra.org/app/projectDetail/VCS/75',
        'https://registry.verra.org/app/projectDetail/VCS/2498',
        'https://registry.verra.org/app/projectDetail/VCS/101',
        'https://registry.verra.org/app/projectDetail/VCS/3408',
        'https://registry.verra.org/app/projectDetail/VCS/1223',
        'https://registry.verra.org/app/projectDetail/VCS/2265',  # From add_vcs_compliance_projects
        'https://registry.verra.org/app/projectDetail/VCS/2271',  # From add_vcs_compliance_projects
    ]
    # check project_id series
    assert df['project_id'].tolist() == [
        'VCS75',
        'VCS2498',
        'VCS101',
        'VCS3408',
        'VCS1223',
        'VCSOPR2',  # From add_vcs_compliance_projects
        'VCSOPR10',  # From add_vcs_compliance_projects
    ]


def test_process_vcs_projects_with_totals_and_dates(vcs_projects, vcs_transactions):
    # Process the vcs_transactions as per your existing pipeline
    # Assuming process_vcs_credits or similar functions are in place
    vcs_credits = process_vcs_credits(vcs_transactions)

    # Process the vcs_projects
    processed_projects = process_vcs_projects(
        vcs_projects, credits=vcs_credits, registry_name='verra', download_type='projects'
    )

    # Assertions for retired and issued totals, and first issuance/retirement dates
    # You need to know expected values for at least one project based on your test data
    project_id = 'VCS2498'

    # Extract the row for the specific project
    project_data = processed_projects[processed_projects['project_id'] == project_id]

    # Assert the total issued and retired quantities
    expected_total_issued = 435078  # Calculate this based on  vcs_transactions fixture
    expected_total_retired = 19549  # Calculate this based on  vcs_transactions fixture
    assert project_data['issued'].iloc[0] == expected_total_issued
    assert project_data['retired'].iloc[0] == expected_total_retired

    assert isinstance(project_data['first_issuance_at'].iloc[0], pd.Timestamp)
    assert isinstance(project_data['first_retirement_at'].iloc[0], pd.Timestamp)
