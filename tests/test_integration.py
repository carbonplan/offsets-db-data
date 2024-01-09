import pandas as pd
import pytest

from offsets_db_data.apx import *  # noqa: F403
from offsets_db_data.arb import *  # noqa: F403  # noqa: F403
from offsets_db_data.gcc import *  # noqa: F403
from offsets_db_data.gld import *  # noqa: F403
from offsets_db_data.models import credit_without_id_schema, project_schema
from offsets_db_data.vcs import *  # noqa: F403


@pytest.fixture
def date() -> str:
    return '2023-11-28'


@pytest.fixture
def bucket() -> str:
    return 's3://carbonplan-offsets-db/raw'


@pytest.fixture
def arb() -> pd.DataFrame:
    data = pd.read_excel(
        's3://carbonplan-offsets-db/raw/2023-11-28/arb/nc-arboc_issuance.xlsx', sheet_name=3
    )
    return data.process_arb()


def test_verra(date, bucket, arb):
    prefix = 'VCS'
    projects = pd.read_csv(f'{bucket}/{date}/verra/projects.csv.gz')
    credits = pd.read_csv(f'{bucket}/{date}/verra/transactions.csv.gz')
    df_credits = credits.process_vcs_credits(arb=arb[arb.project_id.str.startswith(prefix)])
    assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())
    df_projects = projects.process_vcs_projects(credits=df_credits)
    project_schema.validate(df_projects)
    credit_without_id_schema.validate(df_credits)

    assert df_projects['project_id'].str.startswith(prefix).all()
    assert df_credits['project_id'].str.startswith(prefix).all()


@pytest.mark.parametrize(
    'registry, download_types, prefix',
    [
        ('art-trees', ['issuances', 'retirements', 'cancellations'], 'ART'),
        ('american-carbon-registry', ['issuances', 'retirements', 'cancellations'], 'ACR'),
        ('climate-action-reserve', ['issuances', 'retirements', 'cancellations'], 'CAR'),
    ],
)
def test_apx(date, bucket, arb, registry, download_types, prefix):
    dfs = []
    for key in download_types:
        credits = pd.read_csv(f'{bucket}/{date}/{registry}/{key}.csv.gz')
        p = credits.process_apx_credits(download_type=key, registry_name=registry)
        dfs.append(p)

    df_credits = pd.concat(dfs).merge_with_arb(arb=arb[arb.project_id.str.startswith(prefix)])
    credit_without_id_schema.validate(df_credits)

    assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())

    projects = pd.read_csv(f'{bucket}/{date}/{registry}/projects.csv.gz')
    df_projects = projects.process_apx_projects(credits=df_credits, registry_name=registry)
    project_schema.validate(df_projects)

    assert df_projects['project_id'].str.startswith(prefix).all()
    assert df_credits['project_id'].str.startswith(prefix).all()


def test_gld(
    date,
    bucket,
):
    registry = 'gold-standard'
    download_types = ['issuances', 'retirements']
    prefix = 'GLD'

    dfs = []
    for key in download_types:
        credits = pd.read_csv(f'{bucket}/2023-11-27/{registry}/{key}.csv.gz')
        p = credits.process_gld_credits(download_type=key)
        dfs.append(p)

    df_credits = pd.concat(dfs)
    credit_without_id_schema.validate(df_credits)

    assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())

    projects = pd.read_csv(f'{bucket}/{date}/{registry}/projects.csv.gz')
    df_projects = projects.process_gld_projects(credits=df_credits)
    project_schema.validate(df_projects)

    # check if all project_id use the same prefix
    assert df_projects['project_id'].str.startswith(prefix).all()
    assert df_credits['project_id'].str.startswith(prefix).all()


@pytest.mark.parametrize(
    'df_credits',
    [
        pd.DataFrame().process_gld_credits(download_type='issuances'),
        pd.concat(
            [
                pd.read_csv(
                    's3://carbonplan-offsets-db/raw/2023-11-27/gold-standard/issuances.csv.gz'
                ).process_gld_credits(download_type='issuances'),
                pd.read_csv(
                    's3://carbonplan-offsets-db/raw/2023-11-27/gold-standard/retirements.csv.gz'
                ).process_gld_credits(download_type='retirements'),
            ]
        ),
    ],
)
@pytest.mark.parametrize(
    'projects',
    [
        pd.DataFrame(),
        pd.read_csv('s3://carbonplan-offsets-db/raw/2023-11-28/gold-standard/projects.csv.gz'),
    ],
)
def test_gld_empty(df_credits, projects):
    prefix = 'GLD'

    credit_without_id_schema.validate(df_credits)

    assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())

    df_projects = projects.process_gld_projects(credits=df_credits)
    project_schema.validate(df_projects)

    # check if all project_id use the same prefix
    assert df_projects['project_id'].str.startswith(prefix).all()
    assert df_credits['project_id'].str.startswith(prefix).all()


def test_gcc(
    date,
    bucket,
):
    registry = 'global-carbon-council'
    download_types = ['issuances', 'retirements']
    prefix = 'GCC'

    projects = pd.read_csv(f'{bucket}/{date}/{registry}/projects.csv.gz')

    dfs = []
    for key in download_types:
        credits = pd.read_csv(f'{bucket}/{date}/{registry}/{key}.csv.gz')
        p = credits.process_gcc_credits(download_type=key, raw_projects=projects)
        dfs.append(p)

    df_credits = pd.concat(dfs)
    credit_without_id_schema.validate(df_credits)

    assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())

    df_projects = projects.process_gcc_projects(credits=df_credits)
    project_schema.validate(df_projects)

    assert df_projects['project_id'].str.startswith(prefix).all()
    assert df_credits['project_id'].str.startswith(prefix).all()
