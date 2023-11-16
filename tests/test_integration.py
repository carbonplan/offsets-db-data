import pandas as pd
import pytest

from offsets_db_data.apx import *  # noqa: F403
from offsets_db_data.gcc import *  # noqa: F403
from offsets_db_data.gs import *  # noqa: F403
from offsets_db_data.models import credit_without_id_schema, project_schema
from offsets_db_data.verra import *  # noqa: F403


@pytest.fixture
def date() -> str:
    return '2023-11-10'


@pytest.fixture
def bucket() -> str:
    return 's3://carbonplan-offsets-db/raw'


def test_verra(date, bucket):
    projects = pd.read_csv(f'{bucket}/{date}/verra/projects.csv.gz')
    credits = pd.read_csv(f'{bucket}/{date}/verra/transactions.csv.gz')
    df_credits = credits.process_verra_credits()
    df_projects = projects.process_verra_projects(credits=df_credits)
    project_schema.validate(df_projects)
    credit_without_id_schema.validate(df_credits)


@pytest.mark.parametrize(
    'registry, download_types',
    [
        ('art-trees', ['issuances', 'retirements', 'cancellations']),
        ('american-carbon-registry', ['issuances', 'retirements', 'cancellations']),
        ('climate-action-reserve', ['issuances', 'retirements', 'cancellations']),
    ],
)
def test_apx(date, bucket, registry, download_types):
    dfs = []
    for key in download_types:
        credits = pd.read_csv(f'{bucket}/{date}/{registry}/{key}.csv.gz')
        p = credits.process_apx_credits(download_type=key, registry_name=registry)
        dfs.append(p)

    df_credits = pd.concat(dfs)
    credit_without_id_schema.validate(df_credits)

    projects = pd.read_csv(f'{bucket}/{date}/{registry}/projects.csv.gz')
    df_projects = projects.process_apx_projects(credits=df_credits, registry_name=registry)
    project_schema.validate(df_projects)


def test_gs(
    date,
    bucket,
):
    registry = 'gold-standard'
    download_types = ['issuances', 'retirements']

    dfs = []
    for key in download_types:
        credits = pd.read_csv(f'{bucket}/{date}/{registry}/{key}.csv.gz')
        p = credits.process_gs_credits(download_type=key)
        dfs.append(p)

    df_credits = pd.concat(dfs)
    credit_without_id_schema.validate(df_credits)

    projects = pd.read_csv(f'{bucket}/{date}/{registry}/projects.csv.gz')
    df_projects = projects.process_gs_projects(credits=df_credits)
    project_schema.validate(df_projects)


def test_gcc(
    date,
    bucket,
):
    registry = 'global-carbon-council'
    download_types = ['issuances', 'retirements']

    projects = pd.read_csv(f'{bucket}/{date}/{registry}/projects.csv.gz')

    dfs = []
    for key in download_types:
        credits = pd.read_csv(f'{bucket}/{date}/{registry}/{key}.csv.gz')
        p = credits.process_gcc_credits(download_type=key, raw_projects=projects)
        dfs.append(p)

    df_credits = pd.concat(dfs)
    credit_without_id_schema.validate(df_credits)

    df_projects = projects.process_gcc_projects(credits=df_credits)
    project_schema.validate(df_projects)
