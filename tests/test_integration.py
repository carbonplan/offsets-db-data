import pandas as pd
import pytest

from offsets_db_data.apx import *  # noqa: F403
from offsets_db_data.arb import *  # noqa: F403
from offsets_db_data.gld import *  # noqa: F403
from offsets_db_data.isometric import *  # noqa: F403
from offsets_db_data.models import credit_without_id_schema, project_schema
from offsets_db_data.vcs import *  # noqa: F403

# `date`, `bucket`, and `scratch_bucket` fixtures are provided by conftest.py
# `arb` fixture is provided by conftest.py (processes raw_arb from local sample)


@pytest.mark.parametrize(
    'harmonize_beneficiary_info',
    [True, False],
)
def test_verra(subtests, date, bucket, arb, harmonize_beneficiary_info):
    prefix = 'VCS'
    projects = pd.read_csv(f'{bucket}/{date}/verra/projects.csv.gz')
    credits = pd.read_csv(f'{bucket}/{date}/verra/transactions.csv.gz')
    df_credits = credits.process_vcs_credits(
        arb=arb[arb.project_id.str.startswith(prefix)],
        harmonize_beneficiary_info=harmonize_beneficiary_info,
    )
    df_projects = projects.process_vcs_projects(credits=df_credits)

    with subtests.test('credits_schema'):
        credit_without_id_schema.validate(df_credits)
    with subtests.test('credits_columns'):
        assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())
    with subtests.test('credits_project_id_prefix'):
        assert df_credits['project_id'].str.startswith(prefix).all()
    with subtests.test('projects_schema'):
        project_schema.validate(df_projects)
    with subtests.test('projects_project_id_prefix'):
        assert df_projects['project_id'].str.startswith(prefix).all()


@pytest.mark.parametrize(
    'registry, download_types, prefix',
    [
        ('art-trees', ['issuances', 'retirements', 'cancellations'], 'ART'),
        ('american-carbon-registry', ['issuances', 'retirements', 'cancellations'], 'ACR'),
        ('climate-action-reserve', ['issuances', 'retirements', 'cancellations'], 'CAR'),
    ],
)
def test_apx(subtests, date, bucket, arb, registry, download_types, prefix):
    dfs = []
    for key in download_types:
        credits = pd.read_csv(f'{bucket}/{date}/{registry}/{key}.csv.gz')
        p = credits.process_apx_credits(
            download_type=key, registry_name=registry, harmonize_beneficiary_info=True
        )
        dfs.append(p)

    df_credits = pd.concat(dfs).merge_with_arb(arb=arb[arb.project_id.str.startswith(prefix)])
    projects = pd.read_csv(f'{bucket}/{date}/{registry}/projects.csv.gz')
    df_projects = projects.process_apx_projects(credits=df_credits, registry_name=registry)

    with subtests.test('credits_schema'):
        credit_without_id_schema.validate(df_credits)
    with subtests.test('credits_columns'):
        assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())
    with subtests.test('credits_project_id_prefix'):
        assert df_credits['project_id'].str.startswith(prefix).all()
    with subtests.test('projects_schema'):
        project_schema.validate(df_projects)
    with subtests.test('projects_project_id_prefix'):
        assert df_projects['project_id'].str.startswith(prefix).all()


@pytest.mark.parametrize(
    'harmonize_beneficiary_info',
    [True, False],
)
def test_gld(subtests, date, bucket, harmonize_beneficiary_info):
    registry = 'gold-standard'
    prefix = 'GLD'

    dfs = []
    for key in ('issuances', 'retirements'):
        credits = pd.read_csv(f'{bucket}/{date}/{registry}/{key}.csv.gz')
        dfs.append(
            credits.process_gld_credits(
                download_type=key, harmonize_beneficiary_info=harmonize_beneficiary_info
            )
        )

    df_credits = pd.concat(dfs)
    projects = pd.read_csv(f'{bucket}/{date}/{registry}/projects.csv.gz')
    df_projects = projects.process_gld_projects(credits=df_credits)

    with subtests.test('credits_schema'):
        credit_without_id_schema.validate(df_credits)
    with subtests.test('credits_columns'):
        assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())
    with subtests.test('credits_project_id_prefix'):
        assert df_credits['project_id'].str.startswith(prefix).all()
    with subtests.test('projects_schema'):
        project_schema.validate(df_projects)
    with subtests.test('projects_project_id_prefix'):
        assert df_projects['project_id'].str.startswith(prefix).all()


@pytest.mark.parametrize(
    'df_credits',
    [
        pd.DataFrame().process_gld_credits(
            download_type='issuances', harmonize_beneficiary_info=True
        ),
    ],
)
@pytest.mark.parametrize(
    'projects',
    [pd.DataFrame()],
)
def test_gld_empty(subtests, df_credits, projects):
    prefix = 'GLD'

    df_projects = projects.process_gld_projects(credits=df_credits)

    with subtests.test('credits_schema'):
        credit_without_id_schema.validate(df_credits)
    with subtests.test('credits_columns'):
        assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())
    with subtests.test('credits_project_id_prefix'):
        assert df_credits['project_id'].str.startswith(prefix).all()
    with subtests.test('projects_schema'):
        project_schema.validate(df_projects)
    with subtests.test('projects_project_id_prefix'):
        assert df_projects['project_id'].str.startswith(prefix).all()


@pytest.mark.parametrize('harmonize_beneficiary_info', [True, False])
def test_isometric(
    subtests,
    scratch_date,
    scratch_bucket,
    isometric_prj_id_to_short_code,
    harmonize_beneficiary_info,
):
    registry = 'isometric'
    prefix = 'ISO'

    projects = pd.read_csv(f'{scratch_bucket}/{scratch_date}/{registry}/projects.csv.gz')

    dfs = []
    for key in ('issuances', 'retirements'):
        credits = pd.read_csv(f'{scratch_bucket}/{scratch_date}/{registry}/{key}.csv.gz')
        dfs.append(
            credits.process_isometric_credits(
                download_type=key,
                prj_id_to_short_code=isometric_prj_id_to_short_code,
                harmonize_beneficiary_info=harmonize_beneficiary_info,
            )
        )

    df_credits = pd.concat(dfs)
    df_projects = projects.process_isometric_projects(credits=df_credits)

    with subtests.test('credits_schema'):
        credit_without_id_schema.validate(df_credits)
    with subtests.test('credits_columns'):
        assert set(df_credits.columns) == set(credit_without_id_schema.columns.keys())
    with subtests.test('credits_project_id_prefix'):
        assert df_credits['project_id'].str.startswith(prefix).all()
    with subtests.test('projects_schema'):
        project_schema.validate(df_projects)
    with subtests.test('projects_project_id_prefix'):
        assert df_projects['project_id'].str.startswith(prefix).all()
