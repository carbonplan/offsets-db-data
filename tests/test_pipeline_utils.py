import io
import zipfile
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from offsets_db_data.pipeline_utils import (
    _create_data_zip_buffer,
    summarize,
    to_parquet,
    transform_registry_data,
    validate,
    validate_credits,
    validate_data,
    validate_projects,
    write_latest_production,
)

_VALIDATION_FAIL_MSG = 'Validation failed for either 1, 2, 3, or 4 days back'


@pytest.fixture
def sample_credits():
    return pd.DataFrame(
        {
            'project_id': ['VCS123', 'VCS124', 'ACR456', 'CAR789'],
            'quantity': [100, 200, 150, 300],
            'vintage': [2020, 2021, 2020, 2022],
            'transaction_date': pd.to_datetime(
                ['2021-01-01', '2022-02-01', '2021-03-15', '2022-04-30']
            ),
            'transaction_type': ['issuance', 'retirement', 'issuance', 'retirement'],
        }
    )


@pytest.fixture
def sample_projects():
    return pd.DataFrame(
        {
            'project_id': ['VCS123', 'VCS124', 'ACR456', 'CAR789'],
            'name': ['Project A', 'Project B', 'Project C', 'Project D'],
            'registry': ['verra', 'verra', 'american-carbon-registry', 'climate-action-reserve'],
            'is_compliance': [False, True, False, True],
            'retired': [50, 200, 75, 250],
            'issued': [100, 200, 150, 300],
            'type': ['forestry', 'renewable-energy', 'agriculture', 'forestry'],
            'type_source': ['carbonplan', 'berkeley', 'carbonplan', 'carbonplan'],
        }
    )


@patch('offsets_db_data.pipeline_utils.validate_data')
def test_validate_wrappers(mock_validate_data, sample_credits, sample_projects):
    """validate_credits, validate_projects, and validate all delegate to validate_data."""
    as_of = datetime(2023, 1, 1)

    validate_credits(new_credits=sample_credits, as_of=as_of)
    mock_validate_data.assert_called_once()

    mock_validate_data.reset_mock()
    validate_projects(new_projects=sample_projects, as_of=as_of)
    mock_validate_data.assert_called_once()

    mock_validate_data.reset_mock()
    validate(new_credits=sample_credits, new_projects=sample_projects, as_of=as_of)
    assert mock_validate_data.call_count == 2


@patch('offsets_db_data.pipeline_utils.catalog')
def test_validate_data_all_retries_fail(mock_catalog, sample_credits):
    """When all 4 retry attempts fail, validate_data raises ValueError."""
    mock_catalog.__getitem__.return_value = MagicMock(side_effect=Exception('S3 unavailable'))

    with pytest.raises(ValueError, match=_VALIDATION_FAIL_MSG):
        validate_data(
            new_data=sample_credits,
            as_of=datetime(2023, 1, 1),
            data_type='credits',
            quantity_column='quantity',
            aggregation_func=sum,
        )


@patch('offsets_db_data.pipeline_utils.catalog')
def test_validate_data_quantity_regression(mock_catalog, sample_credits):
    """When new quantity < 99% of old, the retry loop catches it and eventually raises."""
    old_data = sample_credits.copy()
    old_data['quantity'] = old_data['quantity'] * 1000  # old >> new → always fails
    mock_read = MagicMock(read=lambda: old_data)
    mock_catalog.__getitem__.return_value = MagicMock(return_value=mock_read)

    with pytest.raises(ValueError, match=_VALIDATION_FAIL_MSG):
        validate_data(
            new_data=sample_credits,
            as_of=datetime(2023, 1, 1),
            data_type='credits',
            quantity_column='quantity',
            aggregation_func=sum,
        )


@patch('offsets_db_data.pipeline_utils.catalog')
def test_validate_data_success(mock_catalog, sample_credits):
    mock_old = sample_credits.copy()
    mock_old['quantity'] = mock_old['quantity'] * 0.9
    mock_catalog.__getitem__.return_value = MagicMock()
    mock_catalog.__getitem__.return_value.read.return_value = mock_old

    validate_data(
        new_data=sample_credits,
        as_of=datetime(2023, 1, 1),
        data_type='credits',
        quantity_column='quantity',
        aggregation_func=sum,
    )

    mock_catalog.__getitem__.assert_called_with('credits')


def test_summarize(subtests, sample_credits, sample_projects, capsys):
    with subtests.test('single_registry'):
        registry = 'verra'
        verra_projects = sample_projects[sample_projects['registry'] == registry]
        verra_credits = sample_credits[sample_credits['project_id'].str.startswith('VCS')]
        summarize(credits=verra_credits, projects=verra_projects, registry_name=registry)
        captured = capsys.readouterr()
        assert f'Retired and Issued (in Millions) summary for {registry}' in captured.out
        assert f'Credits summary (in Millions) for {registry}' in captured.out

    with subtests.test('multi_registry'):
        summarize(credits=sample_credits, projects=sample_projects)
        captured = capsys.readouterr()
        assert 'Summary Statistics for projects (in Millions)' in captured.out
        assert 'Summary Statistics for credits (in Millions)' in captured.out

    with subtests.test('single_registry_empty_projects'):
        summarize(credits=sample_credits, projects=pd.DataFrame(), registry_name='verra')
        captured = capsys.readouterr()
        assert 'No projects found for verra' in captured.out

    with subtests.test('single_registry_empty_credits'):
        summarize(credits=pd.DataFrame(), projects=sample_projects, registry_name='verra')
        captured = capsys.readouterr()
        assert 'No credits found for verra' in captured.out

    with subtests.test('multi_registry_empty_projects'):
        summarize(credits=sample_credits, projects=pd.DataFrame())
        captured = capsys.readouterr()
        assert 'No projects found' in captured.out

    with subtests.test('multi_registry_empty_credits'):
        summarize(credits=pd.DataFrame(), projects=sample_projects)
        captured = capsys.readouterr()
        assert 'No credits found' in captured.out


def test_create_data_zip_buffer_unknown_format(sample_credits, sample_projects):
    """Unknown format_type: only terms + metadata files are written, no data files."""
    import json
    from datetime import timezone

    ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    buffer = _create_data_zip_buffer(
        credits=sample_credits,
        projects=sample_projects,
        format_type='json',
        terms_content='Test terms',
        generated_at=ts,
    )
    with zipfile.ZipFile(buffer, 'r') as zf:
        assert set(zf.namelist()) == {'TERMS_OF_DATA_ACCESS.txt', 'metadata.json'}
        meta = json.loads(zf.read('metadata.json'))
        assert meta['generated_at'] == ts.isoformat()


def test_create_data_zip_buffer(subtests, sample_credits, sample_projects):
    import json
    from datetime import timezone

    import pyarrow.parquet as pq

    ts = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    formats = {
        'csv': ['TERMS_OF_DATA_ACCESS.txt', 'metadata.json', 'credits.csv', 'projects.csv'],
        'parquet': [
            'TERMS_OF_DATA_ACCESS.txt',
            'metadata.json',
            'credits.parquet',
            'projects.parquet',
        ],
    }
    for fmt, expected_files in formats.items():
        with subtests.test(format=fmt):
            buffer = _create_data_zip_buffer(
                credits=sample_credits,
                projects=sample_projects,
                format_type=fmt,
                terms_content='Test terms content',
                generated_at=ts,
            )
            with zipfile.ZipFile(buffer, 'r') as zf:
                names = zf.namelist()
                for fname in expected_files:
                    assert fname in names
                assert zf.read('TERMS_OF_DATA_ACCESS.txt').decode() == 'Test terms content'
                meta = json.loads(zf.read('metadata.json'))
                assert meta['generated_at'] == ts.isoformat()
                if fmt == 'parquet':
                    for parquet_file in ('credits.parquet', 'projects.parquet'):
                        table = pq.read_table(io.BytesIO(zf.read(parquet_file)))
                        assert table.schema.metadata[b'generated_at'] == ts.isoformat().encode()


@patch('fsspec.filesystem')
@patch('fsspec.open')
@patch('offsets_db_data.pipeline_utils._create_data_zip_buffer')
def test_write_latest_production(
    mock_create_buffer,
    mock_fsspec_open,
    mock_fsspec_fs,
    sample_credits,
    sample_projects,
):
    mock_fs = MagicMock()
    mock_fs.read_text.return_value = 'Test terms content'
    mock_fsspec_fs.return_value = mock_fs
    mock_create_buffer.side_effect = [
        io.BytesIO(b'test csv data'),
        io.BytesIO(b'test parquet data'),
    ]
    mock_file = MagicMock()
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_file
    mock_fsspec_open.return_value = mock_context

    write_latest_production(
        credits=sample_credits,
        projects=sample_projects,
        bucket='s3://test-bucket',
    )

    assert mock_create_buffer.call_count == 2
    mock_fsspec_fs.assert_called_once_with('s3', anon=False)
    assert mock_fsspec_open.call_count == 2
    assert mock_file.write.call_count == 2


@patch('offsets_db_data.pipeline_utils.to_parquet')
@patch('offsets_db_data.pipeline_utils.summarize')
def test_transform_registry_data_no_registry_name(
    mock_summarize, mock_to_parquet, sample_credits, sample_projects, capsys
):
    process_credits_fn = MagicMock(return_value=sample_credits)
    process_projects_fn = MagicMock(return_value=sample_projects)
    output_paths = {'credits': 'path/to/credits', 'projects': 'path/to/projects'}

    transform_registry_data(
        process_credits_fn=process_credits_fn,
        process_projects_fn=process_projects_fn,
        output_paths=output_paths,
    )

    captured = capsys.readouterr()
    assert 'processed credits:' in captured.out
    assert 'processed projects:' in captured.out
    mock_summarize.assert_called_once_with(
        credits=sample_credits, projects=sample_projects, registry_name=None
    )


@patch('offsets_db_data.pipeline_utils.to_parquet')
@patch('offsets_db_data.pipeline_utils.summarize')
def test_transform_registry_data(mock_summarize, mock_to_parquet, sample_credits, sample_projects):
    process_credits_fn = MagicMock(return_value=sample_credits)
    process_projects_fn = MagicMock(return_value=sample_projects)
    output_paths = {'credits': 'path/to/credits', 'projects': 'path/to/projects'}

    result_credits, result_projects = transform_registry_data(
        process_credits_fn=process_credits_fn,
        process_projects_fn=process_projects_fn,
        output_paths=output_paths,
        registry_name='test-registry',
    )

    process_credits_fn.assert_called_once()
    process_projects_fn.assert_called_once_with(credits=sample_credits)
    mock_summarize.assert_called_once_with(
        credits=sample_credits, projects=sample_projects, registry_name='test-registry'
    )
    mock_to_parquet.assert_called_once()
    assert result_credits.equals(sample_credits)
    assert result_projects.equals(sample_projects)


@patch('tempfile.NamedTemporaryFile')
def test_to_parquet(mock_temp_file, sample_credits, sample_projects):
    mock_temp = MagicMock()
    mock_temp_file.return_value.__enter__.return_value = mock_temp
    output_paths = {'credits': 'path/to/credits', 'projects': 'path/to/projects'}

    with patch.object(pd.DataFrame, 'to_parquet') as mock_to_parquet:
        to_parquet(
            credits=sample_credits,
            projects=sample_projects,
            output_paths=output_paths,
            registry_name='test-registry',
        )
        assert mock_to_parquet.call_count == 2
