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
    validate_data,
    write_latest_production,
)


@pytest.fixture
def sample_credits():
    """Sample credits dataframe for testing."""
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
    """Sample projects dataframe for testing."""
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


@patch('offsets_db_data.pipeline_utils.catalog')
def test_validate_data_success(mock_catalog, sample_credits):
    """Test validate_data when data passes validation criteria."""
    # Mock old data with 90% of new data quantity
    mock_old_data = sample_credits.copy()
    mock_old_data['quantity'] = mock_old_data['quantity'] * 0.9

    mock_catalog.__getitem__.return_value = MagicMock()
    mock_catalog.__getitem__.return_value.read.return_value = mock_old_data

    # Should not raise exception
    validate_data(
        new_data=sample_credits,
        as_of=datetime(2023, 1, 1),
        data_type='credits',
        quantity_column='quantity',
        aggregation_func=sum,
    )

    # Verify catalog was called properly
    mock_catalog.__getitem__.assert_called_with('credits')


def test_summarize_single_registry(sample_credits, sample_projects, capsys):
    """Test summarize function with a single registry."""
    registry_name = 'verra'

    # Filter data for verra registry
    verra_projects = sample_projects[sample_projects['registry'] == registry_name]
    verra_credits = sample_credits[sample_credits['project_id'].str.startswith('VCS')]

    summarize(
        credits=verra_credits,
        projects=verra_projects,
        registry_name=registry_name,
    )

    captured = capsys.readouterr()

    assert f'Retired and Issued (in Millions) summary for {registry_name}' in captured.out
    assert f'Credits summary (in Millions) for {registry_name}' in captured.out


def test_summarize_multi_registry(sample_credits, sample_projects, capsys):
    """Test summarize function across multiple registries."""

    summarize(
        credits=sample_credits,
        projects=sample_projects,
    )

    captured = capsys.readouterr()

    assert 'Summary Statistics for projects (in Millions)' in captured.out
    assert 'Summary Statistics for credits (in Millions)' in captured.out


def test_create_data_zip_buffer_csv(sample_credits, sample_projects):
    """Test _create_data_zip_buffer with CSV format."""

    buffer = _create_data_zip_buffer(
        credits=sample_credits,
        projects=sample_projects,
        format_type='csv',
        terms_content='Test terms content',
    )

    # Test the buffer contains a valid ZIP
    with zipfile.ZipFile(buffer, 'r') as zip_ref:
        filenames = zip_ref.namelist()

        # Check expected files exist
        assert 'TERMS_OF_DATA_ACCESS.txt' in filenames
        assert 'credits.csv' in filenames
        assert 'projects.csv' in filenames

        # Check terms content
        assert zip_ref.read('TERMS_OF_DATA_ACCESS.txt').decode('utf-8') == 'Test terms content'


def test_create_data_zip_buffer_parquet(sample_credits, sample_projects):
    """Test _create_data_zip_buffer with Parquet format."""
    buffer = _create_data_zip_buffer(
        credits=sample_credits,
        projects=sample_projects,
        format_type='parquet',
        terms_content='Test terms content',
    )

    # Test the buffer contains a valid ZIP
    with zipfile.ZipFile(buffer, 'r') as zip_ref:
        filenames = zip_ref.namelist()

        assert 'TERMS_OF_DATA_ACCESS.txt' in filenames
        assert 'credits.parquet' in filenames
        assert 'projects.parquet' in filenames


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
    """Test write_latest_production function."""
    # Setup mocks
    mock_fs = MagicMock()
    mock_fs.read_text.return_value = 'Test terms content'
    mock_fsspec_fs.return_value = mock_fs

    # Create a new buffer for each call
    mock_create_buffer.side_effect = [
        io.BytesIO(b'test csv data'),
        io.BytesIO(b'test parquet data'),
    ]

    mock_file = MagicMock()
    mock_context = MagicMock()
    mock_context.__enter__.return_value = mock_file
    mock_fsspec_open.return_value = mock_context

    # Call function
    write_latest_production(
        credits=sample_credits,
        projects=sample_projects,
        bucket='s3://test-bucket',
    )

    # Assert mocks called correctly
    assert mock_create_buffer.call_count == 2  # Called for CSV and Parquet
    mock_fsspec_fs.assert_called_once_with('s3', anon=False)
    assert mock_fsspec_open.call_count == 2

    # Verify write calls
    assert mock_file.write.call_count == 2


@patch('offsets_db_data.pipeline_utils.to_parquet')
@patch('offsets_db_data.pipeline_utils.summarize')
def test_transform_registry_data(mock_summarize, mock_to_parquet, sample_credits, sample_projects):
    """Test transform_registry_data function."""
    # Setup mock functions
    process_credits_fn = MagicMock(return_value=sample_credits)
    process_projects_fn = MagicMock(return_value=sample_projects)
    output_paths = {'credits': 'path/to/credits', 'projects': 'path/to/projects'}

    # Call function
    result_credits, result_projects = transform_registry_data(
        process_credits_fn=process_credits_fn,
        process_projects_fn=process_projects_fn,
        output_paths=output_paths,
        registry_name='test-registry',
    )

    # Verify calls and returns
    process_credits_fn.assert_called_once()
    process_projects_fn.assert_called_once_with(credits=sample_credits)
    mock_summarize.assert_called_once_with(
        credits=sample_credits, projects=sample_projects, registry_name='test-registry'
    )
    mock_to_parquet.assert_called_once()

    # Verify return values
    assert result_credits.equals(sample_credits)
    assert result_projects.equals(sample_projects)


@patch('tempfile.NamedTemporaryFile')
def test_to_parquet(mock_temp_file, sample_credits, sample_projects):
    """Test to_parquet function."""
    # Setup mock
    mock_temp = MagicMock()
    mock_temp_file.return_value.__enter__.return_value = mock_temp

    # Setup output paths
    output_paths = {
        'credits': 'path/to/credits',
        'projects': 'path/to/projects',
    }

    # Patch pandas to_parquet to prevent actual file writing
    with patch.object(pd.DataFrame, 'to_parquet') as mock_to_parquet:
        to_parquet(
            credits=sample_credits,
            projects=sample_projects,
            output_paths=output_paths,
            registry_name='test-registry',
        )

        # Assert to_parquet called for all three dataframes
        assert mock_to_parquet.call_count == 2
