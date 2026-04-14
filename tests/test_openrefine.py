"""Unit tests for the openrefine CLI (offsets-db-data-orcli)."""

import subprocess
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from offsets_db_data.openrefine import app

runner = CliRunner()


# ── run command ───────────────────────────────────────────────────────────────


@patch('shutil.which', return_value=None)
def test_run_orcli_not_found(mock_which):
    """When orcli binary is not on PATH, command exits with code 1."""
    result = runner.invoke(app, ['run', 'import', 'csv', 'file.csv'])
    assert result.exit_code == 1


@patch('subprocess.run')
@patch('shutil.which', return_value='/usr/local/bin/orcli')
def test_run_orcli_success(mock_which, mock_run):
    """orcli binary found and subprocess succeeds."""
    mock_run.return_value = MagicMock(stdout='OK', returncode=0)
    result = runner.invoke(app, ['run', 'info', 'my-project'])
    assert result.exit_code == 0
    mock_run.assert_called_once()


@patch('subprocess.run')
@patch('shutil.which', return_value='/usr/local/bin/orcli')
def test_run_orcli_failure(mock_which, mock_run):
    """CalledProcessError with returncode=2 propagates exit_code == 2."""
    mock_run.side_effect = subprocess.CalledProcessError(
        returncode=2, cmd='orcli', stderr='some error'
    )
    result = runner.invoke(app, ['run', 'info', 'my-project'])
    assert result.exit_code == 2


@patch('subprocess.run')
@patch('shutil.which', return_value='/usr/local/bin/orcli')
def test_run_uses_explicit_binary_path(mock_which, mock_run):
    """--binary-path overrides shutil.which lookup."""
    mock_run.return_value = MagicMock(stdout='', returncode=0)
    result = runner.invoke(app, ['run', '--binary-path', '/custom/orcli', 'info', 'my-project'])
    assert result.exit_code == 0
    called_cmd = mock_run.call_args[0][0]
    assert called_cmd[0] == '/custom/orcli'


# ── install command ───────────────────────────────────────────────────────────


@patch('subprocess.run')
@patch('requests.get')
def test_install_success(mock_get, mock_run):
    """install downloads orcli, chmod +x, and moves it."""
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b'#!/bin/bash\necho orcli']
    mock_get.return_value = mock_response
    mock_run.return_value = MagicMock(returncode=0)

    result = runner.invoke(app, ['install', '--destination', '/tmp'])
    assert result.exit_code == 0
    assert mock_run.call_count == 2  # chmod + mv


@patch('requests.get', side_effect=Exception('network error'))
def test_install_failure(mock_get):
    """install exits with code 1 when the download fails."""
    result = runner.invoke(app, ['install'])
    assert result.exit_code == 1
