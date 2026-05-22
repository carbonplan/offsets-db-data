"""Unit tests for scripts/audit-protocols.py."""

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest
import upath

# Import the audit script directly (it lives outside the package).
# Using importlib to avoid relying on sys.path ordering.
_AUDIT_PY = Path(__file__).parent.parent / 'scripts' / 'audit-protocols.py'
_spec = importlib.util.spec_from_file_location('audit', _AUDIT_PY)
_audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_audit)

_DATA_DIR = Path(__file__).parent / 'data'
_KNOWN_UNKNOWN_SENTINELS = (
    'Not defined',
    'Other',
    'Not provided',
    'Not Provided',
    'Methodology Under Development',
)

_EMPTY_PROTOCOL_GAPS = _audit.ProtocolGaps(n_projects=0, string_registries={})
_EMPTY_TYPE_GAPS = _audit.ProjectTypeGaps(
    n_berkeley_resolved=0,
    n_unknown=0,
    no_protocol={},
    has_unassigned={},
    has_protocol=[],
)


# ── build_report ──────────────────────────────────────────────────────────────


def test_build_report_starts_with_html_comment():
    """Report starts with the auto-generated HTML comment, followed by the H1."""
    report = _audit.build_report(_EMPTY_PROTOCOL_GAPS, _EMPTY_TYPE_GAPS)
    assert report.startswith('<!-- Auto-generated')
    assert '# Unmapped Protocols' in report


def test_build_report_no_gaps_shows_all_clear():
    """All-clear messages appear when there are no protocol or type gaps."""
    report = _audit.build_report(_EMPTY_PROTOCOL_GAPS, _EMPTY_TYPE_GAPS)
    assert '✓ All protocol strings are mapped.' in report
    assert '✓ All projects have a recognised project type.' in report


def test_build_report_protocol_gap_appears_in_table():
    """Unrecognised protocol strings are listed in the table with correct counts."""
    protocol_gaps = _audit.ProtocolGaps(n_projects=3, string_registries={'VM0999': {'verra': 3}})
    report = _audit.build_report(protocol_gaps, _EMPTY_TYPE_GAPS)
    assert '3 project(s)' in report
    assert '`VM0999`' in report
    assert '| 3 |' in report


def test_build_report_type_gap_sections():
    """Type gap subsections appear when no_protocol and has_protocol are populated."""
    type_gaps = _audit.ProjectTypeGaps(
        n_berkeley_resolved=10,
        n_unknown=5,
        no_protocol={'gold-standard': 5},
        has_unassigned={},
        has_protocol=[
            _audit.ProtocolEntry(
                protocol_id='ams-i-f', count=3, registries=['verra', 'gold-standard']
            )
        ],
    )
    report = _audit.build_report(_EMPTY_PROTOCOL_GAPS, type_gaps)
    assert '### No protocol in raw data' in report
    assert 'gold-standard' in report
    assert '### Protocols mapped with no project types' in report
    assert '`ams-i-f`' in report


def test_build_report_unassigned_note_cross_references_protocol_section():
    """Berkeley summary line links to the Unmapped Protocols section when has_unassigned."""
    type_gaps = _audit.ProjectTypeGaps(
        n_berkeley_resolved=0,
        n_unknown=5,
        no_protocol={},
        has_unassigned={'verra': 5},
        has_protocol=[],
    )
    report = _audit.build_report(_EMPTY_PROTOCOL_GAPS, type_gaps)
    assert '[Unmapped Protocols](#unmapped-protocols)' in report
    assert '5 of these' in report


def test_build_report_ends_with_newline():
    """Report string always ends with a trailing newline."""
    report = _audit.build_report(_EMPTY_PROTOCOL_GAPS, _EMPTY_TYPE_GAPS)
    assert report.endswith('\n')


# ── _BERKELEY_ID_PREFIXES ────────────────────────────────────────────────────


def test_berkeley_prefix_assigned_to_bare_id_registries():
    """Verra and gold-standard get a Berkeley prefix; other registries get None."""
    from offsets_db_data.registry import GLD_PREFIX, VCS_PREFIX

    sources = _audit._build_registry_sources(upath.UPath(_DATA_DIR))
    by_name = {s['name']: s['berkeley_prefix'] for s in sources}
    assert by_name.get('verra') == VCS_PREFIX
    assert by_name.get('gold-standard') == GLD_PREFIX
    assert by_name.get('american-carbon-registry') is None
    assert by_name.get('climate-action-reserve') is None


def test_berkeley_prefix_registries_have_bare_project_ids(registry_specs, inverted_mapping):
    """Raw project IDs for prefixed registries don't already start with the prefix.

    If a registry's raw CSV IDs already carry the prefix (e.g. 'ACR586'), adding it
    again in _BERKELEY_ID_PREFIXES would produce double-prefixed keys ('ACRACR586')
    that never match the Berkeley data, silently breaking project-type resolution.
    """
    for spec in registry_specs:
        if spec['berkeley_prefix'] is None:
            continue
        df = _audit._load_mapped_df(spec['name'], spec['path'], inverted_mapping)
        if df is None:
            continue
        prefix = spec['berkeley_prefix']
        assert not any(str(pid).startswith(prefix) for pid in df['project_id']), (
            f'{spec["name"]} raw IDs already start with {prefix!r} — '
            f'remove it from _BERKELEY_ID_PREFIXES'
        )


# ── _gather_protocol_gaps ─────────────────────────────────────────────────────


@pytest.fixture(scope='module')
def inverted_mapping():
    from offsets_db_data.common import load_inverted_protocol_mapping

    return load_inverted_protocol_mapping()


@pytest.fixture(scope='module')
def registry_specs():
    import upath

    return _audit._build_registry_sources(upath.UPath(_DATA_DIR))


@pytest.fixture(scope='module')
def protocol_gaps(registry_specs, inverted_mapping):
    return _audit._gather_protocol_gaps(registry_specs, inverted_mapping)


def test_protocol_gaps_finds_gaps(protocol_gaps):
    """At least one unrecognised protocol string exists in the test data."""
    assert protocol_gaps['n_projects'] > 0
    assert len(protocol_gaps['string_registries']) > 0


def test_protocol_gaps_filters_unknown_sentinels(protocol_gaps):
    """Known-unknown sentinel strings are excluded from the gap report."""
    for sentinel in _KNOWN_UNKNOWN_SENTINELS:
        assert sentinel not in protocol_gaps['string_registries'], (
            f'{sentinel!r} should be filtered by the unknown sentinel'
        )


def test_protocol_gaps_counts_are_positive(protocol_gaps):
    """Every registry count in the gap report is greater than zero."""
    for s, reg_counts in protocol_gaps['string_registries'].items():
        assert all(c > 0 for c in reg_counts.values()), f'Zero count for {s!r}'


# ── _gather_project_type_gaps ─────────────────────────────────────────────────


@pytest.fixture(scope='module')
def type_gaps(registry_specs, inverted_mapping):
    return _audit._gather_project_type_gaps(registry_specs, inverted_mapping)


def test_project_type_gaps_has_unknowns(type_gaps):
    """Some projects remain unresolved after Berkeley overrides are applied."""
    assert type_gaps['n_unknown'] > 0


def test_project_type_gaps_berkeley_resolved_some(type_gaps):
    """Berkeley overrides resolve at least one previously unknown project type."""
    assert type_gaps['n_berkeley_resolved'] > 0


def test_project_type_gaps_has_protocol_entries(type_gaps):
    """has_protocol contains valid ProtocolEntry dicts."""
    assert len(type_gaps['has_protocol']) > 0
    for entry in type_gaps['has_protocol']:
        assert isinstance(entry['protocol_id'], str)
        assert entry['count'] > 0
        assert isinstance(entry['registries'], list) and len(entry['registries']) > 0


def test_project_type_gaps_proto_counts_sorted_descending(type_gaps):
    """has_protocol entries are sorted from highest to lowest project count."""
    counts = [entry['count'] for entry in type_gaps['has_protocol']]
    assert counts == sorted(counts, reverse=True)


# ── CLI ───────────────────────────────────────────────────────────────────────


def test_cli_exits_nonzero_on_gaps():
    """Script exits with code 1 when gaps are found in the test data."""
    result = subprocess.run([sys.executable, str(_AUDIT_PY)], capture_output=True, text=True)
    assert result.returncode == 1


def test_cli_stdout_is_markdown():
    """Stdout is a valid markdown report with the expected top-level headings."""
    result = subprocess.run([sys.executable, str(_AUDIT_PY)], capture_output=True, text=True)
    assert result.stdout.startswith('<!-- Auto-generated')
    assert '# Unmapped Protocols' in result.stdout
    assert '## Unmapped Protocols' in result.stdout
    assert '## Unmapped Project Types' in result.stdout


def test_cli_output_flag_writes_file(tmp_path):
    """--output writes the report to the specified file."""
    out_file = tmp_path / 'report.md'
    subprocess.run(
        [sys.executable, str(_AUDIT_PY), '--output', str(out_file)],
        capture_output=True,
        text=True,
    )
    assert out_file.exists()
    assert '# Unmapped Protocols' in out_file.read_text()


def test_cli_stdout_matches_file(tmp_path):
    """stdout and the --output file contain identical content."""
    out_file = tmp_path / 'report.md'
    result = subprocess.run(
        [sys.executable, str(_AUDIT_PY), '--output', str(out_file)],
        capture_output=True,
        text=True,
    )
    assert result.stdout == out_file.read_text()


def test_cli_invalid_data_dir_exits_with_code_2(tmp_path):
    """Passing a nonexistent --data-dir exits with code 2."""
    result = subprocess.run(
        [sys.executable, str(_AUDIT_PY), '--data-dir', str(tmp_path / 'nonexistent')],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2
