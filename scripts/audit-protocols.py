"""Audit mapping coverage for protocols and project types.

Outputs a markdown report to stdout. Use --output to also write it to a file.

Run with:
    pixi run audit-protocols                                      # local test data
    pixi run audit-protocols --data-dir s3://bucket/raw/2026-05-15  # S3 raw data
    pixi run audit-protocols --output docs/unmapped-protocols.md  # also write to file
"""

import argparse
import contextlib
import io
import json
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import TypedDict

import pandas as pd
import upath

from offsets_db_data.common import (
    BERKELEY_PROJECT_TYPE_UPATH,
    PROJECT_SCHEMA_UPATH,
    load_inverted_protocol_mapping,
    load_registry_project_column_mapping,
    load_type_category_mapping,
)
from offsets_db_data.projects import (
    infer_project_type,
    map_project_type_to_display_name,
    map_protocol,
)
from offsets_db_data.registry import GLD_PREFIX, VCS_PREFIX


class RegistrySource(TypedDict):
    name: str
    path: upath.UPath
    berkeley_prefix: str | None


class ProtocolEntry(TypedDict):
    protocol_id: str
    count: int
    registries: list[str]


class ProtocolGaps(TypedDict):
    n_projects: int
    string_registries: dict[str, dict[str, int]]


class ProjectTypeGaps(TypedDict):
    n_berkeley_resolved: int
    n_unknown: int
    no_protocol: dict[str, int]
    has_unassigned: dict[str, int]
    has_protocol: list[ProtocolEntry]


_DEFAULT_DATA_DIR = upath.UPath(Path(__file__).parent.parent / 'tests' / 'data')

# Prefix prepended to the raw project_id when looking up Berkeley data keys.
# Registries not listed here use the raw project_id directly.
_BERKELEY_ID_PREFIXES: dict[str, str] = {
    'gold-standard': GLD_PREFIX,
    'verra': VCS_PREFIX,
}


def _build_registry_sources(data_dir: upath.UPath) -> list[RegistrySource]:
    # Registry names are read from PROJECT_SCHEMA_UPATH (authoritative source).
    # Each spec pairs the registry name with its expected projects.csv path and
    # the optional Berkeley ID prefix used when looking up berkeley-project-types.json.
    names = json.loads(PROJECT_SCHEMA_UPATH.read_text())['project_id'].keys()
    return [
        RegistrySource(
            name=name,
            path=data_dir / name / 'projects.csv',
            berkeley_prefix=_BERKELEY_ID_PREFIXES.get(name),
        )
        for name in names
    ]


def _resolve_csv(csv_path: upath.UPath) -> upath.UPath | None:
    if csv_path.exists():
        return csv_path
    gz_path = csv_path.parent / (csv_path.name + '.gz')
    if gz_path.exists():
        return gz_path
    return None


def _load_mapped_df(
    registry_name: str, csv_path: upath.UPath, inverted_mapping: dict[str, list[str]]
) -> pd.DataFrame | None:
    resolved = _resolve_csv(csv_path)
    if resolved is None:
        sys.stderr.write(f'  {registry_name}: data file not found, skipping\n')
        return None

    col_mapping = load_registry_project_column_mapping(registry_name=registry_name)
    id_col = col_mapping.get('project_id')
    protocol_col = col_mapping.get('original_protocol')

    if not protocol_col:
        sys.stderr.write(f'  {registry_name}: no protocol column in mapping, skipping\n')
        return None

    raw_df = pd.read_csv(resolved, usecols=lambda c: c in {id_col, protocol_col})
    if protocol_col not in raw_df.columns:
        sys.stderr.write(f'  {registry_name}: column {protocol_col!r} absent from CSV, skipping\n')
        return None

    df = raw_df.rename(columns={id_col: 'project_id', protocol_col: 'original_protocol'})
    with contextlib.redirect_stdout(io.StringIO()):
        return map_protocol(df, inverted_protocol_mapping=inverted_mapping)


def _gather_protocol_gaps(
    registry_specs: list[RegistrySource],
    inverted_mapping: dict[str, list[str]],
) -> ProtocolGaps:
    string_registries: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    n_projects = 0
    for spec in registry_specs:
        df = _load_mapped_df(spec['name'], spec['path'], inverted_mapping)
        if df is None:
            continue
        for _, row in df[df['protocol_unassigned'].notna()].iterrows():
            strings = [
                s for s in row['protocol_unassigned'] if inverted_mapping.get(s) != ['unknown']
            ]
            if strings:
                n_projects += 1
                for s in strings:
                    string_registries[s][spec['name']] += 1
    return ProtocolGaps(n_projects=n_projects, string_registries=string_registries)


def _gather_project_type_gaps(
    registry_specs: list[RegistrySource],
    inverted_mapping: dict[str, list[str]],
) -> ProjectTypeGaps:
    type_mapping = load_type_category_mapping()
    try:
        berkeley_overrides = json.loads(BERKELEY_PROJECT_TYPE_UPATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        berkeley_overrides = {}

    rows: list[dict] = []
    for spec in registry_specs:
        df = _load_mapped_df(spec['name'], spec['path'], inverted_mapping)
        if df is None:
            continue
        with contextlib.redirect_stdout(io.StringIO()):
            df = infer_project_type(df)
        for _, row in df.iterrows():
            raw_id = str(row['project_id'])
            prefix = spec['berkeley_prefix']
            rows.append(
                {
                    'registry': spec['name'],
                    'project_id': raw_id,
                    'berkeley_id': f'{prefix}{raw_id}' if prefix else raw_id,
                    'protocol': row['protocol'],
                    'protocol_unassigned': row['protocol_unassigned'],
                    'project_type': row['project_type'],
                }
            )

    if not rows:
        return ProjectTypeGaps(
            n_berkeley_resolved=0, n_unknown=0, no_protocol={}, has_unassigned={}, has_protocol=[]
        )

    df_all = pd.DataFrame(rows)
    n_before = (df_all['project_type'] == 'unknown').sum()
    df_all['project_type'] = (
        df_all['berkeley_id'].map(berkeley_overrides).fillna(df_all['project_type'])
    )
    with contextlib.redirect_stdout(io.StringIO()):
        df_types = map_project_type_to_display_name(
            df_all[['project_id', 'project_type']].copy(), type_category_mapping=type_mapping
        )
    df_all['display_type'] = df_types['project_type']

    unknown = df_all[df_all['display_type'] == 'Unknown']
    n_after = len(unknown)
    no_protocol = unknown[unknown['protocol'].isna() & unknown['protocol_unassigned'].isna()]
    has_unassigned = unknown[unknown['protocol_unassigned'].notna()]
    has_protocol = unknown[unknown['protocol'].notna() & unknown['protocol_unassigned'].isna()]

    protocol_counts: Counter = Counter()
    protocol_registries: dict[str, set[str]] = defaultdict(set)
    for _, row in has_protocol.iterrows():
        for pid in row['protocol']:
            protocol_counts[pid] += 1
            protocol_registries[pid].add(row['registry'])

    return ProjectTypeGaps(
        n_berkeley_resolved=int(n_before - n_after),
        n_unknown=int(n_after),
        no_protocol=dict(no_protocol.groupby('registry')['project_id'].count()),
        has_unassigned=dict(has_unassigned.groupby('registry')['project_id'].count()),
        has_protocol=[
            ProtocolEntry(
                protocol_id=pid,
                count=count,
                registries=sorted(protocol_registries[pid]),
            )
            for pid, count in protocol_counts.most_common()
        ],
    )


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def build_report(protocol_gaps: ProtocolGaps, type_gaps: ProjectTypeGaps) -> str:
    """
    Render a markdown audit report from pre-computed gap data.

    Parameters
    ----------
    protocol_gaps : ProtocolGaps
        Count and per-registry breakdown of unrecognised protocol strings.
    type_gaps : ProjectTypeGaps
        Structured gap data for project type mapping.

    Returns
    -------
    str
        Markdown-formatted report ending with a trailing newline.
    """

    def _md_table(
        headers: list[str], rows: list[list[object]], right_align: set[int] | None = None
    ) -> str:
        right_align = right_align or set()
        sep = ['---:' if i in right_align else '---' for i in range(len(headers))]
        lines = [
            '| ' + ' | '.join(headers) + ' |',
            '| ' + ' | '.join(sep) + ' |',
        ]
        for row in rows:
            lines.append('| ' + ' | '.join(str(c) for c in row) + ' |')
        return '\n'.join(lines)

    out: list[str] = []

    out += [
        '<!-- Auto-generated by `pixi run audit-protocols`. Do not edit manually. -->',
        '',
        '# Unmapped Protocols',
        '',
        f'_Last updated: {date.today().isoformat()}_',
        '',
        '> **Protocol** is a normalized term representing the methodological framework governing projects'
        ' on registries. Other terms this may be referred to as include _methodology_, _protocol_, and _standard_.',
        '',
        '---',
        '',
    ]

    out += ['## Unmapped Protocols', '']
    if not protocol_gaps['string_registries']:
        out.append('✓ All protocol strings are mapped.')
    else:
        out.append(f'{protocol_gaps["n_projects"]} project(s) have unrecognised protocol strings.')
        out.append('')
        table_rows = [
            [f'`{s}`', sum(rc.values()), ', '.join(f'{r} ({c})' for r, c in sorted(rc.items()))]
            for s, rc in sorted(protocol_gaps['string_registries'].items())
        ]
        out.append(_md_table(['String', 'Projects', 'Registries'], table_rows, right_align={1}))

    out += ['', '---', '']

    n = type_gaps['n_unknown']
    n_unassigned = sum(type_gaps['has_unassigned'].values()) if type_gaps['has_unassigned'] else 0
    unassigned_note = (
        f' Resolving the protocol strings in the [Unmapped Protocols](#unmapped-protocols)'
        f' section above will also fix {n_unassigned} of these.'
        if n_unassigned
        else ''
    )
    out += [
        '## Unmapped Project Types',
        '',
        f'Berkeley overrides resolved {type_gaps["n_berkeley_resolved"]} project(s); {n} still unknown.{unassigned_note}',
        '',
    ]

    if n == 0:
        out.append('✓ All projects have a recognised project type.')
    else:
        if type_gaps['no_protocol']:
            total = sum(type_gaps['no_protocol'].values())
            out += [
                f'### No protocol in raw data ({total} projects)',
                '',
                'The raw registry data contains no protocol field for these projects, there is nothing to map.',
                '',
            ]
            out.append(
                _md_table(
                    ['Registry', 'Projects'],
                    [[r, c] for r, c in sorted(type_gaps['no_protocol'].items())],
                    right_align={1},
                )
            )
            out.append('')

        if type_gaps['has_protocol']:
            n_has_protocol = sum(e['count'] for e in type_gaps['has_protocol'])
            out += [
                f'### Protocols mapped with no project types ({n_has_protocol} projects)',
                '',
                'The protocol string is mapped to a known protocol ID, but `infer_project_type()` has no rule'
                ' for that protocol ID. Add a rule to `offsets_db_data/projects.py` to resolve them.',
                '',
            ]
            out.append(
                _md_table(
                    ['Protocol', 'Projects', 'Registries'],
                    [
                        [f'`{e["protocol_id"]}`', e['count'], ', '.join(e['registries'])]
                        for e in type_gaps['has_protocol']
                    ],
                    right_align={1},
                )
            )
            out.append('')

    return '\n'.join(out) + '\n'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Audit mapping coverage for protocols and project types.'
    )
    parser.add_argument(
        '--data-dir',
        type=upath.UPath,
        default=_DEFAULT_DATA_DIR,
        metavar='PATH',
        help=f'Path to registry data directory, local or S3 URI (default: {_DEFAULT_DATA_DIR})',
    )
    parser.add_argument(
        '--output',
        type=Path,
        default=None,
        metavar='PATH',
        help='Also write the report to this file',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir

    if not data_dir.exists():
        sys.stderr.write(f'ERROR: data directory not found: {data_dir}\n')
        if data_dir == upath.UPath(_DEFAULT_DATA_DIR):
            sys.stderr.write(
                'Run the test suite first to populate sample data, or pass --data-dir.\n'
            )
        sys.exit(2)

    inverted_mapping = load_inverted_protocol_mapping()
    specs = _build_registry_sources(data_dir)

    protocol_gaps = _gather_protocol_gaps(specs, inverted_mapping)
    type_gaps = _gather_project_type_gaps(specs, inverted_mapping)
    n_types = type_gaps['n_unknown']

    report = build_report(protocol_gaps, type_gaps)
    print(report, end='')

    if args.output:
        args.output.write_text(report)
        sys.stderr.write(f'Report written to {args.output}\n')

    total = protocol_gaps['n_projects'] + n_types
    if total > 0:
        sys.stderr.write(
            f'\n{total} gap(s) found: {protocol_gaps["n_projects"]} unmapped protocol string(s),'
            f' {n_types} unmapped project type(s).\n'
        )
        sys.exit(1)


if __name__ == '__main__':
    main()
