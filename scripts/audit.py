"""Audit mapping coverage for methodologies and project types.

Prints a report of raw strings in the data directory that are absent from the config
mapping files, and checks that every project type infer_project_type() can produce has
a display name in type-category-mapping.json.

Run with:
    pixi run audit                                      # local test data
    pixi run audit --data-dir s3://bucket/raw/2026-05-15  # S3 raw data

Exits non-zero if any gaps are found, so it can be used in CI.
"""

import argparse
import contextlib
import io
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import upath

from offsets_db_data.common import (
    BERKELEY_PROJECT_TYPE_UPATH,
    load_inverted_protocol_mapping,
    load_registry_project_column_mapping,
    load_type_category_mapping,
)
from offsets_db_data.projects import (
    infer_project_type,
    map_project_type_to_display_name,
    map_protocol,
)

_DEFAULT_DATA_DIR = upath.UPath(Path(__file__).parent.parent / 'tests' / 'data')

# (registry_name, directory_slug, berkeley_id_prefix)
# berkeley_id_prefix: string prepended to the raw project_id to match Berkeley data keys,
# or None if the raw ID already matches (ACR, CAR).
_REGISTRIES = [
    ('verra', 'verra', 'VCS'),
    ('american-carbon-registry', 'american-carbon-registry', None),
    ('gold-standard', 'gold-standard', 'GLD'),
    ('climate-action-reserve', 'climate-action-reserve', None),
    ('art-trees', 'art-trees', None),
    ('cercarbono', 'cercarbono', None),
    ('isometric', 'isometric', None),
]


def _registry_specs(data_dir: upath.UPath) -> list[tuple[str, upath.UPath, str | None]]:
    return [(name, data_dir / slug / 'projects.csv', prefix) for name, slug, prefix in _REGISTRIES]


def _section(title: str) -> None:
    print(f'\n{title}')
    print('─' * len(title))


def _resolve_csv(csv_path: upath.UPath) -> upath.UPath | None:
    """Return csv_path if it exists, falling back to .csv.gz, or None if neither exists."""
    if csv_path.exists():
        return csv_path
    gz_path = csv_path.parent / (csv_path.name + '.gz')
    if gz_path.exists():
        return gz_path
    return None


def _load_mapped_df(
    registry_name: str, csv_path: upath.UPath, inverted_mapping: dict
) -> pd.DataFrame | None:
    """Load a registry CSV (or .csv.gz) and run protocol mapping. Returns None if unavailable."""
    resolved = _resolve_csv(csv_path)
    if resolved is None:
        print(f'  {registry_name}: sample file not found, skipping')
        return None

    col_mapping = load_registry_project_column_mapping(registry_name=registry_name)
    id_col = col_mapping.get('project_id')
    proto_col = col_mapping.get('original_protocol')

    if not proto_col:
        print(f'  {registry_name}: no protocol column in mapping, skipping')
        return None

    raw_df = pd.read_csv(resolved, usecols=lambda c: c in {id_col, proto_col})
    if proto_col not in raw_df.columns:
        print(f'  {registry_name}: column {proto_col!r} absent from CSV, skipping')
        return None

    df = raw_df.rename(columns={id_col: 'project_id', proto_col: 'original_protocol'})
    with contextlib.redirect_stdout(io.StringIO()):
        return map_protocol(df, inverted_protocol_mapping=inverted_mapping)


def audit_methodologies(
    registry_specs: list[tuple[str, upath.UPath, str | None]], inverted_mapping: dict
) -> int:
    _section('Methodology mapping  (offsets_db_data/configs/all-protocol-mapping.json)')

    # string → {registry → count}
    string_registries: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    n_projects = 0

    for registry_name, csv_path, _prefix in registry_specs:
        df = _load_mapped_df(registry_name, csv_path, inverted_mapping)
        if df is None:
            continue

        unassigned = df[df['protocol_unassigned'].notna()]
        n_projects += len(unassigned)
        for _, row in unassigned.iterrows():
            for s in row['protocol_unassigned']:
                string_registries[s][registry_name] += 1

    if not string_registries:
        print('  ✓ All methodology strings are mapped')
    else:
        print(f'  {n_projects} project(s) have unrecognised methodology strings:\n')
        for s, reg_counts in sorted(string_registries.items()):
            total = sum(reg_counts.values())
            reg_summary = ', '.join(
                f'{reg} ({count})' for reg, count in sorted(reg_counts.items())
            )
            print(f'    {s!r}  —  {total} project(s)  [{reg_summary}]')

    return n_projects


def audit_project_types(
    registry_specs: list[tuple[str, upath.UPath, str | None]], inverted_mapping: dict
) -> int:
    _section('Project type mapping  (offsets_db_data/configs/type-category-mapping.json)')
    type_mapping = load_type_category_mapping()
    try:
        berkeley_overrides = json.loads(BERKELEY_PROJECT_TYPE_UPATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f'  WARNING: could not load Berkeley overrides ({e}), skipping')
        berkeley_overrides = {}

    rows: list[dict] = []
    for registry_name, csv_path, id_prefix in registry_specs:
        df = _load_mapped_df(registry_name, csv_path, inverted_mapping)
        if df is None:
            continue

        with contextlib.redirect_stdout(io.StringIO()):
            df = infer_project_type(df)

        for _, row in df.iterrows():
            raw_id = str(row['project_id'])
            berkeley_id = f'{id_prefix}{raw_id}' if id_prefix else raw_id
            rows.append(
                {
                    'registry': registry_name,
                    'project_id': raw_id,
                    'berkeley_id': berkeley_id,
                    'protocol': row['protocol'],
                    'protocol_unassigned': row['protocol_unassigned'],
                    'project_type': row['project_type'],
                }
            )

    if not rows:
        print('  ✓ All projects have a recognised project type')
        return 0

    df_all = pd.DataFrame(rows)

    n_before_berkeley = (df_all['project_type'] == 'unknown').sum()

    # Apply Berkeley overrides
    df_all['project_type'] = df_all.apply(
        lambda r: berkeley_overrides.get(r['berkeley_id'], r['project_type']), axis=1
    )

    # Apply display-name mapping
    with contextlib.redirect_stdout(io.StringIO()):
        df_types = map_project_type_to_display_name(
            df_all[['project_id', 'project_type']].copy(), type_category_mapping=type_mapping
        )
    df_all['display_type'] = df_types['project_type']

    unknown = df_all[df_all['display_type'] == 'Unknown']
    n_after_berkeley = len(unknown)

    n_berkeley_resolved = n_before_berkeley - n_after_berkeley
    print(
        f'  Berkeley overrides resolve {n_berkeley_resolved} project(s); '
        f'{n_after_berkeley} still unknown.'
    )

    if n_after_berkeley == 0:
        print('  ✓ All projects have a recognised project type')
        return 0

    # Mutually exclusive breakdown: prioritise unassigned strings over mapped-but-no-type-rule
    no_proto = unknown[unknown['protocol'].isna() & unknown['protocol_unassigned'].isna()]
    has_unassigned = unknown[unknown['protocol_unassigned'].notna()]
    has_proto = unknown[unknown['protocol'].notna() & unknown['protocol_unassigned'].isna()]

    print(f'\n  {n_after_berkeley} unresolved project(s) broken down by reason:\n')

    if not no_proto.empty:
        by_registry = no_proto.groupby('registry')['project_id'].count()
        print(f'  No methodology in raw data ({len(no_proto)} projects):')
        for reg, count in by_registry.items():
            print(f'    {count:3d}  [{reg}]')

    if not has_unassigned.empty:
        by_registry = has_unassigned.groupby('registry')['project_id'].count()
        print(
            f'\n  Methodology string not in all-protocol-mapping.json ({len(has_unassigned)} projects):'
        )
        for reg, count in by_registry.items():
            print(f'    {count:3d}  [{reg}]')

    if not has_proto.empty:
        proto_counts: Counter = Counter()
        proto_registries: dict = defaultdict(set)
        for _, row in has_proto.iterrows():
            for pid in row['protocol']:
                proto_counts[pid] += 1
                proto_registries[pid].add(row['registry'])
        print(
            f'\n  Protocol mapped but no type rule in infer_project_type() ({len(has_proto)} projects):'
        )
        print('  → add rules to offsets_db_data/projects.py')
        for proto_id, count in proto_counts.most_common():
            regs = ', '.join(sorted(proto_registries[proto_id]))
            print(f'    {count:3d}  {proto_id!r}  [{regs}]')

    return n_after_berkeley


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Audit mapping coverage for methodologies and project types.'
    )
    parser.add_argument(
        '--data-dir',
        type=upath.UPath,
        default=_DEFAULT_DATA_DIR,  # already a UPath
        metavar='PATH',
        help=f'Path to registry data directory, local or S3 URI (default: {_DEFAULT_DATA_DIR})',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir

    if not data_dir.exists():
        print(f'ERROR: data directory not found: {data_dir}')
        if data_dir == upath.UPath(_DEFAULT_DATA_DIR):
            print('Run the test suite first to populate sample data, or pass --data-dir.')
        sys.exit(2)

    print('Mapping Coverage Audit')
    print('=' * 22)
    print(f'Data: {data_dir}')

    inverted_mapping = load_inverted_protocol_mapping()
    specs = _registry_specs(data_dir)

    n_method = audit_methodologies(specs, inverted_mapping)
    n_types = audit_project_types(specs, inverted_mapping)

    total = n_method + n_types
    print()
    if total == 0:
        print('All mappings are complete.')
    else:
        print(
            f'{total} gap(s) found: {n_method} unmapped methodology string(s),'
            f' {n_types} unmapped project type(s).'
        )
        sys.exit(1)


if __name__ == '__main__':
    main()
