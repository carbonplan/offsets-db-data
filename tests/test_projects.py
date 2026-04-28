"""Unit tests for projects transformation functions."""

import pandas as pd

from offsets_db_data.projects import find_protocol, get_protocol_category, map_protocol

# ── find_protocol ──────────────────────────────────────────────────────────────


def test_find_protocol_mapped():
    """Known string returns (mapped_ids, None)."""
    mapping = {'afolu-redd': ['afolu-redd']}
    mapped, unmatched = find_protocol(search_string='afolu-redd', inverted_protocol_mapping=mapping)
    assert mapped == ['afolu-redd']
    assert unmatched is None


def test_find_protocol_unmapped():
    """Unknown string returns (None, [raw_string])."""
    mapped, unmatched = find_protocol(
        search_string='some-novel-method', inverted_protocol_mapping={}
    )
    assert mapped is None
    assert unmatched == ['some-novel-method']


def test_find_protocol_nan():
    """NaN input returns (None, None)."""
    mapped, unmatched = find_protocol(search_string=float('nan'), inverted_protocol_mapping={})
    assert mapped is None
    assert unmatched is None


def test_find_protocol_empty_string():
    """Empty / whitespace-only string returns (None, None)."""
    for val in ('', '   '):
        mapped, unmatched = find_protocol(search_string=val, inverted_protocol_mapping={})
        assert mapped is None
        assert unmatched is None


def test_find_protocol_null_sentinel():
    """Strings mapped to ['unknown'] yield (None, [raw_string]), not (['unknown'], None)."""
    mapping = {'Not defined': ['unknown'], 'Other': ['unknown']}
    for s in ('Not defined', 'Other'):
        mapped, unmatched = find_protocol(search_string=s, inverted_protocol_mapping=mapping)
        assert mapped is None
        assert unmatched == [s]


# ── map_protocol ──────────────────────────────────────────────────────────────


def test_map_protocol_mapped_value():
    """Recognised strings land in protocol; protocol_unassigned is None."""
    df = pd.DataFrame({'original_protocol': ['afolu-redd']})
    mapping = {'afolu-redd': ['afolu-redd']}
    result = map_protocol(df, inverted_protocol_mapping=mapping)
    assert result.loc[0, 'protocol'] == ['afolu-redd']
    assert result.loc[0, 'protocol_unassigned'] is None


def test_map_protocol_unmapped_value():
    """Unrecognised strings land in protocol_unassigned; protocol is None."""
    df = pd.DataFrame({'original_protocol': ['unknown-proto']})
    result = map_protocol(df, inverted_protocol_mapping={})
    assert result.loc[0, 'protocol'] is None
    assert result.loc[0, 'protocol_unassigned'] == ['unknown-proto']


def test_map_protocol_nan_value():
    """NaN input yields None in both columns."""
    df = pd.DataFrame({'original_protocol': [float('nan')]})
    result = map_protocol(df, inverted_protocol_mapping={})
    assert result.loc[0, 'protocol'] is None
    assert result.loc[0, 'protocol_unassigned'] is None


def test_map_protocol_missing_column():
    """When the protocol column is absent, both columns are None for every row."""
    df = pd.DataFrame({'name': ['Project A', 'Project B']})
    result = map_protocol(
        df, inverted_protocol_mapping={}, original_protocol_column='nonexistent_col'
    )
    assert 'protocol' in result.columns
    assert 'protocol_unassigned' in result.columns
    assert all(v is None for v in result['protocol'])
    assert all(v is None for v in result['protocol_unassigned'])


# ── get_protocol_category ─────────────────────────────────────────────────────


def test_get_protocol_category_string_input():
    """Single string input is wrapped in a list and processed."""
    mapping = {'afolu-redd': {'category': 'forestry'}}
    result = get_protocol_category(protocol_strs='afolu-redd', protocol_mapping=mapping)
    assert result == ['forestry']


def test_get_protocol_category_unknown_protocol():
    """Protocol not in mapping (None.get → AttributeError) returns 'unknown'."""
    mapping = {'known-proto': {'category': 'forestry'}}
    result = get_protocol_category(protocol_strs=['completely-unknown'], protocol_mapping=mapping)
    assert result == ['unknown']


def test_get_protocol_category_multiple_same_category():
    """Multiple protocols with the same category deduplicate to one entry."""
    mapping = {
        'proto-a': {'category': 'forestry'},
        'proto-b': {'category': 'forestry'},
    }
    result = get_protocol_category(protocol_strs=['proto-a', 'proto-b'], protocol_mapping=mapping)
    assert result == ['forestry']
