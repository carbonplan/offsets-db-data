"""Unit tests for projects transformation functions."""

import pandas as pd

from offsets_db_data.projects import get_protocol_category, map_protocol

# ── map_protocol ──────────────────────────────────────────────────────────────


def test_map_protocol_existing_column():
    """When the protocol column exists, values are mapped correctly."""
    df = pd.DataFrame({'original_protocol': ['afolu-redd', 'unknown-proto']})
    mapping = {'afolu-redd': 'afolu-redd', 'other': 'other'}
    result = map_protocol(df, inverted_protocol_mapping=mapping)
    assert 'protocol' in result.columns


def test_map_protocol_missing_column():
    """When the protocol column is absent, protocol is set to ['unknown'] for every row."""
    df = pd.DataFrame({'name': ['Project A', 'Project B']})
    result = map_protocol(
        df, inverted_protocol_mapping={}, original_protocol_column='nonexistent_col'
    )
    assert 'protocol' in result.columns
    assert all(v == ['unknown'] for v in result['protocol'])


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
