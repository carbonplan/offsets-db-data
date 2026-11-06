"""Unit tests for common transformation utilities."""

import json

import pandas as pd
import pytest

from offsets_db_data.common import CREDIT_SCHEMA_UPATH, convert_to_datetime


def test_convert_to_datetime_missing_column():
    """Raises KeyError when the requested column is absent."""
    df = pd.DataFrame({'quantity': [100]})
    with pytest.raises(KeyError, match='missing_col'):
        convert_to_datetime(df, columns=['missing_col'])


def test_credit_column_mappings_all_map_the_same_fields(subtests):
    mappings = json.loads(CREDIT_SCHEMA_UPATH.read_text())
    fields = set(mappings['american-carbon-registry']['issuances'])
    for registry, entries in mappings.items():
        for download_type, mapping in entries.items():
            with subtests.test(case=f'{registry}/{download_type}'):
                assert set(mapping) == fields
