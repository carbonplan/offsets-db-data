"""Unit tests for common transformation utilities."""

import pandas as pd
import pytest

from offsets_db_data.common import convert_to_datetime


def test_convert_to_datetime_missing_column():
    """Raises KeyError when the requested column is absent."""
    df = pd.DataFrame({'quantity': [100]})
    with pytest.raises(KeyError, match='missing_col'):
        convert_to_datetime(df, columns=['missing_col'])
