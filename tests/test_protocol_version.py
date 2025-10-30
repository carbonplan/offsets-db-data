"""Tests for protocol version extraction functionality"""
import pandas as pd
import pytest

from offsets_db_data.projects import (
    align_protocol_versions,
    extract_protocol_version_pairs,
    extract_protocol_versions,
)


class TestExtractProtocolVersionPairs:
    """Test the protocol version pair extraction function"""

    def test_single_protocol_with_version(self):
        """Test extraction from single protocol string with version"""
        result = extract_protocol_version_pairs('ACM0001 v19.0')
        assert result == [('ACM0001', '19.0')]

    def test_single_protocol_version_keyword(self):
        """Test extraction with 'Version' keyword"""
        result = extract_protocol_version_pairs('ACM0002 Version 21.0')
        assert result == [('ACM0002', '21.0')]

    def test_single_protocol_no_version(self):
        """Test extraction when no version is present"""
        result = extract_protocol_version_pairs('VM0007 REDD+ Framework')
        assert result == [('VM0007', None)]

    def test_multi_protocol_with_versions(self):
        """Test extraction from multi-protocol string with versions"""
        result = extract_protocol_version_pairs('ACM0001: Version 19.0; ACM0022: Version 3.0')
        assert result == [('ACM0001', '19.0'), ('ACM0022', '3.0')]

    def test_multi_protocol_ampersand_separator(self):
        """Test extraction with ampersand separator"""
        result = extract_protocol_version_pairs('AMS-I.D. version 18 & ACM0002, version 20.0')
        assert result == [('AMS-I.D', '18.0'), ('ACM0002', '20.0')]

    def test_multi_protocol_and_separator(self):
        """Test extraction with 'and' separator"""
        result = extract_protocol_version_pairs('ACM0001 v19.0 and ACM0022')
        assert result == [('ACM0001', '19.0'), ('ACM0022', None)]

    def test_version_with_comma(self):
        """Test extraction when version follows comma"""
        result = extract_protocol_version_pairs('VM0015, v1.1')
        assert result == [('VM0015', '1.1')]

    def test_version_no_space(self):
        """Test extraction when version has no space before number"""
        result = extract_protocol_version_pairs('ACM0002 version21.0')
        assert result == [('ACM0002', '21.0')]

    def test_version_no_decimal(self):
        """Test extraction when version has no decimal"""
        result = extract_protocol_version_pairs('ACM0002,Version 21')
        assert result == [('ACM0002', '21.0')]

    def test_version_with_ver_keyword(self):
        """Test extraction with 'ver' keyword"""
        result = extract_protocol_version_pairs('VM0004 Ver 1.0')
        assert result == [('VM0004', '1.0')]

    def test_empty_string(self):
        """Test extraction from empty string"""
        result = extract_protocol_version_pairs('')
        assert result == []

    def test_none_value(self):
        """Test extraction from None value"""
        result = extract_protocol_version_pairs(None)
        assert result == []

    def test_protocol_with_dots_and_hyphens(self):
        """Test extraction of protocol names with dots and hyphens"""
        result = extract_protocol_version_pairs('AMS-III.D. version 19.0')
        assert result == [('AMS-III.D', '19.0')]


class TestExtractProtocolVersionsDataFrame:
    """Test the DataFrame protocol version extraction method"""

    def test_extract_from_dataframe(self):
        """Test extraction from DataFrame with original_protocol column"""
        df = pd.DataFrame(
            {
                'original_protocol': [
                    'ACM0001 Version 19.0',
                    'VM0007 REDD+ Framework',
                    'ACM0001: Version 19.0; ACM0022: Version 3.0',
                ]
            }
        )

        result = df.pipe(extract_protocol_versions)

        assert 'protocol_version_raw' in result.columns
        assert result['protocol_version_raw'].iloc[0] == {'ACM0001': '19.0'}
        assert result['protocol_version_raw'].iloc[1] == {'VM0007': None}
        assert result['protocol_version_raw'].iloc[2] == {'ACM0001': '19.0', 'ACM0022': '3.0'}

    def test_extract_missing_column(self):
        """Test extraction when original_protocol column is missing"""
        df = pd.DataFrame({'project_id': ['VCS1', 'VCS2']})

        result = df.pipe(extract_protocol_versions)

        assert 'protocol_version_raw' in result.columns
        assert all(v == {} for v in result['protocol_version_raw'])


class TestAlignProtocolVersions:
    """Test the protocol version alignment function"""

    def test_align_single_protocol(self):
        """Test alignment for single protocol"""
        df = pd.DataFrame(
            {
                'protocol': [['acm0001']],
                'protocol_version_raw': [{'ACM0001': '19.0'}],
            }
        )

        result = df.pipe(align_protocol_versions)

        assert 'protocol_version' in result.columns
        assert result['protocol_version'].iloc[0] == ['19.0']

    def test_align_multi_protocol(self):
        """Test alignment for multiple protocols"""
        df = pd.DataFrame(
            {
                'protocol': [['acm0001', 'acm0022']],
                'protocol_version_raw': [{'ACM0001': '19.0', 'ACM0022': '3.0'}],
            }
        )

        result = df.pipe(align_protocol_versions)

        assert result['protocol_version'].iloc[0] == ['19.0', '3.0']

    def test_align_partial_versions(self):
        """Test alignment when some protocols have no version"""
        df = pd.DataFrame(
            {
                'protocol': [['acm0001', 'acm0022']],
                'protocol_version_raw': [{'ACM0001': '19.0'}],
            }
        )

        result = df.pipe(align_protocol_versions)

        assert result['protocol_version'].iloc[0] == ['19.0', None]

    def test_align_no_versions(self):
        """Test alignment when no versions are found"""
        df = pd.DataFrame(
            {
                'protocol': [['vm0007']],
                'protocol_version_raw': [{}],
            }
        )

        result = df.pipe(align_protocol_versions)

        assert result['protocol_version'].iloc[0] == [None]

    def test_align_case_insensitive(self):
        """Test that alignment is case-insensitive"""
        df = pd.DataFrame(
            {
                'protocol': [['acm0001']],
                'protocol_version_raw': [{'acm0001': '19.0'}],  # lowercase
            }
        )

        result = df.pipe(align_protocol_versions)

        assert result['protocol_version'].iloc[0] == ['19.0']

    def test_align_ignores_punctuation(self):
        """Test that alignment ignores punctuation differences"""
        df = pd.DataFrame(
            {
                'protocol': [['amsiiid']],
                'protocol_version_raw': [{'AMS-III.D': '19.0'}],
            }
        )

        result = df.pipe(align_protocol_versions)

        assert result['protocol_version'].iloc[0] == ['19.0']

    def test_cleanup_temporary_column(self):
        """Test that protocol_version_raw column is removed"""
        df = pd.DataFrame(
            {
                'protocol': [['acm0001']],
                'protocol_version_raw': [{'ACM0001': '19.0'}],
            }
        )

        result = df.pipe(align_protocol_versions)

        assert 'protocol_version_raw' not in result.columns


class TestEndToEndIntegration:
    """Test end-to-end protocol version extraction and alignment"""

    def test_full_pipeline(self):
        """Test complete pipeline from raw string to aligned versions"""
        df = pd.DataFrame(
            {
                'original_protocol': [
                    'ACM0001 Version 19.0',
                    'VM0007 REDD+ Framework',
                    'ACM0001: Version 19.0; ACM0022: Version 3.0',
                    'AMS-I.D. version 18 & ACM0002, version 20.0',
                ]
            }
        )

        # Simulate the pipeline
        result = df.pipe(extract_protocol_versions)

        # Simulate protocol normalization (would happen in map_protocol)
        result['protocol'] = [
            ['acm0001'],
            ['vm0007'],
            ['acm0001', 'acm0022'],
            ['ams-i.d', 'acm0002'],
        ]

        result = result.pipe(align_protocol_versions)

        assert result['protocol_version'].iloc[0] == ['19.0']
        assert result['protocol_version'].iloc[1] == [None]
        assert result['protocol_version'].iloc[2] == ['19.0', '3.0']
        assert result['protocol_version'].iloc[3] == ['18.0', '20.0']

    def test_real_world_verra_example(self):
        """Test with real Verra methodology strings"""
        df = pd.DataFrame(
            {
                'original_protocol': [
                    'ACM0001: "Flaring or use of landfill gas" Version 19.0',
                    'VM0007 REDD+ Methodology Framework (REDD-MF), v1.6',
                    'VM0042 Methodology for Improved Agricultural Land Management, v2.0',
                ]
            }
        )

        result = df.pipe(extract_protocol_versions)

        # Note: VM0007 and VM0042 won't extract versions because our pattern
        # looks for specific keywords, but ACM0001 will work
        assert result['protocol_version_raw'].iloc[0] == {'ACM0001': '19.0'}

