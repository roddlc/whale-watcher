"""Tests for SEC EDGAR API client."""

import time
from datetime import date
from unittest.mock import Mock, patch

import pytest
import requests

from whale_watcher.clients.sec_edgar import SECEdgarClient, FilingMetadata


class TestFilingMetadata:
    """Test FilingMetadata dataclass."""

    def test_filing_metadata_creation(self) -> None:
        """Test FilingMetadata can be created with all fields."""
        filing = FilingMetadata(
            accession_number="0001067983-25-000005",
            filing_date=date(2025, 2, 14),
            report_date=date(2024, 12, 31),
            primary_document="form13fInfoTable.xml",
            form_type="13F-HR"
        )

        assert filing.accession_number == "0001067983-25-000005"
        assert filing.filing_date == date(2025, 2, 14)
        assert filing.report_date == date(2024, 12, 31)
        assert filing.primary_document == "form13fInfoTable.xml"
        assert filing.form_type == "13F-HR"


class TestSECEdgarClient:
    """Test SEC EDGAR API client."""

    @pytest.fixture
    def mock_config(self) -> Mock:
        """Create mock config object."""
        config = Mock()
        config.user_agent = "TestAgent/1.0 (test@example.com)"
        config.requests_per_second = 10  # Fast for tests
        return config

    def test_initialization(self, mock_config: Mock) -> None:
        """Test client initializes with correct settings."""
        client = SECEdgarClient(mock_config)

        assert client.config == mock_config
        assert client._min_interval == 0.1  # 1/10 req/sec
        assert client.session.headers['User-Agent'] == "TestAgent/1.0 (test@example.com)"
        assert client.session.headers['Accept'] == 'application/json'

    def test_rate_limit_enforces_delay(self, mock_config: Mock) -> None:
        """Test that rate limiter enforces minimum delay between requests."""
        client = SECEdgarClient(mock_config)

        # First call should not sleep
        start = time.time()
        client._rate_limit()
        first_call_duration = time.time() - start
        assert first_call_duration < 0.01  # Should be nearly instant

        # Second call immediately after should sleep
        start = time.time()
        client._rate_limit()
        second_call_duration = time.time() - start
        assert second_call_duration >= 0.09  # Should sleep ~0.1 sec

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_get_submissions_constructs_correct_url(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test that get_submissions constructs correct URL with zero-padded CIK."""
        client = SECEdgarClient(mock_config)

        # Mock successful response
        mock_response = Mock()
        mock_response.json.return_value = {
            'cik': '0001067983',
            'name': 'BERKSHIRE HATHAWAY INC'
        }
        mock_get.return_value = mock_response

        # Call with CIK without leading zeros
        result = client.get_submissions("1067983")

        # Verify URL construction
        call_args = mock_get.call_args[0]
        assert "CIK0001067983.json" in call_args[0]
        assert call_args[0].startswith("https://data.sec.gov/submissions/")

        # Verify result
        assert result == mock_response.json.return_value

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_get_submissions_handles_already_padded_cik(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test get_submissions works with already zero-padded CIK."""
        client = SECEdgarClient(mock_config)

        mock_response = Mock()
        mock_response.json.return_value = {'cik': '0001067983'}
        mock_get.return_value = mock_response

        result = client.get_submissions("0001067983")

        call_args = mock_get.call_args[0]
        # Should not double-pad
        assert "CIK0001067983.json" in call_args[0]

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_get_submissions_raises_on_http_error(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test get_submissions raises HTTPError on failed request."""
        client = SECEdgarClient(mock_config)

        # Mock 403 response
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            client.get_submissions("1067983")

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_get_13f_filings_filters_by_form_type(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test get_13f_filings filters for 13F-HR forms only."""
        mock_config.start_year = 2025
        mock_config.end_year = 2025
        client = SECEdgarClient(mock_config)

        # Mock submissions response with mixed form types
        mock_response = Mock()
        mock_response.json.return_value = {
            'filings': {
                'recent': {
                    'accessionNumber': ['0001-25-001', '0001-25-002', '0001-25-003'],
                    'filingDate': ['2025-02-14', '2025-02-13', '2025-01-15'],
                    'reportDate': ['2024-12-31', '2024-12-31', '2024-09-30'],
                    'primaryDocument': ['doc1.xml', 'doc2.xml', 'doc3.xml'],
                    'form': ['13F-HR', '10-K', '13F-HR']  # Mixed forms
                }
            }
        }
        mock_get.return_value = mock_response

        result = client.get_13f_filings("1067983")

        # Should only return 13F-HR filings
        assert len(result) == 2
        assert all(f.form_type == "13F-HR" for f in result)
        assert result[0].accession_number == '0001-25-001'
        assert result[1].accession_number == '0001-25-003'

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_get_13f_filings_filters_by_year(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test get_13f_filings filters by year range."""
        mock_config.start_year = 2025
        mock_config.end_year = 2025
        client = SECEdgarClient(mock_config)

        # Mock submissions with filings from different years
        mock_response = Mock()
        mock_response.json.return_value = {
            'filings': {
                'recent': {
                    'accessionNumber': ['0001-25-001', '0001-24-001', '0001-25-002'],
                    'filingDate': ['2025-02-14', '2024-02-14', '2025-05-14'],
                    'reportDate': ['2024-12-31', '2023-12-31', '2025-03-31'],
                    'primaryDocument': ['doc1.xml', 'doc2.xml', 'doc3.xml'],
                    'form': ['13F-HR', '13F-HR', '13F-HR']
                }
            }
        }
        mock_get.return_value = mock_response

        result = client.get_13f_filings("1067983")

        # Should only return 2025 filings based on report_date
        assert len(result) == 1
        assert result[0].report_date.year == 2025

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_get_13f_filings_uses_custom_year_range(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test get_13f_filings accepts custom year range."""
        mock_config.start_year = 2025  # Default
        mock_config.end_year = 2025    # Default
        client = SECEdgarClient(mock_config)

        mock_response = Mock()
        mock_response.json.return_value = {
            'filings': {
                'recent': {
                    'accessionNumber': ['0001-24-001', '0001-23-001'],
                    'filingDate': ['2024-02-14', '2023-02-14'],
                    'reportDate': ['2023-12-31', '2022-12-31'],
                    'primaryDocument': ['doc1.xml', 'doc2.xml'],
                    'form': ['13F-HR', '13F-HR']
                }
            }
        }
        mock_get.return_value = mock_response

        # Override with custom years
        result = client.get_13f_filings("1067983", start_year=2023, end_year=2024)

        # Should return both 2023 and 2022 report dates (within 2023-2024 range)
        assert len(result) == 1
        assert result[0].report_date.year == 2023

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_get_13f_filings_returns_filing_metadata(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test get_13f_filings returns properly structured FilingMetadata objects."""
        mock_config.start_year = 2025
        mock_config.end_year = 2025
        client = SECEdgarClient(mock_config)

        mock_response = Mock()
        mock_response.json.return_value = {
            'filings': {
                'recent': {
                    'accessionNumber': ['0001067983-25-000005'],
                    'filingDate': ['2025-02-14'],
                    'reportDate': ['2024-12-31'],
                    'primaryDocument': ['form13fInfoTable.xml'],
                    'form': ['13F-HR']
                }
            }
        }
        mock_get.return_value = mock_response

        result = client.get_13f_filings("1067983")

        assert len(result) == 1
        filing = result[0]
        assert isinstance(filing, FilingMetadata)
        assert filing.accession_number == '0001067983-25-000005'
        assert filing.filing_date == date(2025, 2, 14)
        assert filing.report_date == date(2024, 12, 31)
        assert filing.primary_document == 'form13fInfoTable.xml'
        assert filing.form_type == '13F-HR'

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_download_filing_xml_constructs_correct_url(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test download_filing_xml constructs correct SEC Archives URL."""
        client = SECEdgarClient(mock_config)

        mock_response = Mock()
        mock_response.text = '<xml>Sample Filing</xml>'
        mock_get.return_value = mock_response

        result = client.download_filing_xml(
            cik="0001067983",
            accession_number="0001067983-25-000005",
            primary_document="form13fInfoTable.xml"
        )

        # Verify URL construction
        call_args = mock_get.call_args[0]
        url = call_args[0]

        # Should remove leading zeros from CIK
        assert "/1067983/" in url
        assert "CIK0001067983" not in url  # No "CIK" prefix in archives URL

        # Should remove dashes from accession number
        assert "/000106798325000005/" in url
        assert "0001067983-25-000005" not in url

        # Should include primary document
        assert url.endswith("form13fInfoTable.xml")

        # Should use correct base URL
        assert url.startswith("https://www.sec.gov/Archives/edgar/data/")

        assert result == '<xml>Sample Filing</xml>'

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_download_filing_xml_raises_on_http_error(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test download_filing_xml raises HTTPError on failed request."""
        client = SECEdgarClient(mock_config)

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response

        with pytest.raises(requests.HTTPError):
            client.download_filing_xml(
                cik="0001067983",
                accession_number="0001067983-25-000005",
                primary_document="form13fInfoTable.xml"
            )

    def test_close_closes_session(self, mock_config: Mock) -> None:
        """Test close method closes the requests session."""
        client = SECEdgarClient(mock_config)

        with patch.object(client.session, 'close') as mock_close:
            client.close()
            mock_close.assert_called_once()

    @patch('whale_watcher.clients.sec_edgar.requests.Session.get')
    def test_get_13f_filings_handles_empty_filings(
        self,
        mock_get: Mock,
        mock_config: Mock
    ) -> None:
        """Test get_13f_filings handles response with no filings."""
        mock_config.start_year = 2025
        mock_config.end_year = 2025
        client = SECEdgarClient(mock_config)

        mock_response = Mock()
        mock_response.json.return_value = {
            'filings': {
                'recent': {
                    'accessionNumber': [],
                    'filingDate': [],
                    'reportDate': [],
                    'primaryDocument': [],
                    'form': []
                }
            }
        }
        mock_get.return_value = mock_response

        result = client.get_13f_filings("1067983")

        assert result == []
