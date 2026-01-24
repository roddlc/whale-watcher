"""Tests for data extractor module."""

from datetime import date
from unittest.mock import Mock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from whale_watcher.database.models import Filer, Filing, Holding
from whale_watcher.etl.extractor import (
    get_existing_accession_numbers,
    get_or_create_filer,
    extract_new_filings,
    download_and_store_filing_metadata,
)
from whale_watcher.clients.sec_edgar import FilingMetadata


class TestGetExistingAccessionNumbers:
    """Test get_existing_accession_numbers function."""

    def test_returns_empty_set_when_no_filings_exist(self, db_session) -> None:
        """Test returns empty set when filer has no filings."""
        result = get_existing_accession_numbers(db_session, filer_id=999)
        assert result == set()

    def test_returns_accession_numbers_for_filer(self, db_session) -> None:
        """Test returns correct accession numbers for a filer."""
        # Create filer
        filer = Filer(
            cik="0001067983",
            name="Test Filer",
            category="test",
            enabled=True
        )
        db_session.add(filer)
        db_session.commit()

        # Create filings
        filing1 = Filing(
            filer_id=filer.id,
            accession_number="0001067983-25-000001",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            processed=False
        )
        filing2 = Filing(
            filer_id=filer.id,
            accession_number="0001067983-25-000002",
            filing_date=date(2025, 5, 14),
            period_of_report=date(2025, 3, 31),
            processed=False
        )
        db_session.add_all([filing1, filing2])
        db_session.commit()

        result = get_existing_accession_numbers(db_session, filer.id)

        assert result == {"0001067983-25-000001", "0001067983-25-000002"}

    def test_only_returns_accession_numbers_for_specified_filer(self, db_session) -> None:
        """Test does not return filings from other filers."""
        # Create two filers
        filer1 = Filer(cik="0001067983", name="Filer 1", category="test", enabled=True)
        filer2 = Filer(cik="0001234567", name="Filer 2", category="test", enabled=True)
        db_session.add_all([filer1, filer2])
        db_session.commit()

        # Create filing for filer1
        filing1 = Filing(
            filer_id=filer1.id,
            accession_number="0001067983-25-000001",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            processed=False
        )
        # Create filing for filer2
        filing2 = Filing(
            filer_id=filer2.id,
            accession_number="0001234567-25-000001",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            processed=False
        )
        db_session.add_all([filing1, filing2])
        db_session.commit()

        result = get_existing_accession_numbers(db_session, filer1.id)

        assert result == {"0001067983-25-000001"}
        assert "0001234567-25-000001" not in result


class TestGetOrCreateFiler:
    """Test get_or_create_filer function."""

    def test_creates_new_filer_when_not_exists(self, db_session) -> None:
        """Test creates new filer if it doesn't exist."""
        filer = get_or_create_filer(
            session=db_session,
            cik="0001067983",
            name="Berkshire Hathaway",
            description="Warren Buffett's vehicle",
            category="value_investing"
        )

        assert filer.id is not None
        assert filer.cik == "0001067983"
        assert filer.name == "Berkshire Hathaway"
        assert filer.description == "Warren Buffett's vehicle"
        assert filer.category == "value_investing"
        assert filer.enabled is True

        # Verify it's in database
        db_filer = db_session.query(Filer).filter(Filer.cik == "0001067983").first()
        assert db_filer is not None
        assert db_filer.id == filer.id

    def test_returns_existing_filer_when_exists(self, db_session) -> None:
        """Test returns existing filer if CIK already exists."""
        # Create filer first
        existing = Filer(
            cik="0001067983",
            name="Original Name",
            description="Original Description",
            category="original_category",
            enabled=True
        )
        db_session.add(existing)
        db_session.commit()
        original_id = existing.id

        # Try to create again with different details
        filer = get_or_create_filer(
            session=db_session,
            cik="0001067983",
            name="New Name",
            description="New Description",
            category="new_category"
        )

        # Should return existing filer with original details
        assert filer.id == original_id
        assert filer.name == "Original Name"  # Not updated
        assert filer.description == "Original Description"
        assert filer.category == "original_category"

    def test_handles_none_description(self, db_session) -> None:
        """Test creates filer with None description."""
        filer = get_or_create_filer(
            session=db_session,
            cik="0001067983",
            name="Test Filer",
            description=None,
            category="test"
        )

        assert filer.description is None


class TestExtractNewFilings:
    """Test extract_new_filings function."""

    @patch('whale_watcher.etl.extractor.SECEdgarClient')
    def test_creates_filer_if_not_exists(
        self,
        mock_client_class: Mock,
        mock_config: Mock,
        mock_db: Mock
    ) -> None:
        """Test creates filer in database if it doesn't exist."""
        # Mock client
        mock_client = Mock()
        mock_client.get_13f_filings.return_value = []
        mock_client_class.return_value = mock_client

        # Mock database session
        mock_session = Mock()
        mock_filer = Mock()
        mock_filer.id = 1
        # First call to query (in get_or_create_filer) - no existing filer
        # Second call to query (in get_existing_accession_numbers) - empty filings
        mock_session.query.return_value.filter.return_value.first.return_value = None
        mock_session.query.return_value.filter.return_value.all.return_value = []

        # After filer is created, it should have an ID
        def add_side_effect(obj):
            obj.id = 1
        mock_session.add.side_effect = add_side_effect

        mock_db.session_scope.return_value.__enter__.return_value = mock_session

        extract_new_filings(
            cik="0001067983",
            name="Berkshire Hathaway",
            description="Warren Buffett's vehicle",
            category="value_investing",
            config=mock_config,
            db=mock_db,
            limit=None
        )

        # Verify filer was created
        assert mock_session.add.called
        assert mock_session.flush.called

    @patch('whale_watcher.etl.extractor.SECEdgarClient')
    def test_fetches_filings_from_sec(
        self,
        mock_client_class: Mock,
        mock_config: Mock,
        mock_db: Mock
    ) -> None:
        """Test fetches filings from SEC API."""
        # Mock client
        mock_client = Mock()
        mock_filings = [
            FilingMetadata(
                accession_number="0001-25-001",
                filing_date=date(2025, 2, 14),
                report_date=date(2024, 12, 31),
                primary_document="doc.xml",
                form_type="13F-HR"
            )
        ]
        mock_client.get_13f_filings.return_value = mock_filings
        mock_client_class.return_value = mock_client

        # Mock database
        mock_session = Mock()
        mock_filer = Mock()
        mock_filer.id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_filer
        mock_session.query.return_value.filter.return_value.all.return_value = []
        mock_db.session_scope.return_value.__enter__.return_value = mock_session

        result = extract_new_filings(
            cik="0001067983",
            name="Test",
            description=None,
            category="test",
            config=mock_config,
            db=mock_db
        )

        # Verify SEC client was called
        mock_client.get_13f_filings.assert_called_once_with("0001067983")
        mock_client.close.assert_called_once()

        assert len(result) == 1

    @patch('whale_watcher.etl.extractor.SECEdgarClient')
    def test_filters_out_existing_filings(
        self,
        mock_client_class: Mock,
        mock_config: Mock,
        mock_db: Mock
    ) -> None:
        """Test filters out filings that already exist in database."""
        # Mock client with 3 filings
        mock_client = Mock()
        mock_filings = [
            FilingMetadata(
                accession_number="0001-25-001",
                filing_date=date(2025, 2, 14),
                report_date=date(2024, 12, 31),
                primary_document="doc1.xml",
                form_type="13F-HR"
            ),
            FilingMetadata(
                accession_number="0001-25-002",  # This one exists in DB
                filing_date=date(2025, 5, 14),
                report_date=date(2025, 3, 31),
                primary_document="doc2.xml",
                form_type="13F-HR"
            ),
            FilingMetadata(
                accession_number="0001-25-003",
                filing_date=date(2025, 8, 14),
                report_date=date(2025, 6, 30),
                primary_document="doc3.xml",
                form_type="13F-HR"
            ),
        ]
        mock_client.get_13f_filings.return_value = mock_filings
        mock_client_class.return_value = mock_client

        # Mock database - filing 002 already exists
        mock_session = Mock()
        mock_filer = Mock()
        mock_filer.id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_filer

        # Mock existing filing
        existing_filing = Mock()
        existing_filing.accession_number = "0001-25-002"
        mock_session.query.return_value.filter.return_value.all.return_value = [existing_filing]

        mock_db.session_scope.return_value.__enter__.return_value = mock_session

        result = extract_new_filings(
            cik="0001067983",
            name="Test",
            description=None,
            category="test",
            config=mock_config,
            db=mock_db
        )

        # Should only return 2 new filings (001 and 003)
        assert len(result) == 2
        assert result[0].accession_number == "0001-25-001"
        assert result[1].accession_number == "0001-25-003"

    @patch('whale_watcher.etl.extractor.SECEdgarClient')
    def test_respects_limit_parameter(
        self,
        mock_client_class: Mock,
        mock_config: Mock,
        mock_db: Mock
    ) -> None:
        """Test respects limit parameter for testing."""
        # Mock client with 3 filings
        mock_client = Mock()
        mock_filings = [
            FilingMetadata(
                accession_number=f"0001-25-00{i}",
                filing_date=date(2025, i * 2, 14),
                report_date=date(2024, 12, 31),
                primary_document=f"doc{i}.xml",
                form_type="13F-HR"
            )
            for i in range(1, 4)
        ]
        mock_client.get_13f_filings.return_value = mock_filings
        mock_client_class.return_value = mock_client

        # Mock database
        mock_session = Mock()
        mock_filer = Mock()
        mock_filer.id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_filer
        mock_session.query.return_value.filter.return_value.all.return_value = []
        mock_db.session_scope.return_value.__enter__.return_value = mock_session

        result = extract_new_filings(
            cik="0001067983",
            name="Test",
            description=None,
            category="test",
            config=mock_config,
            db=mock_db,
            limit=1  # Only get 1 filing
        )

        assert len(result) == 1
        assert result[0].accession_number == "0001-25-001"


class TestDownloadAndStoreFilingMetadata:
    """Test download_and_store_filing_metadata function."""

    @patch('whale_watcher.etl.extractor.parse_13f_info_table')
    @patch('whale_watcher.etl.extractor.SECEdgarClient')
    def test_downloads_xml_and_stores_metadata(
        self,
        mock_client_class: Mock,
        mock_parse: Mock,
        mock_config: Mock,
        mock_db: Mock
    ) -> None:
        """Test downloads XML and stores filing metadata."""
        # Mock SEC client
        mock_client = Mock()
        mock_client.download_filing_xml.return_value = "<xml>Test Filing</xml>"
        mock_client.download_info_table_xml.return_value = "<xml>Info Table</xml>"
        mock_client_class.return_value = mock_client

        # Mock parser
        from whale_watcher.etl.parser import FilingSummary, HoldingData
        mock_summary = FilingSummary(total_value=100000, holdings_count=1)
        mock_holdings = [
            HoldingData(
                cusip="037833100",
                security_name="APPLE INC",
                shares=1000,
                market_value=100000,
                voting_authority_sole=1000,
                voting_authority_shared=0,
                voting_authority_none=0
            )
        ]
        mock_parse.return_value = (mock_summary, mock_holdings)

        # Mock database
        mock_session = Mock()
        mock_filer = Mock()
        mock_filer.id = 1
        mock_session.query.return_value.filter.return_value.first.return_value = mock_filer

        # Mock filing record
        mock_filing_record = Mock()
        mock_filing_record.id = 123
        mock_session.add.side_effect = lambda obj: setattr(obj, 'id', 123)

        mock_db.session_scope.return_value.__enter__.return_value = mock_session

        filing_metadata = FilingMetadata(
            accession_number="0001067983-25-000005",
            filing_date=date(2025, 2, 14),
            report_date=date(2024, 12, 31),
            primary_document="form13fInfoTable.xml",
            form_type="13F-HR"
        )

        result = download_and_store_filing_metadata(
            cik="0001067983",
            name="Berkshire Hathaway",
            filing=filing_metadata,
            config=mock_config,
            db=mock_db
        )

        # Verify XML was downloaded
        mock_client.download_filing_xml.assert_called_once_with(
            "0001067983",
            "0001067983-25-000005",
            "form13fInfoTable.xml"
        )

        # Verify info table was downloaded
        mock_client.download_info_table_xml.assert_called_once_with(
            "0001067983",
            "0001067983-25-000005"
        )

        # Verify filing was added to session
        assert mock_session.add.called
        assert mock_session.flush.called
        mock_client.close.assert_called_once()

    @patch('whale_watcher.etl.extractor.parse_13f_info_table')
    @patch('whale_watcher.etl.extractor.SECEdgarClient')
    def test_raises_error_if_filer_not_found(
        self,
        mock_client_class: Mock,
        mock_parse: Mock,
        mock_config: Mock,
        mock_db: Mock
    ) -> None:
        """Test raises ValueError if filer doesn't exist in database."""
        # Mock SEC client
        mock_client = Mock()
        mock_client.download_filing_xml.return_value = "<xml>Test</xml>"
        mock_client.download_info_table_xml.return_value = "<xml>Info Table</xml>"
        mock_client_class.return_value = mock_client

        # Mock parser
        from whale_watcher.etl.parser import FilingSummary, HoldingData
        mock_summary = FilingSummary(total_value=100000, holdings_count=1)
        mock_holdings = [
            HoldingData(
                cusip="037833100",
                security_name="APPLE INC",
                shares=1000,
                market_value=100000,
                voting_authority_sole=1000,
                voting_authority_shared=0,
                voting_authority_none=0
            )
        ]
        mock_parse.return_value = (mock_summary, mock_holdings)

        # Mock database - filer not found
        mock_session = Mock()
        mock_session.query.return_value.filter.return_value.first.return_value = None
        mock_db.session_scope.return_value.__enter__.return_value = mock_session

        filing_metadata = FilingMetadata(
            accession_number="0001-25-001",
            filing_date=date(2025, 2, 14),
            report_date=date(2024, 12, 31),
            primary_document="doc.xml",
            form_type="13F-HR"
        )

        with pytest.raises(ValueError, match="Filer not found"):
            download_and_store_filing_metadata(
                cik="9999999999",
                name="Nonexistent",
                filing=filing_metadata,
                config=mock_config,
                db=mock_db
            )

    @patch('whale_watcher.etl.extractor.parse_13f_info_table')
    @patch('whale_watcher.etl.extractor.SECEdgarClient')
    def test_creates_filing_with_correct_fields(
        self,
        mock_client_class: Mock,
        mock_parse: Mock,
        mock_config: Mock,
        db_connection
    ) -> None:
        """Test creates Filing record with correct fields (integration with real DB)."""
        # Create real filer first using db_connection
        with db_connection.session_scope() as session:
            filer = Filer(
                cik="0001067983",
                name="Berkshire Hathaway",
                category="value_investing",
                enabled=True
            )
            session.add(filer)
            session.flush()
            filer_id = filer.id

        # Mock SEC client
        mock_client = Mock()
        mock_client.download_filing_xml.return_value = "<xml>Test Filing</xml>"
        mock_client.download_info_table_xml.return_value = "<xml>Info Table</xml>"
        mock_client_class.return_value = mock_client

        # Mock parser
        from whale_watcher.etl.parser import FilingSummary, HoldingData
        mock_summary = FilingSummary(total_value=100000, holdings_count=2)
        mock_holdings = [
            HoldingData(
                cusip="037833100",
                security_name="APPLE INC",
                shares=1000,
                market_value=50000,
                voting_authority_sole=1000,
                voting_authority_shared=0,
                voting_authority_none=0
            ),
            HoldingData(
                cusip="594918104",
                security_name="MICROSOFT CORP",
                shares=500,
                market_value=50000,
                voting_authority_sole=500,
                voting_authority_shared=0,
                voting_authority_none=0
            )
        ]
        mock_parse.return_value = (mock_summary, mock_holdings)

        filing_metadata = FilingMetadata(
            accession_number="0001067983-25-000005",
            filing_date=date(2025, 2, 14),
            report_date=date(2024, 12, 31),
            primary_document="form13fInfoTable.xml",
            form_type="13F-HR"
        )

        filing_id = download_and_store_filing_metadata(
            cik="0001067983",
            name="Berkshire Hathaway",
            filing=filing_metadata,
            config=mock_config,
            db=db_connection
        )

        # Verify filing was created correctly with Phase 4 updates
        with db_connection.session_scope() as session:
            filing = session.query(Filing).filter(Filing.id == filing_id).first()
            assert filing is not None
            assert filing.filer_id == filer_id
            assert filing.accession_number == "0001067983-25-000005"
            assert filing.filing_date == date(2025, 2, 14)
            assert filing.period_of_report == date(2024, 12, 31)
            assert filing.processed is True  # Phase 4: now set to True
            assert filing.total_value == 100000  # Phase 4: now populated
            assert filing.holdings_count == 2  # Phase 4: now populated

            # Verify holdings were loaded
            holdings = session.query(Holding).filter(Holding.filing_id == filing_id).all()
            assert len(holdings) == 2


@pytest.fixture
def mock_config() -> Mock:
    """Mock configuration object."""
    config = Mock()
    config.user_agent = "TestAgent/1.0"
    config.requests_per_second = 10
    config.start_year = 2025
    config.end_year = 2025
    return config


@pytest.fixture
def mock_db() -> Mock:
    """Mock database connection with context manager support.

    MagicMock is needed instead of Mock because our code uses:
        with db.session_scope() as session:

    This requires __enter__ and __exit__ magic methods which regular Mock
    doesn't support. MagicMock automatically provides these magic methods.
    """
    from unittest.mock import MagicMock
    db = MagicMock()
    return db
