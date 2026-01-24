"""Tests for data loader module."""

from datetime import date

import pytest

from whale_watcher.database.models import Filer, Filing, Holding
from whale_watcher.etl.loader import load_holdings, update_filing_summary
from whale_watcher.etl.parser import FilingSummary, HoldingData


class TestLoadHoldings:
    """Test load_holdings function."""

    def test_bulk_insert_holdings(self, db_session) -> None:
        """Test successfully inserts multiple holdings."""
        # Create Filer
        filer = Filer(
            cik="0001067983",
            name="Test Filer",
            category="test",
            enabled=True
        )
        db_session.add(filer)
        db_session.flush()

        # Create Filing
        filing = Filing(
            filer_id=filer.id,
            accession_number="0001067983-25-000001",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            processed=False
        )
        db_session.add(filing)
        db_session.flush()
        filing_id = filing.id

        # Create HoldingData objects
        holdings_data = [
            HoldingData(
                cusip="037833100",
                security_name="APPLE INC",
                shares=1000000,
                market_value=150000,
                voting_authority_sole=1000000,
                voting_authority_shared=0,
                voting_authority_none=0
            ),
            HoldingData(
                cusip="594918104",
                security_name="MICROSOFT CORP",
                shares=500000,
                market_value=200000,
                voting_authority_sole=500000,
                voting_authority_shared=0,
                voting_authority_none=0
            ),
            HoldingData(
                cusip="30303M102",
                security_name="META PLATFORMS INC",
                shares=250000,
                market_value=100000,
                voting_authority_sole=250000,
                voting_authority_shared=0,
                voting_authority_none=0
            )
        ]

        # Load holdings
        load_holdings(db_session, filing_id, holdings_data)

        # Query holdings from database
        holdings = db_session.query(Holding).filter(Holding.filing_id == filing_id).all()

        # Verify count
        assert len(holdings) == 3

        # Verify first holding
        apple = next(h for h in holdings if h.cusip == "037833100")
        assert apple.security_name == "APPLE INC"
        assert apple.shares == 1000000
        assert apple.market_value == 150000
        assert apple.voting_authority_sole == 1000000
        assert apple.voting_authority_shared == 0
        assert apple.voting_authority_none == 0
        assert apple.discretion is None  # Not set by loader

        # Verify second holding
        msft = next(h for h in holdings if h.cusip == "594918104")
        assert msft.security_name == "MICROSOFT CORP"
        assert msft.shares == 500000
        assert msft.market_value == 200000

    def test_empty_holdings_list(self, db_session) -> None:
        """Test handles empty holdings list gracefully."""
        # Create Filer
        filer = Filer(
            cik="0001067983",
            name="Test Filer",
            category="test",
            enabled=True
        )
        db_session.add(filer)
        db_session.flush()

        # Create Filing
        filing = Filing(
            filer_id=filer.id,
            accession_number="0001067983-25-000001",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            processed=False
        )
        db_session.add(filing)
        db_session.flush()
        filing_id = filing.id

        # Load empty list
        load_holdings(db_session, filing_id, [])

        # Verify no holdings were created
        holdings_count = db_session.query(Holding).filter(Holding.filing_id == filing_id).count()
        assert holdings_count == 0

    def test_holdings_with_all_voting_authority_fields(self, db_session) -> None:
        """Test holdings with various voting authority combinations."""
        # Create Filer
        filer = Filer(
            cik="0001067983",
            name="Test Filer",
            category="test",
            enabled=True
        )
        db_session.add(filer)
        db_session.flush()

        # Create Filing
        filing = Filing(
            filer_id=filer.id,
            accession_number="0001067983-25-000001",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            processed=False
        )
        db_session.add(filing)
        db_session.flush()
        filing_id = filing.id

        # Create holdings with different voting authority patterns
        holdings_data = [
            HoldingData(
                cusip="037833100",
                security_name="APPLE INC",
                shares=1000000,
                market_value=150000,
                voting_authority_sole=1000000,
                voting_authority_shared=0,
                voting_authority_none=0
            ),
            HoldingData(
                cusip="594918104",
                security_name="MICROSOFT CORP",
                shares=500000,
                market_value=200000,
                voting_authority_sole=0,
                voting_authority_shared=500000,
                voting_authority_none=0
            ),
            HoldingData(
                cusip="30303M102",
                security_name="META PLATFORMS INC",
                shares=250000,
                market_value=100000,
                voting_authority_sole=100000,
                voting_authority_shared=100000,
                voting_authority_none=50000
            )
        ]

        # Load holdings
        load_holdings(db_session, filing_id, holdings_data)

        # Verify voting authority fields
        holdings = db_session.query(Holding).filter(Holding.filing_id == filing_id).all()

        apple = next(h for h in holdings if h.cusip == "037833100")
        assert apple.voting_authority_sole == 1000000
        assert apple.voting_authority_shared == 0
        assert apple.voting_authority_none == 0

        msft = next(h for h in holdings if h.cusip == "594918104")
        assert msft.voting_authority_sole == 0
        assert msft.voting_authority_shared == 500000
        assert msft.voting_authority_none == 0

        meta = next(h for h in holdings if h.cusip == "30303M102")
        assert meta.voting_authority_sole == 100000
        assert meta.voting_authority_shared == 100000
        assert meta.voting_authority_none == 50000


class TestUpdateFilingSummary:
    """Test update_filing_summary function."""

    def test_update_summary_fields(self, db_session) -> None:
        """Test successfully updates filing summary fields."""
        # Create Filer
        filer = Filer(
            cik="0001067983",
            name="Test Filer",
            category="test",
            enabled=True
        )
        db_session.add(filer)
        db_session.flush()

        # Create Filing with processed=False
        filing = Filing(
            filer_id=filer.id,
            accession_number="0001067983-25-000001",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            processed=False,
            total_value=None,
            holdings_count=None
        )
        db_session.add(filing)
        db_session.flush()
        filing_id = filing.id

        # Create FilingSummary
        summary = FilingSummary(
            total_value=450000,
            holdings_count=3
        )

        # Update filing summary
        update_filing_summary(db_session, filing_id, summary)

        # Query and verify
        updated_filing = db_session.query(Filing).filter(Filing.id == filing_id).first()
        assert updated_filing is not None
        assert updated_filing.total_value == 450000
        assert updated_filing.holdings_count == 3
        assert updated_filing.processed is True

    def test_update_nonexistent_filing(self, db_session) -> None:
        """Test raises ValueError when filing doesn't exist."""
        # Create FilingSummary
        summary = FilingSummary(
            total_value=450000,
            holdings_count=3
        )

        # Try to update nonexistent filing
        with pytest.raises(ValueError, match="Filing not found with id=999"):
            update_filing_summary(db_session, 999, summary)

    def test_summary_values_are_correct(self, db_session) -> None:
        """Test verifies exact values from summary are stored."""
        # Create Filer
        filer = Filer(
            cik="0001067983",
            name="Test Filer",
            category="test",
            enabled=True
        )
        db_session.add(filer)
        db_session.flush()

        # Create Filing
        filing = Filing(
            filer_id=filer.id,
            accession_number="0001067983-25-000001",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            processed=False
        )
        db_session.add(filing)
        db_session.flush()
        filing_id = filing.id

        # Create summary with specific values
        summary = FilingSummary(
            total_value=123456789,
            holdings_count=42
        )

        # Update filing
        update_filing_summary(db_session, filing_id, summary)

        # Verify exact values
        updated_filing = db_session.query(Filing).filter(Filing.id == filing_id).first()
        assert updated_filing.total_value == 123456789
        assert updated_filing.holdings_count == 42
        assert updated_filing.processed is True

    def test_marks_filing_as_processed(self, db_session) -> None:
        """Test ensures processed flag is set to True."""
        # Create Filer
        filer = Filer(
            cik="0001067983",
            name="Test Filer",
            category="test",
            enabled=True
        )
        db_session.add(filer)
        db_session.flush()

        # Create Filing with processed explicitly False
        filing = Filing(
            filer_id=filer.id,
            accession_number="0001067983-25-000001",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            processed=False
        )
        db_session.add(filing)
        db_session.flush()
        filing_id = filing.id

        # Verify it starts as False
        assert filing.processed is False

        # Update summary
        summary = FilingSummary(total_value=100000, holdings_count=1)
        update_filing_summary(db_session, filing_id, summary)

        # Verify processed is now True
        updated_filing = db_session.query(Filing).filter(Filing.id == filing_id).first()
        assert updated_filing.processed is True


class TestTransactionBehavior:
    """Test transaction and rollback behavior."""

    def test_rollback_on_error(self, db_connection) -> None:
        """Test that holdings are not committed if error occurs."""
        # Create Filer and Filing
        with db_connection.session_scope() as session:
            filer = Filer(
                cik="0001067983",
                name="Test Filer",
                category="test",
                enabled=True
            )
            session.add(filer)
            session.flush()
            filer_id = filer.id

            filing = Filing(
                filer_id=filer_id,
                accession_number="0001067983-25-000001",
                filing_date=date(2025, 2, 14),
                period_of_report=date(2024, 12, 31),
                processed=False
            )
            session.add(filing)
            session.flush()
            filing_id = filing.id

        # Try to load holdings in transaction that will fail
        holdings_data = [
            HoldingData(
                cusip="037833100",
                security_name="APPLE INC",
                shares=1000000,
                market_value=150000,
                voting_authority_sole=1000000,
                voting_authority_shared=0,
                voting_authority_none=0
            )
        ]

        # Use session_scope and force an error
        with pytest.raises(RuntimeError):
            with db_connection.session_scope() as session:
                load_holdings(session, filing_id, holdings_data)
                # Force error to trigger rollback
                raise RuntimeError("Simulated error")

        # Verify holdings were rolled back
        with db_connection.session_scope() as session:
            holdings_count = session.query(Holding).filter(Holding.filing_id == filing_id).count()
            assert holdings_count == 0

    def test_load_and_update_in_one_transaction(self, db_connection) -> None:
        """Test both functions work correctly in single transaction."""
        # Create Filer and Filing
        with db_connection.session_scope() as session:
            filer = Filer(
                cik="0001067983",
                name="Test Filer",
                category="test",
                enabled=True
            )
            session.add(filer)
            session.flush()
            filer_id = filer.id

            filing = Filing(
                filer_id=filer_id,
                accession_number="0001067983-25-000001",
                filing_date=date(2025, 2, 14),
                period_of_report=date(2024, 12, 31),
                processed=False
            )
            session.add(filing)
            session.flush()
            filing_id = filing.id

        # Load holdings and update summary in single transaction
        holdings_data = [
            HoldingData(
                cusip="037833100",
                security_name="APPLE INC",
                shares=1000000,
                market_value=150000,
                voting_authority_sole=1000000,
                voting_authority_shared=0,
                voting_authority_none=0
            ),
            HoldingData(
                cusip="594918104",
                security_name="MICROSOFT CORP",
                shares=500000,
                market_value=200000,
                voting_authority_sole=500000,
                voting_authority_shared=0,
                voting_authority_none=0
            )
        ]

        summary = FilingSummary(
            total_value=350000,
            holdings_count=2
        )

        with db_connection.session_scope() as session:
            load_holdings(session, filing_id, holdings_data)
            update_filing_summary(session, filing_id, summary)

        # Verify everything was committed
        with db_connection.session_scope() as session:
            filing = session.query(Filing).filter(Filing.id == filing_id).first()
            holdings = session.query(Holding).filter(Holding.filing_id == filing_id).all()

            assert filing is not None
            assert filing.total_value == 350000
            assert filing.holdings_count == 2
            assert filing.processed is True
            assert len(holdings) == 2
