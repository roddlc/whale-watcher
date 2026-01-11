"""Tests for database models."""

from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from whale_watcher.database.models import (
    Base,
    ChangeType,
    Filer,
    Filing,
    Holding,
    PositionChange,
)


@pytest.fixture
def engine():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture
def session(engine) -> Session:
    """Create a database session for testing."""
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    yield session
    session.close()


class TestFilerModel:
    """Test Filer model."""

    def test_create_filer(self, session: Session) -> None:
        """Test creating a filer record."""
        filer = Filer(
            cik="0001067983",
            name="Berkshire Hathaway",
            description="Warren Buffett's investment vehicle",
            category="value_investing",
            enabled=True
        )
        session.add(filer)
        session.commit()

        assert filer.id is not None
        assert filer.cik == "0001067983"
        assert filer.name == "Berkshire Hathaway"
        assert filer.enabled is True

    def test_filer_timestamps_auto_populated(self, session: Session) -> None:
        """Test that created_at and updated_at are automatically set."""
        filer = Filer(
            cik="0001067983",
            name="Test Filer",
            category="test",
            enabled=True
        )
        session.add(filer)
        session.commit()

        assert filer.created_at is not None
        assert filer.updated_at is not None
        assert isinstance(filer.created_at, datetime)
        assert isinstance(filer.updated_at, datetime)

    def test_filer_cik_unique_constraint(self, session: Session) -> None:
        """Test that CIK must be unique."""
        filer1 = Filer(cik="0001067983", name="Filer 1", category="test", enabled=True)
        filer2 = Filer(cik="0001067983", name="Filer 2", category="test", enabled=True)

        session.add(filer1)
        session.commit()

        session.add(filer2)
        with pytest.raises(IntegrityError):
            session.commit()

    def test_filer_relationships(self, session: Session) -> None:
        """Test filer relationships to filings and position_changes."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        # Should have empty relationships initially
        assert len(filer.filings) == 0
        assert len(filer.position_changes) == 0


class TestFilingModel:
    """Test Filing model."""

    def test_create_filing(self, session: Session) -> None:
        """Test creating a filing record."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31),
            total_value=1500000000,
            holdings_count=50,
            processed=False
        )
        session.add(filing)
        session.commit()

        assert filing.id is not None
        assert filing.filer_id == filer.id
        assert filing.accession_number == "0001193125-25-001234"
        assert filing.processed is False

    def test_filing_accession_number_unique(self, session: Session) -> None:
        """Test that accession_number must be unique."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        filing1 = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        filing2 = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 15),
            period_of_report=date(2025, 3, 31)
        )

        session.add(filing1)
        session.commit()

        session.add(filing2)
        with pytest.raises(IntegrityError):
            session.commit()

    def test_filing_filer_period_unique_constraint(self, session: Session) -> None:
        """Test that (filer_id, period_of_report) must be unique."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        filing1 = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        filing2 = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-999999",  # Different accession
            filing_date=date(2025, 2, 15),
            period_of_report=date(2024, 12, 31)  # Same period
        )

        session.add(filing1)
        session.commit()

        session.add(filing2)
        with pytest.raises(IntegrityError):
            session.commit()

    def test_filing_filer_relationship(self, session: Session) -> None:
        """Test filing relationship to filer."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        session.add(filing)
        session.commit()

        # Navigate relationship
        assert filing.filer.id == filer.id
        assert filing.filer.name == "Test Filer"

        # Navigate reverse relationship
        assert len(filer.filings) == 1
        assert filer.filings[0].id == filing.id


class TestHoldingModel:
    """Test Holding model."""

    def test_create_holding(self, session: Session) -> None:
        """Test creating a holding record."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        session.add(filing)
        session.commit()

        holding = Holding(
            filing_id=filing.id,
            cusip="037833100",
            security_name="Apple Inc",
            shares=1000000,
            market_value=150000,  # In thousands
            voting_authority_sole=1000000,
            voting_authority_shared=0,
            voting_authority_none=0,
            discretion="SOLE"
        )
        session.add(holding)
        session.commit()

        assert holding.id is not None
        assert holding.filing_id == filing.id
        assert holding.cusip == "037833100"
        assert holding.shares == 1000000
        assert holding.market_value == 150000

    def test_holding_filing_relationship(self, session: Session) -> None:
        """Test holding relationship to filing."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        session.add(filing)
        session.commit()

        holding = Holding(
            filing_id=filing.id,
            cusip="037833100",
            security_name="Apple Inc",
            shares=1000000,
            market_value=150000
        )
        session.add(holding)
        session.commit()

        # Navigate relationship
        assert holding.filing.id == filing.id
        assert holding.filing.filer.name == "Test Filer"

        # Navigate reverse relationship
        assert len(filing.holdings) == 1
        assert filing.holdings[0].cusip == "037833100"

    def test_holding_multiple_per_filing(self, session: Session) -> None:
        """Test that a filing can have multiple holdings."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        session.add(filing)
        session.commit()

        holdings = [
            Holding(filing_id=filing.id, cusip="037833100", security_name="Apple Inc", shares=1000000, market_value=150000),
            Holding(filing_id=filing.id, cusip="594918104", security_name="Microsoft Corp", shares=500000, market_value=200000),
            Holding(filing_id=filing.id, cusip="172967424", security_name="Coca-Cola Co", shares=2000000, market_value=100000),
        ]

        session.add_all(holdings)
        session.commit()

        # Verify all holdings are linked
        assert len(filing.holdings) == 3
        cusips = [h.cusip for h in filing.holdings]
        assert "037833100" in cusips
        assert "594918104" in cusips
        assert "172967424" in cusips


class TestPositionChangeModel:
    """Test PositionChange model."""

    def test_create_position_change(self, session: Session) -> None:
        """Test creating a position change record."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        prev_filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-24-001234",
            filing_date=date(2024, 11, 14),
            period_of_report=date(2024, 9, 30)
        )
        curr_filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        session.add_all([prev_filing, curr_filing])
        session.commit()

        position_change = PositionChange(
            filer_id=filer.id,
            cusip="037833100",
            security_name="Apple Inc",
            prev_filing_id=prev_filing.id,
            prev_period=date(2024, 9, 30),
            prev_shares=1000000,
            prev_market_value=150000,
            curr_filing_id=curr_filing.id,
            curr_period=date(2024, 12, 31),
            curr_shares=1200000,
            curr_market_value=180000,
            shares_change=200000,
            shares_change_pct=20.0,
            value_change=30000,
            change_type=ChangeType.INCREASED
        )
        session.add(position_change)
        session.commit()

        assert position_change.id is not None
        assert position_change.change_type == ChangeType.INCREASED
        assert position_change.shares_change == 200000
        assert position_change.shares_change_pct == 20.0

    def test_position_change_enum_values(self, session: Session) -> None:
        """Test ChangeType enum values."""
        assert ChangeType.NEW == "NEW"
        assert ChangeType.CLOSED == "CLOSED"
        assert ChangeType.INCREASED == "INCREASED"
        assert ChangeType.DECREASED == "DECREASED"
        assert ChangeType.UNCHANGED == "UNCHANGED"

    def test_position_change_relationships(self, session: Session) -> None:
        """Test position change relationships."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        prev_filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-24-001234",
            filing_date=date(2024, 11, 14),
            period_of_report=date(2024, 9, 30)
        )
        curr_filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        session.add_all([prev_filing, curr_filing])
        session.commit()

        position_change = PositionChange(
            filer_id=filer.id,
            cusip="037833100",
            security_name="Apple Inc",
            prev_filing_id=prev_filing.id,
            prev_period=date(2024, 9, 30),
            prev_shares=1000000,
            prev_market_value=150000,
            curr_filing_id=curr_filing.id,
            curr_period=date(2024, 12, 31),
            curr_shares=1200000,
            curr_market_value=180000,
            shares_change=200000,
            shares_change_pct=20.0,
            value_change=30000,
            change_type=ChangeType.INCREASED
        )
        session.add(position_change)
        session.commit()

        # Navigate relationships
        assert position_change.filer.name == "Test Filer"
        assert position_change.prev_filing.period_of_report == date(2024, 9, 30)
        assert position_change.curr_filing.period_of_report == date(2024, 12, 31)

        # Navigate reverse relationship
        assert len(filer.position_changes) == 1
        assert filer.position_changes[0].cusip == "037833100"

    def test_position_change_new_position(self, session: Session) -> None:
        """Test NEW position (no previous filing)."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        curr_filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        session.add(curr_filing)
        session.commit()

        position_change = PositionChange(
            filer_id=filer.id,
            cusip="037833100",
            security_name="Apple Inc",
            prev_filing_id=None,
            prev_period=None,
            prev_shares=None,
            prev_market_value=None,
            curr_filing_id=curr_filing.id,
            curr_period=date(2024, 12, 31),
            curr_shares=1000000,
            curr_market_value=150000,
            shares_change=1000000,
            shares_change_pct=None,  # Can't calculate percentage from 0
            value_change=150000,
            change_type=ChangeType.NEW
        )
        session.add(position_change)
        session.commit()

        assert position_change.change_type == ChangeType.NEW
        assert position_change.prev_filing_id is None
        assert position_change.curr_shares == 1000000

    def test_position_change_closed_position(self, session: Session) -> None:
        """Test CLOSED position (no current filing)."""
        filer = Filer(cik="0001067983", name="Test Filer", category="test", enabled=True)
        session.add(filer)
        session.commit()

        prev_filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-24-001234",
            filing_date=date(2024, 11, 14),
            period_of_report=date(2024, 9, 30)
        )
        curr_filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        session.add_all([prev_filing, curr_filing])
        session.commit()

        position_change = PositionChange(
            filer_id=filer.id,
            cusip="037833100",
            security_name="Apple Inc",
            prev_filing_id=prev_filing.id,
            prev_period=date(2024, 9, 30),
            prev_shares=1000000,
            prev_market_value=150000,
            curr_filing_id=curr_filing.id,  # Filing exists, but position doesn't
            curr_period=date(2024, 12, 31),
            curr_shares=None,
            curr_market_value=None,
            shares_change=-1000000,
            shares_change_pct=-100.0,
            value_change=-150000,
            change_type=ChangeType.CLOSED
        )
        session.add(position_change)
        session.commit()

        assert position_change.change_type == ChangeType.CLOSED
        assert position_change.curr_shares is None
        assert position_change.shares_change == -1000000


class TestModelTimestamps:
    """Test timestamp behavior across all models."""

    def test_all_models_have_timestamps(self, session: Session) -> None:
        """Verify all models have created_at and updated_at fields."""
        filer = Filer(cik="0001067983", name="Test", category="test", enabled=True)
        session.add(filer)
        session.commit()

        filing = Filing(
            filer_id=filer.id,
            accession_number="0001193125-25-001234",
            filing_date=date(2025, 2, 14),
            period_of_report=date(2024, 12, 31)
        )
        session.add(filing)
        session.commit()

        holding = Holding(
            filing_id=filing.id,
            cusip="037833100",
            security_name="Apple Inc",
            shares=1000000,
            market_value=150000
        )
        session.add(holding)
        session.commit()

        position_change = PositionChange(
            filer_id=filer.id,
            cusip="037833100",
            security_name="Apple Inc",
            curr_filing_id=filing.id,
            curr_period=date(2024, 12, 31),
            curr_shares=1000000,
            curr_market_value=150000,
            shares_change=1000000,
            value_change=150000,
            change_type=ChangeType.NEW
        )
        session.add(position_change)
        session.commit()

        # All models should have timestamps
        for model in [filer, filing, holding, position_change]:
            assert hasattr(model, 'created_at')
            assert hasattr(model, 'updated_at')
            assert model.created_at is not None
            assert model.updated_at is not None
