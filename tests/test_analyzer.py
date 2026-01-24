"""Tests for position change analysis module."""

from datetime import date

import pytest
from sqlalchemy.orm import Session

from whale_watcher.database.models import ChangeType, Filer, Filing, Holding, PositionChange
from whale_watcher.etl.analyzer import (
    calculate_percentage_change,
    calculate_position_changes,
    classify_change_type,
    get_previous_filing,
)


class TestGetPreviousFiling:
    """Tests for get_previous_filing function."""

    def test_no_previous_filing(self, db_session: Session) -> None:
        """Test when there is no previous filing (first filing for filer)."""
        filer = Filer(cik="0001234567", name="Test Filer", category="test", enabled=True)
        db_session.add(filer)
        db_session.flush()

        filing = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000001",
            filing_date=date(2025, 5, 15),
            period_of_report=date(2025, 3, 31),
        )
        db_session.add(filing)
        db_session.flush()

        previous = get_previous_filing(db_session, filing)

        assert previous is None

    def test_with_previous_filing(self, db_session: Session) -> None:
        """Test getting the most recent previous filing."""
        filer = Filer(cik="0001234567", name="Test Filer", category="test", enabled=True)
        db_session.add(filer)
        db_session.flush()

        # Create Q1 filing
        filing_q1 = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000001",
            filing_date=date(2025, 5, 15),
            period_of_report=date(2025, 3, 31),
        )
        db_session.add(filing_q1)

        # Create Q2 filing
        filing_q2 = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000002",
            filing_date=date(2025, 8, 15),
            period_of_report=date(2025, 6, 30),
        )
        db_session.add(filing_q2)
        db_session.flush()

        previous = get_previous_filing(db_session, filing_q2)

        assert previous is not None
        assert previous.id == filing_q1.id
        assert previous.period_of_report == date(2025, 3, 31)

    def test_multiple_previous_filings(self, db_session: Session) -> None:
        """Test that it returns the most recent previous filing."""
        filer = Filer(cik="0001234567", name="Test Filer", category="test", enabled=True)
        db_session.add(filer)
        db_session.flush()

        # Create Q1, Q2, Q3 filings
        filing_q1 = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000001",
            filing_date=date(2025, 5, 15),
            period_of_report=date(2025, 3, 31),
        )
        filing_q2 = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000002",
            filing_date=date(2025, 8, 15),
            period_of_report=date(2025, 6, 30),
        )
        filing_q3 = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000003",
            filing_date=date(2025, 11, 15),
            period_of_report=date(2025, 9, 30),
        )
        db_session.add_all([filing_q1, filing_q2, filing_q3])
        db_session.flush()

        # Q3 should find Q2 as previous (not Q1)
        previous = get_previous_filing(db_session, filing_q3)

        assert previous is not None
        assert previous.id == filing_q2.id
        assert previous.period_of_report == date(2025, 6, 30)

    def test_different_filer_ignored(self, db_session: Session) -> None:
        """Test that filings from other filers are ignored."""
        filer1 = Filer(cik="0001111111", name="Filer 1", category="test", enabled=True)
        filer2 = Filer(cik="0002222222", name="Filer 2", category="test", enabled=True)
        db_session.add_all([filer1, filer2])
        db_session.flush()

        # Filer 1 Q1 filing
        filing_f1_q1 = Filing(
            filer_id=filer1.id,
            accession_number="0001111111-25-000001",
            filing_date=date(2025, 5, 15),
            period_of_report=date(2025, 3, 31),
        )
        # Filer 2 Q2 filing
        filing_f2_q2 = Filing(
            filer_id=filer2.id,
            accession_number="0002222222-25-000001",
            filing_date=date(2025, 8, 15),
            period_of_report=date(2025, 6, 30),
        )
        db_session.add_all([filing_f1_q1, filing_f2_q2])
        db_session.flush()

        # Filer 2 Q2 should have no previous (Filer 1's filing doesn't count)
        previous = get_previous_filing(db_session, filing_f2_q2)

        assert previous is None


class TestClassifyChangeType:
    """Tests for classify_change_type function."""

    def test_new_position(self) -> None:
        """Test classification of new positions."""
        assert classify_change_type(None, 1000) == ChangeType.NEW
        assert classify_change_type(None, 500000) == ChangeType.NEW

    def test_closed_position(self) -> None:
        """Test classification of closed positions."""
        assert classify_change_type(1000, None) == ChangeType.CLOSED
        assert classify_change_type(500000, None) == ChangeType.CLOSED

    def test_increased_position(self) -> None:
        """Test classification of increased positions."""
        assert classify_change_type(1000, 2000) == ChangeType.INCREASED
        assert classify_change_type(100, 150) == ChangeType.INCREASED

    def test_decreased_position(self) -> None:
        """Test classification of decreased positions."""
        assert classify_change_type(2000, 1000) == ChangeType.DECREASED
        assert classify_change_type(150, 100) == ChangeType.DECREASED

    def test_unchanged_position(self) -> None:
        """Test classification of unchanged positions."""
        assert classify_change_type(1000, 1000) == ChangeType.UNCHANGED
        assert classify_change_type(500000, 500000) == ChangeType.UNCHANGED


class TestCalculatePercentageChange:
    """Tests for calculate_percentage_change function."""

    def test_positive_change(self) -> None:
        """Test calculating positive percentage change."""
        result = calculate_percentage_change(100, 150)
        assert result == 50.0

    def test_negative_change(self) -> None:
        """Test calculating negative percentage change."""
        result = calculate_percentage_change(150, 100)
        assert result == pytest.approx(-33.333, rel=0.01)

    def test_no_change(self) -> None:
        """Test when there's no change."""
        result = calculate_percentage_change(100, 100)
        assert result == 0.0

    def test_division_by_zero(self) -> None:
        """Test handling of division by zero."""
        result = calculate_percentage_change(0, 100)
        assert result is None

    def test_large_values(self) -> None:
        """Test with large values (millions of shares)."""
        result = calculate_percentage_change(1_000_000, 1_500_000)
        assert result == 50.0


class TestCalculatePositionChanges:
    """Tests for calculate_position_changes function."""

    def test_first_filing_all_new(self, db_session: Session) -> None:
        """Test first filing for a filer - all positions should be NEW."""
        # Setup filer and filing
        filer = Filer(cik="0001234567", name="Test Filer", category="test", enabled=True)
        db_session.add(filer)
        db_session.flush()

        filing = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000001",
            filing_date=date(2025, 5, 15),
            period_of_report=date(2025, 3, 31),
        )
        db_session.add(filing)
        db_session.flush()

        # Add holdings
        holdings = [
            Holding(
                filing_id=filing.id,
                cusip="037833100",
                security_name="APPLE INC",
                shares=100000,
                market_value=20000000,
            ),
            Holding(
                filing_id=filing.id,
                cusip="594918104",
                security_name="MICROSOFT CORP",
                shares=50000,
                market_value=15000000,
            ),
        ]
        db_session.add_all(holdings)
        db_session.flush()

        # Calculate position changes
        count = calculate_position_changes(db_session, filing.id)

        assert count == 2

        # Verify changes
        changes = db_session.query(PositionChange).all()
        assert len(changes) == 2

        for change in changes:
            assert change.change_type == ChangeType.NEW
            assert change.prev_filing_id is None
            assert change.prev_period is None
            assert change.prev_shares is None
            assert change.prev_market_value is None
            assert change.curr_filing_id == filing.id
            assert change.curr_shares is not None
            assert change.shares_change == change.curr_shares
            assert change.shares_change_pct is None  # No percentage for NEW

    def test_second_filing_mixed_changes(self, db_session: Session) -> None:
        """Test second filing with NEW, CLOSED, INCREASED, DECREASED, UNCHANGED."""
        # Setup filer
        filer = Filer(cik="0001234567", name="Test Filer", category="test", enabled=True)
        db_session.add(filer)
        db_session.flush()

        # Q1 filing
        filing_q1 = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000001",
            filing_date=date(2025, 5, 15),
            period_of_report=date(2025, 3, 31),
        )
        db_session.add(filing_q1)
        db_session.flush()

        # Q1 holdings
        holdings_q1 = [
            Holding(filing_id=filing_q1.id, cusip="111111111", security_name="STOCK A",
                   shares=100000, market_value=10000000),  # Will INCREASE
            Holding(filing_id=filing_q1.id, cusip="222222222", security_name="STOCK B",
                   shares=50000, market_value=5000000),    # Will DECREASE
            Holding(filing_id=filing_q1.id, cusip="333333333", security_name="STOCK C",
                   shares=75000, market_value=7500000),    # Will be CLOSED
            Holding(filing_id=filing_q1.id, cusip="444444444", security_name="STOCK D",
                   shares=25000, market_value=2500000),    # UNCHANGED
        ]
        db_session.add_all(holdings_q1)
        db_session.flush()

        # Q2 filing
        filing_q2 = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000002",
            filing_date=date(2025, 8, 15),
            period_of_report=date(2025, 6, 30),
        )
        db_session.add(filing_q2)
        db_session.flush()

        # Q2 holdings
        holdings_q2 = [
            Holding(filing_id=filing_q2.id, cusip="111111111", security_name="STOCK A",
                   shares=150000, market_value=15000000),  # INCREASED
            Holding(filing_id=filing_q2.id, cusip="222222222", security_name="STOCK B",
                   shares=30000, market_value=3000000),    # DECREASED
            # 333333333 CLOSED (not present)
            Holding(filing_id=filing_q2.id, cusip="444444444", security_name="STOCK D",
                   shares=25000, market_value=2500000),    # UNCHANGED
            Holding(filing_id=filing_q2.id, cusip="555555555", security_name="STOCK E",
                   shares=40000, market_value=4000000),    # NEW
        ]
        db_session.add_all(holdings_q2)
        db_session.flush()

        # Calculate position changes for Q2
        count = calculate_position_changes(db_session, filing_q2.id)

        assert count == 5  # 4 current + 1 closed

        # Verify changes
        changes = db_session.query(PositionChange).filter(
            PositionChange.curr_filing_id == filing_q2.id
        ).all()
        assert len(changes) == 5

        # Check each change type
        changes_by_cusip = {c.cusip: c for c in changes}

        # INCREASED
        increased = changes_by_cusip["111111111"]
        assert increased.change_type == ChangeType.INCREASED
        assert increased.prev_shares == 100000
        assert increased.curr_shares == 150000
        assert increased.shares_change == 50000
        assert increased.shares_change_pct == 50.0

        # DECREASED
        decreased = changes_by_cusip["222222222"]
        assert decreased.change_type == ChangeType.DECREASED
        assert decreased.prev_shares == 50000
        assert decreased.curr_shares == 30000
        assert decreased.shares_change == -20000
        assert decreased.shares_change_pct == pytest.approx(-40.0, rel=0.01)

        # CLOSED
        closed = changes_by_cusip["333333333"]
        assert closed.change_type == ChangeType.CLOSED
        assert closed.prev_shares == 75000
        assert closed.curr_shares is None
        assert closed.shares_change == -75000
        assert closed.shares_change_pct is None

        # UNCHANGED
        unchanged = changes_by_cusip["444444444"]
        assert unchanged.change_type == ChangeType.UNCHANGED
        assert unchanged.prev_shares == 25000
        assert unchanged.curr_shares == 25000
        assert unchanged.shares_change == 0
        assert unchanged.shares_change_pct == 0.0

        # NEW
        new = changes_by_cusip["555555555"]
        assert new.change_type == ChangeType.NEW
        assert new.prev_shares is None
        assert new.curr_shares == 40000
        assert new.shares_change == 40000
        assert new.shares_change_pct is None

    def test_idempotency(self, db_session: Session) -> None:
        """Test that running analysis twice produces same result."""
        # Setup filer and filing
        filer = Filer(cik="0001234567", name="Test Filer", category="test", enabled=True)
        db_session.add(filer)
        db_session.flush()

        filing = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000001",
            filing_date=date(2025, 5, 15),
            period_of_report=date(2025, 3, 31),
        )
        db_session.add(filing)
        db_session.flush()

        holding = Holding(
            filing_id=filing.id,
            cusip="037833100",
            security_name="APPLE INC",
            shares=100000,
            market_value=20000000,
        )
        db_session.add(holding)
        db_session.flush()

        # First calculation
        count1 = calculate_position_changes(db_session, filing.id)
        assert count1 == 1

        # Second calculation (should delete and recreate)
        count2 = calculate_position_changes(db_session, filing.id)
        assert count2 == 1

        # Should only have 1 position change total
        total_changes = db_session.query(PositionChange).count()
        assert total_changes == 1

    def test_filing_not_found(self, db_session: Session) -> None:
        """Test error handling when filing doesn't exist."""
        with pytest.raises(ValueError, match="Filing not found"):
            calculate_position_changes(db_session, 99999)

    def test_division_by_zero_edge_case(self, db_session: Session) -> None:
        """Test handling when previous shares is 0 (shouldn't happen in real data)."""
        # Setup
        filer = Filer(cik="0001234567", name="Test Filer", category="test", enabled=True)
        db_session.add(filer)
        db_session.flush()

        filing_q1 = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000001",
            filing_date=date(2025, 5, 15),
            period_of_report=date(2025, 3, 31),
        )
        db_session.add(filing_q1)
        db_session.flush()

        # Edge case: 0 shares in Q1 (shouldn't happen but testing safety)
        holding_q1 = Holding(
            filing_id=filing_q1.id,
            cusip="037833100",
            security_name="APPLE INC",
            shares=0,
            market_value=0,
        )
        db_session.add(holding_q1)
        db_session.flush()

        filing_q2 = Filing(
            filer_id=filer.id,
            accession_number="0001234567-25-000002",
            filing_date=date(2025, 8, 15),
            period_of_report=date(2025, 6, 30),
        )
        db_session.add(filing_q2)
        db_session.flush()

        holding_q2 = Holding(
            filing_id=filing_q2.id,
            cusip="037833100",
            security_name="APPLE INC",
            shares=100000,
            market_value=20000000,
        )
        db_session.add(holding_q2)
        db_session.flush()

        # Calculate changes
        count = calculate_position_changes(db_session, filing_q2.id)
        assert count == 1

        change = db_session.query(PositionChange).first()
        assert change.shares_change_pct is None  # Should be None due to div by zero protection
