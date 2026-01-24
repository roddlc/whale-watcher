"""Position change analysis for calculating quarter-over-quarter changes in holdings."""

from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from whale_watcher.database.models import ChangeType, Filing, Holding, PositionChange
from whale_watcher.utils.logger import get_logger

logger = get_logger(__name__)


def get_previous_filing(session: Session, current_filing: Filing) -> Optional[Filing]:
    """
    Get the most recent filing before the current one for the same filer.

    Args:
        session: Database session
        current_filing: The current filing to find previous filing for

    Returns:
        Previous Filing or None if this is the first filing for this filer
    """
    previous = (
        session.query(Filing)
        .filter(Filing.filer_id == current_filing.filer_id)
        .filter(Filing.period_of_report < current_filing.period_of_report)
        .order_by(Filing.period_of_report.desc())
        .first()
    )

    return previous


def classify_change_type(prev_shares: Optional[int], curr_shares: Optional[int]) -> ChangeType:
    """
    Classify the type of position change based on share counts.

    Args:
        prev_shares: Share count in previous quarter (None if position didn't exist)
        curr_shares: Share count in current quarter (None if position was closed)

    Returns:
        ChangeType enum value
    """
    if prev_shares is None and curr_shares is not None and curr_shares > 0:
        return ChangeType.NEW
    if prev_shares is not None and prev_shares > 0 and curr_shares is None:
        return ChangeType.CLOSED
    if curr_shares is not None and prev_shares is not None:
        if curr_shares > prev_shares:
            return ChangeType.INCREASED
        elif curr_shares < prev_shares:
            return ChangeType.DECREASED
        else:
            return ChangeType.UNCHANGED

    # Fallback (shouldn't happen with valid data)
    return ChangeType.UNCHANGED


def calculate_percentage_change(prev_value: int, curr_value: int) -> Optional[float]:
    """
    Calculate percentage change, safely handling division by zero.

    Args:
        prev_value: Previous value
        curr_value: Current value

    Returns:
        Percentage change as float, or None if prev_value is 0
    """
    if prev_value == 0:
        return None

    return ((curr_value - prev_value) / prev_value) * 100


def calculate_position_changes(session: Session, filing_id: int) -> int:
    """
    Calculate quarter-over-quarter position changes for a filing.

    This function:
    1. Finds the previous quarter filing for the same filer
    2. Matches holdings by CUSIP between current and previous
    3. Calculates deltas and classifies change types
    4. Bulk inserts PositionChange records

    This function is idempotent - if position changes already exist for this
    filing, they will be deleted and recalculated.

    Args:
        session: Database session (caller manages transaction)
        filing_id: ID of the filing to analyze

    Returns:
        Number of position changes created

    Raises:
        ValueError: If filing doesn't exist
    """
    # Get current filing
    current_filing = session.query(Filing).filter(Filing.id == filing_id).first()

    if current_filing is None:
        raise ValueError(f"Filing not found with id={filing_id}")

    logger.info(
        f"Calculating position changes for filing_id={filing_id} "
        f"(period={current_filing.period_of_report})"
    )

    # Check for existing position changes and delete if found (idempotency)
    existing_count = (
        session.query(PositionChange)
        .filter(PositionChange.curr_filing_id == filing_id)
        .count()
    )

    if existing_count > 0:
        logger.info(f"Deleting {existing_count} existing position changes for idempotency")
        session.query(PositionChange).filter(
            PositionChange.curr_filing_id == filing_id
        ).delete()
        session.flush()

    # Get previous filing
    previous_filing = get_previous_filing(session, current_filing)

    if previous_filing is None:
        logger.info("No previous filing found - all positions will be marked as NEW")

    # Get current holdings
    current_holdings = (
        session.query(Holding)
        .filter(Holding.filing_id == filing_id)
        .all()
    )

    logger.info(f"Found {len(current_holdings)} current holdings")

    # Build lookup dict for previous holdings by CUSIP
    prev_holdings_by_cusip: Dict[str, Holding] = {}
    if previous_filing is not None:
        prev_holdings = (
            session.query(Holding)
            .filter(Holding.filing_id == previous_filing.id)
            .all()
        )
        prev_holdings_by_cusip = {h.cusip: h for h in prev_holdings}
        logger.info(f"Found {len(prev_holdings_by_cusip)} previous holdings")

    # Track CUSIPs we've seen in current holdings
    current_cusips = set()

    # Create position changes for current holdings
    position_changes: List[PositionChange] = []

    for curr_holding in current_holdings:
        current_cusips.add(curr_holding.cusip)
        prev_holding = prev_holdings_by_cusip.get(curr_holding.cusip)

        # Extract values
        curr_shares = curr_holding.shares
        curr_market_value = curr_holding.market_value
        prev_shares = prev_holding.shares if prev_holding else None
        prev_market_value = prev_holding.market_value if prev_holding else None

        # Calculate changes
        shares_change = curr_shares - (prev_shares or 0)
        value_change = curr_market_value - (prev_market_value or 0)

        # Calculate percentage change (None for NEW positions)
        shares_change_pct = None
        if prev_shares is not None and prev_shares > 0:
            shares_change_pct = calculate_percentage_change(prev_shares, curr_shares)

        # Classify change type
        change_type = classify_change_type(prev_shares, curr_shares)

        # Create PositionChange record
        position_change = PositionChange(
            filer_id=current_filing.filer_id,
            cusip=curr_holding.cusip,
            security_name=curr_holding.security_name,
            prev_filing_id=previous_filing.id if previous_filing else None,
            prev_period=previous_filing.period_of_report if previous_filing else None,
            prev_shares=prev_shares,
            prev_market_value=prev_market_value,
            curr_filing_id=current_filing.id,
            curr_period=current_filing.period_of_report,
            curr_shares=curr_shares,
            curr_market_value=curr_market_value,
            shares_change=shares_change,
            shares_change_pct=shares_change_pct,
            value_change=value_change,
            change_type=change_type,
        )

        position_changes.append(position_change)

    # Handle CLOSED positions (existed in previous but not in current)
    if previous_filing is not None:
        for cusip, prev_holding in prev_holdings_by_cusip.items():
            if cusip not in current_cusips:
                # Position was closed
                shares_change = -prev_holding.shares
                value_change = -prev_holding.market_value

                position_change = PositionChange(
                    filer_id=current_filing.filer_id,
                    cusip=prev_holding.cusip,
                    security_name=prev_holding.security_name,
                    prev_filing_id=previous_filing.id,
                    prev_period=previous_filing.period_of_report,
                    prev_shares=prev_holding.shares,
                    prev_market_value=prev_holding.market_value,
                    curr_filing_id=current_filing.id,
                    curr_period=current_filing.period_of_report,
                    curr_shares=None,
                    curr_market_value=None,
                    shares_change=shares_change,
                    shares_change_pct=None,  # Can't calculate % for closed positions
                    value_change=value_change,
                    change_type=ChangeType.CLOSED,
                )

                position_changes.append(position_change)

    # Bulk insert position changes
    session.add_all(position_changes)
    session.flush()

    logger.info(f"Created {len(position_changes)} position changes for filing_id={filing_id}")

    # Log summary by change type
    change_type_counts = {}
    for pc in position_changes:
        change_type_counts[pc.change_type] = change_type_counts.get(pc.change_type, 0) + 1

    for change_type, count in sorted(change_type_counts.items()):
        logger.info(f"  {change_type.value}: {count}")

    return len(position_changes)
