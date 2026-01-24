"""Data loading functions for inserting parsed 13F holdings into database."""

from typing import List

from sqlalchemy.orm import Session

from whale_watcher.database.models import Filing, Holding
from whale_watcher.etl.parser import FilingSummary, HoldingData
from whale_watcher.utils.logger import get_logger

logger = get_logger(__name__)


def load_holdings(session: Session, filing_id: int, holdings: List[HoldingData]) -> None:
    """
    Bulk insert holdings into database for a filing.

    Args:
        session: Active database session (caller manages transaction)
        filing_id: ID of the filing these holdings belong to
        holdings: List of parsed holding data to insert

    Raises:
        Exception: Propagates any database errors (caller handles rollback)
    """
    logger.info(f"Loading {len(holdings)} holdings for filing_id={filing_id}")

    # Create Holding model instances from HoldingData
    holdings_list = [
        Holding(
            filing_id=filing_id,
            cusip=holding.cusip,
            security_name=holding.security_name,
            shares=holding.shares,
            market_value=holding.market_value,
            voting_authority_sole=holding.voting_authority_sole,
            voting_authority_shared=holding.voting_authority_shared,
            voting_authority_none=holding.voting_authority_none,
            # discretion field left as NULL (not in HoldingData)
        )
        for holding in holdings
    ]

    # Bulk insert
    session.add_all(holdings_list)
    session.flush()

    logger.info(f"Successfully loaded {len(holdings)} holdings for filing_id={filing_id}")


def update_filing_summary(session: Session, filing_id: int, summary: FilingSummary) -> None:
    """
    Update filing record with summary data and mark as processed.

    Args:
        session: Active database session (caller manages transaction)
        filing_id: ID of the filing to update
        summary: Parsed filing summary with total_value and holdings_count

    Raises:
        ValueError: If filing with given ID does not exist
        Exception: Propagates any database errors (caller handles rollback)
    """
    # Query for filing
    filing = session.query(Filing).filter(Filing.id == filing_id).first()

    if filing is None:
        raise ValueError(f"Filing not found with id={filing_id}")

    # Update summary fields
    filing.total_value = summary.total_value
    filing.holdings_count = summary.holdings_count
    filing.processed = True

    session.flush()

    logger.info(
        f"Updated filing_id={filing_id}: "
        f"total_value=${summary.total_value:,}, "
        f"holdings_count={summary.holdings_count}, "
        f"processed=True"
    )
