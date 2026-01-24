"""
Sample queries demonstrating position change analysis capabilities.

This script shows the types of insights you can extract from the position_changes table.
"""

from datetime import date

from sqlalchemy import and_, desc, func

from whale_watcher.config import load_config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.models import ChangeType, Filer, PositionChange
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging(level='INFO')
logger = get_logger(__name__)


def query_new_positions(db: DatabaseConnection, filer_name: str, period: date) -> None:
    """Show new positions opened in a specific quarter."""
    logger.info(f"\n{'=' * 80}")
    logger.info(f"NEW POSITIONS: {filer_name} - {period}")
    logger.info(f"{'=' * 80}")

    with db.session_scope() as session:
        results = (
            session.query(PositionChange)
            .join(Filer, PositionChange.filer_id == Filer.id)
            .filter(Filer.name == filer_name)
            .filter(PositionChange.curr_period == period)
            .filter(PositionChange.change_type == ChangeType.NEW)
            .order_by(desc(PositionChange.curr_market_value))
            .limit(10)
            .all()
        )

        if not results:
            logger.info("No new positions found")
            return

        logger.info(f"Top 10 new positions by value:")
        for i, pc in enumerate(results, 1):
            logger.info(
                f"{i:2d}. {pc.security_name[:40]:40s} | "
                f"Shares: {pc.curr_shares:>12,} | "
                f"Value: ${pc.curr_market_value:>15,}"
            )


def query_closed_positions(db: DatabaseConnection, filer_name: str, period: date) -> None:
    """Show positions closed in a specific quarter."""
    logger.info(f"\n{'=' * 80}")
    logger.info(f"CLOSED POSITIONS: {filer_name} - {period}")
    logger.info(f"{'=' * 80}")

    with db.session_scope() as session:
        results = (
            session.query(PositionChange)
            .join(Filer, PositionChange.filer_id == Filer.id)
            .filter(Filer.name == filer_name)
            .filter(PositionChange.curr_period == period)
            .filter(PositionChange.change_type == ChangeType.CLOSED)
            .order_by(desc(PositionChange.prev_market_value))
            .limit(10)
            .all()
        )

        if not results:
            logger.info("No closed positions found")
            return

        logger.info(f"Top 10 closed positions by previous value:")
        for i, pc in enumerate(results, 1):
            logger.info(
                f"{i:2d}. {pc.security_name[:40]:40s} | "
                f"Shares: {pc.prev_shares:>12,} | "
                f"Value: ${pc.prev_market_value:>15,}"
            )


def query_biggest_increases(db: DatabaseConnection, filer_name: str, period: date) -> None:
    """Show biggest position increases by percentage."""
    logger.info(f"\n{'=' * 80}")
    logger.info(f"BIGGEST INCREASES: {filer_name} - {period}")
    logger.info(f"{'=' * 80}")

    with db.session_scope() as session:
        results = (
            session.query(PositionChange)
            .join(Filer, PositionChange.filer_id == Filer.id)
            .filter(Filer.name == filer_name)
            .filter(PositionChange.curr_period == period)
            .filter(PositionChange.change_type == ChangeType.INCREASED)
            .filter(PositionChange.shares_change_pct.isnot(None))
            .order_by(desc(PositionChange.shares_change_pct))
            .limit(10)
            .all()
        )

        if not results:
            logger.info("No increased positions found")
            return

        logger.info(f"Top 10 increases by percentage:")
        for i, pc in enumerate(results, 1):
            logger.info(
                f"{i:2d}. {pc.security_name[:40]:40s} | "
                f"From {pc.prev_shares:>12,} to {pc.curr_shares:>12,} | "
                f"+{pc.shares_change_pct:>6.1f}%"
            )


def query_consensus_buys(db: DatabaseConnection, period: date, min_whales: int = 2) -> None:
    """Find stocks that multiple whales bought in the same quarter."""
    logger.info(f"\n{'=' * 80}")
    logger.info(f"CONSENSUS BUYS: {period} (min {min_whales} whales)")
    logger.info(f"{'=' * 80}")

    with db.session_scope() as session:
        # Find CUSIPs with NEW or INCREASED from multiple filers
        results = (
            session.query(
                PositionChange.cusip,
                PositionChange.security_name,
                func.count(func.distinct(PositionChange.filer_id)).label('whale_count'),
                func.sum(PositionChange.shares_change).label('total_shares_added')
            )
            .filter(PositionChange.curr_period == period)
            .filter(PositionChange.change_type.in_([ChangeType.NEW, ChangeType.INCREASED]))
            .group_by(PositionChange.cusip, PositionChange.security_name)
            .having(func.count(func.distinct(PositionChange.filer_id)) >= min_whales)
            .order_by(desc('whale_count'), desc('total_shares_added'))
            .limit(15)
            .all()
        )

        if not results:
            logger.info(f"No consensus buys found (min {min_whales} whales)")
            return

        logger.info(f"Stocks bought by {min_whales}+ whales:")
        for i, (cusip, name, whale_count, total_shares) in enumerate(results, 1):
            logger.info(
                f"{i:2d}. {name[:40]:40s} | "
                f"{whale_count} whales | "
                f"{total_shares:>15,} shares added"
            )


def query_largest_value_changes(db: DatabaseConnection, filer_name: str, period: date) -> None:
    """Show largest absolute value changes (increases or decreases)."""
    logger.info(f"\n{'=' * 80}")
    logger.info(f"LARGEST VALUE CHANGES: {filer_name} - {period}")
    logger.info(f"{'=' * 80}")

    with db.session_scope() as session:
        results = (
            session.query(PositionChange)
            .join(Filer, PositionChange.filer_id == Filer.id)
            .filter(Filer.name == filer_name)
            .filter(PositionChange.curr_period == period)
            .filter(PositionChange.change_type.in_([ChangeType.INCREASED, ChangeType.DECREASED]))
            .order_by(desc(func.abs(PositionChange.value_change)))
            .limit(10)
            .all()
        )

        if not results:
            logger.info("No value changes found")
            return

        logger.info(f"Top 10 largest value changes:")
        for i, pc in enumerate(results, 1):
            direction = "+" if pc.value_change > 0 else ""
            logger.info(
                f"{i:2d}. {pc.security_name[:40]:40s} | "
                f"{direction}${pc.value_change:>15,} | "
                f"{pc.change_type.value}"
            )


def main() -> None:
    """Run sample queries."""
    config = load_config()
    db = DatabaseConnection(config.database_url)

    # Q3 2025
    period_q3 = date(2025, 9, 30)

    # Sample queries
    query_new_positions(db, "Berkshire Hathaway", period_q3)
    query_closed_positions(db, "Berkshire Hathaway", period_q3)
    query_biggest_increases(db, "Berkshire Hathaway", period_q3)
    query_largest_value_changes(db, "Berkshire Hathaway", period_q3)
    query_consensus_buys(db, period_q3, min_whales=2)

    logger.info(f"\n{'=' * 80}")
    logger.info("Sample queries complete!")
    logger.info(f"{'=' * 80}")


if __name__ == "__main__":
    main()
