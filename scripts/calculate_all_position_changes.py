"""
Calculate position changes for all existing filings in the database.

This script processes all filings and calculates quarter-over-quarter position changes.
Run this after populating the database with filings to analyze position changes.
"""

from whale_watcher.config import load_config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.models import Filer, Filing, PositionChange
from whale_watcher.etl.analyzer import calculate_position_changes
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging(level='INFO')
logger = get_logger(__name__)


def main() -> None:
    """Calculate position changes for all filings."""
    config = load_config()
    db = DatabaseConnection(config.database_url)

    with db.session_scope() as session:
        # Get all filers
        filers = session.query(Filer).all()

        logger.info(f"Processing {len(filers)} filers...")

        total_changes = 0

        for filer in filers:
            logger.info(f"\n{'=' * 80}")
            logger.info(f"Processing: {filer.name}")
            logger.info(f"{'=' * 80}")

            # Get all filings for this filer, ordered by period
            filings = (
                session.query(Filing)
                .filter(Filing.filer_id == filer.id)
                .order_by(Filing.period_of_report)
                .all()
            )

            logger.info(f"Found {len(filings)} filings")

            # Calculate position changes for each filing
            for filing in filings:
                logger.info(
                    f"\nAnalyzing filing: {filing.accession_number[:20]}... "
                    f"(period={filing.period_of_report})"
                )

                changes_count = calculate_position_changes(session, filing.id)
                total_changes += changes_count

        logger.info(f"\n{'=' * 80}")
        logger.info("SUMMARY")
        logger.info(f"{'=' * 80}")

        # Count position changes by type
        for filer in filers:
            changes = (
                session.query(PositionChange)
                .filter(PositionChange.filer_id == filer.id)
                .all()
            )

            if changes:
                change_counts = {}
                for change in changes:
                    change_type = change.change_type.value
                    change_counts[change_type] = change_counts.get(change_type, 0) + 1

                logger.info(f"\n{filer.name}:")
                logger.info(f"  Total changes: {len(changes)}")
                for change_type, count in sorted(change_counts.items()):
                    logger.info(f"    {change_type}: {count}")

        logger.info(f"\nTotal position changes calculated: {total_changes}")


if __name__ == "__main__":
    main()
