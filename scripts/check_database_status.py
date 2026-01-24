"""Quick script to check database population status."""

from whale_watcher.config import load_config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.models import Filer, Filing, Holding, PositionChange
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging(level='INFO')
logger = get_logger(__name__)


def main() -> None:
    """Check database population status."""
    config = load_config()
    db = DatabaseConnection(config.database_url)

    with db.session_scope() as session:
        # Count filers
        filer_count = session.query(Filer).count()
        logger.info(f"Filers: {filer_count}")

        # Count filings per filer
        filers = session.query(Filer).all()
        for filer in filers:
            filing_count = session.query(Filing).filter(Filing.filer_id == filer.id).count()
            holdings_count = (
                session.query(Holding)
                .join(Filing)
                .filter(Filing.filer_id == filer.id)
                .count()
            )
            logger.info(f"  {filer.name}: {filing_count} filings, {holdings_count} total holdings")

            # Show filing details
            filings = (
                session.query(Filing)
                .filter(Filing.filer_id == filer.id)
                .order_by(Filing.period_of_report)
                .all()
            )
            for filing in filings:
                holdings = session.query(Holding).filter(Holding.filing_id == filing.id).count()
                logger.info(
                    f"    {filing.period_of_report} - "
                    f"{filing.accession_number[:20]}... - "
                    f"{holdings} holdings, "
                    f"${filing.total_value:,} total value, "
                    f"processed={filing.processed}"
                )

        # Count total holdings
        total_holdings = session.query(Holding).count()
        logger.info(f"\nTotal holdings across all filings: {total_holdings}")

        # Count position changes
        position_changes = session.query(PositionChange).count()
        logger.info(f"Position changes: {position_changes}")


if __name__ == "__main__":
    main()
