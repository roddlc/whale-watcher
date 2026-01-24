"""Check the raw database values for Apple holdings."""

from whale_watcher.config import load_config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.models import Filer, Filing, Holding
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging(level='INFO')
logger = get_logger(__name__)


def main() -> None:
    """Show raw Apple holdings data."""
    config = load_config()
    db = DatabaseConnection(config.database_url)

    with db.session_scope() as session:
        # Get Apple holdings for Berkshire
        holdings = (
            session.query(Holding, Filing)
            .join(Filing, Holding.filing_id == Filing.id)
            .join(Filer, Filing.filer_id == Filer.id)
            .filter(Filer.name == "Berkshire Hathaway")
            .filter(Holding.security_name == "APPLE INC")
            .order_by(Filing.period_of_report)
            .all()
        )

        logger.info("=" * 100)
        logger.info("APPLE INC - Raw Database Values")
        logger.info("=" * 100)

        for holding, filing in holdings:
            price_per_share = holding.market_value / holding.shares
            logger.info(f"\nQuarter: {filing.period_of_report}")
            logger.info(f"  Shares:       {holding.shares:,}")
            logger.info(f"  Market value: ${holding.market_value:,}")
            logger.info(f"  Price change per share:  ${price_per_share:,.2f}")


if __name__ == "__main__":
    main()
