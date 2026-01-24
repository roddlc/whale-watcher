"""Investigate the Apple position change to understand the value increase despite share decrease."""

from datetime import date

from whale_watcher.config import load_config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.models import Filer, PositionChange
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging(level='INFO')
logger = get_logger(__name__)


def main() -> None:
    """Show detailed Apple position changes."""
    config = load_config()
    db = DatabaseConnection(config.database_url)

    with db.session_scope() as session:
        # Get Apple position changes for Berkshire
        apple_changes = (
            session.query(PositionChange)
            .join(Filer, PositionChange.filer_id == Filer.id)
            .filter(Filer.name == "Berkshire Hathaway")
            .filter(PositionChange.security_name == "APPLE INC")
            .order_by(PositionChange.curr_period)
            .all()
        )

        logger.info("=" * 100)
        logger.info("APPLE INC - Berkshire Hathaway Position Changes")
        logger.info("=" * 100)

        for pc in apple_changes:
            logger.info(f"\nQuarter: {pc.curr_period}")
            logger.info(f"  Change Type: {pc.change_type.value}")

            if pc.prev_shares is not None:
                # Calculate implied price per share (values are in dollars)
                prev_price_per_share = pc.prev_market_value / pc.prev_shares
                curr_price_per_share = pc.curr_market_value / pc.curr_shares
                price_change_pct = ((curr_price_per_share - prev_price_per_share) / prev_price_per_share) * 100

                logger.info(f"\n  Previous Quarter ({pc.prev_period}):")
                logger.info(f"    Shares:      {pc.prev_shares:>15,}")
                logger.info(f"    Value:       ${pc.prev_market_value:>15,}")
                logger.info(f"    Price/Share: ${prev_price_per_share:>15,.2f}")

                logger.info(f"\n  Current Quarter ({pc.curr_period}):")
                logger.info(f"    Shares:      {pc.curr_shares:>15,}")
                logger.info(f"    Value:       ${pc.curr_market_value:>15,}")
                logger.info(f"    Price/Share: ${curr_price_per_share:>15,.2f}")

                logger.info(f"\n  Changes:")
                logger.info(f"    Shares:      {pc.shares_change:>15,} ({pc.shares_change_pct:>+6.1f}%)")
                logger.info(f"    Value:       ${pc.value_change:>15,}")
                logger.info(f"    Price/Share: ${curr_price_per_share - prev_price_per_share:>15,.2f} ({price_change_pct:>+6.1f}%)")

                logger.info(f"\n  Explanation:")
                if pc.shares_change < 0 and pc.value_change > 0:
                    logger.info(f"    Berkshire SOLD {abs(pc.shares_change):,} shares ({abs(pc.shares_change_pct):.1f}% reduction)")
                    logger.info(f"    BUT Apple's stock price rose {price_change_pct:+.1f}%")
                    logger.info(f"    Result: Remaining position worth ${pc.value_change:,} MORE despite selling shares")
            else:
                logger.info(f"  New position: {pc.curr_shares:,} shares worth ${pc.curr_market_value:,}")

        # Same for Bank of America
        logger.info("\n" + "=" * 100)
        logger.info("BANK OF AMERICA - Berkshire Hathaway Position Changes")
        logger.info("=" * 100)

        bac_changes = (
            session.query(PositionChange)
            .join(Filer, PositionChange.filer_id == Filer.id)
            .filter(Filer.name == "Berkshire Hathaway")
            .filter(PositionChange.security_name == "BANK AMER CORP")
            .order_by(PositionChange.curr_period)
            .all()
        )

        for pc in bac_changes:
            logger.info(f"\nQuarter: {pc.curr_period}")
            logger.info(f"  Change Type: {pc.change_type.value}")

            if pc.prev_shares is not None:
                prev_price_per_share = pc.prev_market_value / pc.prev_shares
                curr_price_per_share = pc.curr_market_value / pc.curr_shares
                price_change_pct = ((curr_price_per_share - prev_price_per_share) / prev_price_per_share) * 100

                logger.info(f"\n  Previous: {pc.prev_shares:,} shares @ ${prev_price_per_share:.2f} = ${pc.prev_market_value:,}")
                logger.info(f"  Current:  {pc.curr_shares:,} shares @ ${curr_price_per_share:.2f} = ${pc.curr_market_value:,}")
                logger.info(f"\n  Share change: {pc.shares_change:,} ({pc.shares_change_pct:+.1f}%)")
                logger.info(f"  Price change: ${curr_price_per_share - prev_price_per_share:,.2f} ({price_change_pct:+.1f}%)")
                logger.info(f"  Value change: ${pc.value_change:,}")


if __name__ == "__main__":
    main()
