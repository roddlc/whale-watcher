"""
Populate database with all 2025 filings for configured whales.

This script:
1. Initializes the database (creates tables)
2. Loads whales from config
3. For each enabled whale, extracts and stores all 2025 13F-HR filings
4. Reports summary statistics

Run this script to populate a fresh database or to catch up on new filings.
"""

from whale_watcher.config import load_config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.schema import create_tables
from whale_watcher.etl.extractor import (
    extract_new_filings,
    download_and_store_filing_metadata,
)
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging(level='INFO')
logger = get_logger(__name__)


def main() -> None:
    """Populate database with all 2025 filings."""
    # Load configuration
    logger.info("Loading configuration...")
    config = load_config()

    # Initialize database
    logger.info("Initializing database...")
    db = DatabaseConnection(config.database_url)
    create_tables(db.engine)

    # Track statistics
    total_filings_processed = 0
    whale_stats = {}

    # Process each enabled whale
    for whale in config.whales:
        if not whale["enabled"]:
            logger.info(f"Skipping disabled whale: {whale['name']}")
            continue

        logger.info(f"\n{'=' * 80}")
        logger.info(f"Processing whale: {whale['name']} (CIK: {whale['cik']})")
        logger.info(f"{'=' * 80}")

        try:
            # Extract new filings (no limit - get all)
            new_filings = extract_new_filings(
                cik=whale["cik"],
                name=whale["name"],
                description=whale.get("description"),
                category=whale["category"],
                config=config,
                db=db,
                limit=None  # Get ALL filings
            )

            if not new_filings:
                logger.info(f"No new filings to process for {whale['name']}")
                whale_stats[whale["name"]] = 0
                continue

            # Download and store each filing
            filings_processed = 0
            for filing in new_filings:
                logger.info(f"\nProcessing filing: {filing.accession_number}")
                logger.info(f"  Filing Date: {filing.filing_date}")
                logger.info(f"  Period of Report: {filing.report_date}")

                try:
                    filing_id = download_and_store_filing_metadata(
                        cik=whale["cik"],
                        name=whale["name"],
                        filing=filing,
                        config=config,
                        db=db
                    )
                    logger.info(f"  ✅ Successfully stored filing (ID: {filing_id})")
                    filings_processed += 1

                except Exception as e:
                    logger.error(f"  ❌ Failed to process filing: {e}")
                    continue

            whale_stats[whale["name"]] = filings_processed
            total_filings_processed += filings_processed

            logger.info(f"\nCompleted {whale['name']}: {filings_processed} filings processed")

        except Exception as e:
            logger.error(f"Failed to process whale {whale['name']}: {e}")
            whale_stats[whale["name"]] = 0
            continue

    # Print summary
    logger.info(f"\n{'=' * 80}")
    logger.info("SUMMARY")
    logger.info(f"{'=' * 80}")

    for whale_name, count in whale_stats.items():
        logger.info(f"  {whale_name}: {count} filings")

    logger.info(f"\nTotal filings processed: {total_filings_processed}")
    logger.info("Database population complete!")


if __name__ == "__main__":
    main()
