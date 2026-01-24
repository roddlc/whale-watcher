"""
Test script for Phase 3: Download and parse 13F info table.

This script demonstrates:
1. Downloading the information table XML from SEC
2. Parsing it to extract holdings
3. Aggregating by CUSIP
4. Displaying summary statistics
"""

from whale_watcher.clients.sec_edgar import SECEdgarClient
from whale_watcher.config import Config
from whale_watcher.etl.parser import parse_13f_info_table
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging(level='DEBUG')
logger = get_logger(__name__)


def test_parse_filing(cik: str, accession_number: str, config: Config) -> None:
    """Download and parse a specific 13F filing.

    Args:
        cik: Central Index Key
        accession_number: Filing accession number
        config: Application configuration
    """
    client = SECEdgarClient(config)
    logger.debug('Logging at debug level')

    try:
        logger.info(f"Downloading info table for {accession_number}")

        # Download the information table XML
        xml_content = client.download_info_table_xml(cik, accession_number)

        logger.info(f"Downloaded {len(xml_content)} bytes of XML")

        # Save for inspection
        from pathlib import Path
        xml_path = Path("local/info_table_sample.xml")
        xml_path.parent.mkdir(parents=True, exist_ok=True)
        xml_path.write_text(xml_content)
        logger.info(f"Saved XML to {xml_path}")

        # Parse the XML
        summary, holdings = parse_13f_info_table(xml_content)

        # Display results
        logger.info("=" * 60)
        logger.info("FILING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Portfolio Value: ${summary.total_value:,}")
        logger.info(f"Number of Holdings: {summary.holdings_count}")
        logger.info("")

        logger.info("=" * 60)
        logger.info("TOP 10 HOLDINGS BY VALUE")
        logger.info("=" * 60)

        # Sort by market value descending
        top_holdings = sorted(holdings, key=lambda h: h.market_value, reverse=True)[:10]

        for i, holding in enumerate(top_holdings, 1):
            logger.info(f"{i}. {holding.security_name}")
            logger.info(f"   CUSIP: {holding.cusip}")
            logger.info(f"   Shares: {holding.shares:,}")
            logger.info(f"   Market Value: ${holding.market_value:,}")
            logger.info("")

    finally:
        client.close()


if __name__ == "__main__":
    config = Config()

    # Test with Berkshire Hathaway Q1 2025 filing
    # This is one of the filings we fetched in Phase 2
    test_parse_filing(
        cik="0001067983",
        accession_number="0000950123-25-005701",
        config=config
    )
