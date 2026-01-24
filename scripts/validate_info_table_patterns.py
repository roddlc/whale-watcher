"""
Validation script: Test info table discovery across all 4 whales.

This script validates that our info table filename patterns work for all
institutional investors in config/whales.yaml by:
1. Fetching one recent 13F filing from each whale
2. Attempting to find the info table document
3. Downloading and parsing it
4. Reporting success/failure and the actual filename found

This helps identify if any investor uses a naming convention we haven't
accounted for in our regex patterns.
"""

from whale_watcher.clients.sec_edgar import SECEdgarClient
from whale_watcher.config import Config
from whale_watcher.etl.parser import parse_13f_info_table
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def validate_whale(name: str, cik: str, config: Config) -> dict:
    """Validate info table discovery for a single whale.

    Args:
        name: Whale name for display
        cik: Central Index Key
        config: Application configuration

    Returns:
        Dict with validation results
    """
    client = SECEdgarClient(config)
    result = {
        "name": name,
        "cik": cik,
        "success": False,
        "info_table_filename": None,
        "holdings_count": 0,
        "error": None
    }

    try:
        # Get most recent 13F filing
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Validating: {name} (CIK {cik})")
        logger.info('=' * 60)

        filings = client.get_13f_filings(cik)

        if not filings:
            result["error"] = "No 13F filings found in date range"
            logger.warning(f"No 13F filings found for {name}")
            return result

        # Use most recent filing (first in list)
        filing = filings[0]
        logger.info(f"Testing filing: {filing.accession_number}")
        logger.info(f"Report date: {filing.report_date}")

        # Try to find info table document
        info_table_filename = client.find_info_table_document(cik, filing.accession_number)

        if info_table_filename is None:
            result["error"] = "Info table document not found"
            logger.error(f"❌ FAILED: No info table found for {name}")

            # Log all documents for debugging
            all_docs = client.get_filing_documents(cik, filing.accession_number)
            logger.info(f"All documents in filing: {all_docs}")
            return result

        result["info_table_filename"] = info_table_filename
        logger.info(f"✓ Found info table: {info_table_filename}")

        # Download and parse to validate it's actually valid
        xml_content = client.download_info_table_xml(cik, filing.accession_number)
        summary, holdings = parse_13f_info_table(xml_content)

        result["holdings_count"] = summary.holdings_count
        result["success"] = True

        logger.info(f"✓ Successfully parsed {summary.holdings_count} holdings")
        logger.info(f"✓ Total portfolio value: ${summary.total_value:,}")

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"❌ FAILED: {name} - {e}")

    finally:
        client.close()

    return result


def main():
    """Validate info table discovery for all whales."""
    config = Config()

    # Test all 4 whales from config
    whales_to_test = [
        ("Berkshire Hathaway", "0001067983"),      # Warren Buffett
        ("Bridgewater Associates", "0001350694"),  # Ray Dalio
        ("ARK Invest", "0001697748"),              # Cathie Wood (corrected CIK)
        ("Renaissance Technologies", "0001037389"), # Jim Simons
    ]

    results = []

    for name, cik in whales_to_test:
        result = validate_whale(name, cik, config)
        results.append(result)

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)

    success_count = sum(1 for r in results if r["success"])

    for r in results:
        status = "✓ PASS" if r["success"] else "❌ FAIL"
        logger.info(f"{status} - {r['name']}")
        if r["success"]:
            logger.info(f"       Filename: {r['info_table_filename']}")
            logger.info(f"       Holdings: {r['holdings_count']}")
        else:
            logger.info(f"       Error: {r['error']}")

    logger.info("")
    logger.info(f"Results: {success_count}/{len(results)} whales validated")

    if success_count == len(results):
        logger.info("✓ All whales validated successfully!")
    else:
        logger.warning("⚠ Some whales failed - review patterns in sec_edgar.py")


if __name__ == "__main__":
    main()
