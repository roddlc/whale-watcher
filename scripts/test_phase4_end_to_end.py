"""
Test script for Phase 4: Complete ETL workflow with database loading.

This script demonstrates the full extract → parse → load workflow:
1. Download 13F info table XML from SEC
2. Parse XML to extract holdings
3. Create Filing record in database
4. Load holdings into database
5. Update filing summary and mark as processed
6. Verify results and display statistics
"""

from datetime import date

from whale_watcher.clients.sec_edgar import SECEdgarClient
from whale_watcher.config import Config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.models import Filer, Filing, Holding
from whale_watcher.etl.loader import load_holdings, update_filing_summary
from whale_watcher.etl.parser import parse_13f_info_table
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging(level='INFO')
logger = get_logger(__name__)


def test_full_etl_workflow(
    cik: str,
    accession_number: str,
    filer_name: str,
    filing_date: date,
    period_of_report: date,
    config: Config,
    db: DatabaseConnection
) -> None:
    """
    Test the complete ETL workflow from download to database storage.

    Args:
        cik: Central Index Key
        accession_number: Filing accession number
        filer_name: Name of the institutional investor
        filing_date: Date the filing was submitted
        period_of_report: Quarter end date for the filing
        config: Application configuration
        db: Database connection
    """
    logger.info("=" * 80)
    logger.info("PHASE 4 END-TO-END TEST: Extract → Parse → Load")
    logger.info("=" * 80)
    logger.info(f"Filer: {filer_name} (CIK: {cik})")
    logger.info(f"Filing: {accession_number}")
    logger.info(f"Period: {period_of_report}")
    logger.info("")

    client = SECEdgarClient(config)

    try:
        # STEP 1: Download info table XML
        logger.info("STEP 1: Downloading info table XML...")
        xml_content = client.download_info_table_xml(cik, accession_number)
        logger.info(f"Downloaded {len(xml_content):,} bytes of XML")
        logger.info("")

        # STEP 2: Parse XML
        logger.info("STEP 2: Parsing XML...")
        summary, holdings = parse_13f_info_table(xml_content)
        logger.info(f"Parsed {summary.holdings_count} holdings")
        logger.info(f"Total portfolio value: ${summary.total_value:,} (thousands)")
        logger.info("")

        # STEP 3: Database operations
        logger.info("STEP 3: Loading data into database...")

        with db.session_scope() as session:
            # Create or get Filer
            filer = session.query(Filer).filter(Filer.cik == cik).first()

            if filer is None:
                logger.info(f"Creating new filer: {filer_name}")
                filer = Filer(
                    cik=cik,
                    name=filer_name,
                    category="hedge_fund",
                    enabled=True
                )
                session.add(filer)
                session.flush()
                logger.info(f"Created filer with id={filer.id}")
            else:
                logger.info(f"Using existing filer: {filer.name} (id={filer.id})")

            # Check if filing already exists
            existing_filing = session.query(Filing).filter(
                Filing.accession_number == accession_number
            ).first()

            if existing_filing is not None:
                logger.info(f"Filing {accession_number} already exists (id={existing_filing.id})")
                logger.info("Deleting existing filing and holdings to re-test...")
                session.delete(existing_filing)
                session.flush()

            # Create Filing record
            filing_record = Filing(
                filer_id=filer.id,
                accession_number=accession_number,
                filing_date=filing_date,
                period_of_report=period_of_report,
                processed=False
            )
            session.add(filing_record)
            session.flush()
            filing_id = filing_record.id
            logger.info(f"Created filing record with id={filing_id}")

            # Load holdings
            load_holdings(session, filing_id, holdings)

            # Update filing summary
            update_filing_summary(session, filing_id, summary)

        logger.info("")

        # STEP 4: Verify results
        logger.info("STEP 4: Verifying database contents...")

        with db.session_scope() as session:
            # Query filing
            filing = session.query(Filing).filter(Filing.id == filing_id).first()

            if filing is None:
                raise ValueError(f"Filing not found with id={filing_id}")

            # Count holdings
            holdings_count = session.query(Holding).filter(Holding.filing_id == filing_id).count()

            logger.info(f"Filing verification:")
            logger.info(f"  Accession: {filing.accession_number}")
            logger.info(f"  Total value: ${filing.total_value:,} (thousands)")
            logger.info(f"  Holdings count: {filing.holdings_count}")
            logger.info(f"  Processed: {filing.processed}")
            logger.info(f"  Actual holdings in DB: {holdings_count}")

            # Verify counts match
            if filing.holdings_count != holdings_count:
                logger.error(
                    f"MISMATCH: filing.holdings_count={filing.holdings_count} "
                    f"but actual count={holdings_count}"
                )
            else:
                logger.info("✓ Holdings count matches actual records")

            # Verify processed flag
            if not filing.processed:
                logger.error("ERROR: Filing not marked as processed!")
            else:
                logger.info("✓ Filing marked as processed")

            logger.info("")

            # STEP 5: Display top holdings
            logger.info("STEP 5: Displaying top 10 holdings by market value...")
            logger.info("=" * 80)

            top_holdings = (
                session.query(Holding)
                .filter(Holding.filing_id == filing_id)
                .order_by(Holding.market_value.desc())
                .limit(10)
                .all()
            )

            for i, holding in enumerate(top_holdings, 1):
                logger.info(f"{i}. {holding.security_name}")
                logger.info(f"   CUSIP: {holding.cusip}")
                logger.info(f"   Shares: {holding.shares:,}")
                logger.info(f"   Market Value: ${holding.market_value:,} (thousands)")
                logger.info(f"   Voting - Sole: {holding.voting_authority_sole:,}, "
                           f"Shared: {holding.voting_authority_shared:,}, "
                           f"None: {holding.voting_authority_none:,}")
                logger.info("")

            # STEP 6: Verify CUSIP aggregation
            logger.info("STEP 6: Verifying CUSIP aggregation...")

            # Check for ALLY FINL (should be single aggregated entry)
            ally_holdings = (
                session.query(Holding)
                .filter(Holding.filing_id == filing_id)
                .filter(Holding.security_name.like('%ALLY%'))
                .all()
            )

            if ally_holdings:
                logger.info(f"Found {len(ally_holdings)} ALLY entries:")
                for ally in ally_holdings:
                    logger.info(f"  {ally.security_name} (CUSIP: {ally.cusip}) - "
                               f"{ally.shares:,} shares, ${ally.market_value:,}")

                if len(ally_holdings) == 1:
                    logger.info("✓ ALLY properly aggregated into single entry")
                else:
                    logger.warning(f"⚠ Expected 1 ALLY entry, found {len(ally_holdings)}")
            else:
                logger.info("No ALLY holdings found (may not be in this filing)")

            logger.info("")

        logger.info("=" * 80)
        logger.info("END-TO-END TEST COMPLETE")
        logger.info("=" * 80)

    finally:
        client.close()


if __name__ == "__main__":
    config = Config()
    db = DatabaseConnection(config.database_url)

    try:
        # Test with Berkshire Hathaway Q1 2025 filing
        test_full_etl_workflow(
            cik="0001067983",
            accession_number="0000950123-25-005701",
            filer_name="BERKSHIRE HATHAWAY INC",
            filing_date=date(2025, 5, 15),  # Approximate filing date
            period_of_report=date(2025, 3, 31),  # Q1 2025
            config=config,
            db=db
        )
    finally:
        db.close()
