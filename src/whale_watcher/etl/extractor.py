"""Data extraction module for fetching 13F filings from SEC EDGAR."""

from pathlib import Path
from typing import List, Optional, Set, Union

from sqlalchemy.orm import Session

from whale_watcher.clients.sec_edgar import SECEdgarClient, FilingMetadata
from whale_watcher.config import Config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.models import Filer, Filing
from whale_watcher.etl.analyzer import calculate_position_changes
from whale_watcher.etl.loader import load_holdings, update_filing_summary
from whale_watcher.etl.parser import parse_13f_info_table
from whale_watcher.utils.logger import get_logger

logger = get_logger(__name__)


def get_existing_accession_numbers(session: Session, filer_id: int) -> Set[str]:
    """Get set of accession numbers already in database for a filer.

    This is used to avoid re-downloading filings that have already been processed.

    Args:
        session: SQLAlchemy database session
        filer_id: ID of the filer to check

    Returns:
        Set of accession numbers (strings) that already exist in the database
    """
    filings = session.query(Filing.accession_number).filter(
        Filing.filer_id == filer_id
    ).all()

    return {filing.accession_number for filing in filings}


def get_or_create_filer(
    session: Session,
    cik: str,
    name: str,
    description: Optional[str],
    category: str
) -> Filer:
    """Get existing filer or create new one.

    This function is idempotent - calling it multiple times with the same CIK
    will return the same filer without creating duplicates.

    Args:
        session: SQLAlchemy database session
        cik: Central Index Key (10 digits with leading zeros)
        name: Filer name (e.g., "Berkshire Hathaway")
        description: Optional description of the filer
        category: Category (e.g., "value_investing")

    Returns:
        Filer instance (either existing or newly created)
    """
    filer = session.query(Filer).filter(Filer.cik == cik).first()

    if filer is None:
        logger.info(f"Creating new filer: {name} (CIK: {cik})")
        filer = Filer(
            cik=cik,
            name=name,
            description=description,
            category=category,
            enabled=True
        )
        session.add(filer)
        session.flush()  # Get ID without committing transaction
    else:
        logger.debug(f"Filer already exists: {name} (ID: {filer.id})")

    return filer


def extract_new_filings(
    cik: str,
    name: str,
    description: Optional[str],
    category: str,
    config: Config,
    db: DatabaseConnection,
    limit: Optional[int] = None
) -> List[FilingMetadata]:
    """Extract new 13F filings for a filer that don't exist in database.

    This function:
    1. Creates or retrieves the filer from the database
    2. Fetches all 13F-HR filings from SEC EDGAR API
    3. Filters out filings that already exist in the database
    4. Optionally limits the number of filings returned (for testing)

    Args:
        cik: Central Index Key
        name: Filer name
        description: Optional filer description
        category: Filer category
        config: Application configuration
        db: Database connection
        limit: Optional limit on number of filings to return (for testing with ONE filing)

    Returns:
        List of FilingMetadata for new filings that should be downloaded
    """
    client = SECEdgarClient(config)

    try:
        # Get or create filer in database
        with db.session_scope() as session:
            filer = get_or_create_filer(session, cik, name, description, category)
            filer_id = filer.id
            existing = get_existing_accession_numbers(session, filer_id)

        logger.info(f"Found {len(existing)} existing filings for {name}")

        # Fetch all filings from SEC
        all_filings = client.get_13f_filings(cik)

        # Filter out existing
        new_filings = [
            filing for filing in all_filings
            if filing.accession_number not in existing
        ]

        logger.info(f"Found {len(new_filings)} new filings for {name}")

        # Apply limit if specified (for testing). For example, I want to pull just one filing for testing
        if limit is not None and len(new_filings) > limit:
            new_filings = new_filings[:limit]
            logger.info(f"Limited to {limit} filings for testing")

        return new_filings

    finally:
        client.close()


def download_and_store_filing_metadata(
    cik: str,
    name: str,
    filing: FilingMetadata,
    config: Config,
    db: DatabaseConnection,
    save_xml_path: Optional[Union[str, Path]] = None
) -> int:
    """Download filing XML, parse holdings, and store in database.

    This function implements the complete ETL workflow:
    1. Downloads primary XML document (for backward compatibility)
    2. Downloads info table XML
    3. Parses info table to extract holdings
    4. Stores filing metadata, holdings, and summary in database
    5. Marks filing as processed

    Args:
        cik: Central Index Key
        name: Filer name
        filing: FilingMetadata to download and store
        config: Application configuration
        db: Database connection
        save_xml_path: Optional path to save XML content for inspection/testing

    Returns:
        Filing ID (database primary key) of the stored filing

    Raises:
        ValueError: If filer doesn't exist in database
    """
    client = SECEdgarClient(config)

    try:
        # Download primary XML (backward compatible - kept for save_xml_path)
        xml_content = client.download_filing_xml(
            cik,
            filing.accession_number,
            filing.primary_document
        )

        logger.info(
            f"Downloaded {len(xml_content)} bytes for "
            f"{filing.accession_number}"
        )

        # Optionally save XML to file for inspection/testing
        if save_xml_path is not None:
            xml_path = Path(save_xml_path)
            xml_path.parent.mkdir(parents=True, exist_ok=True)
            xml_path.write_text(xml_content)
            logger.info(f"Saved XML to {xml_path}")

        # Download info table XML
        try:
            info_table_xml = client.download_info_table_xml(cik, filing.accession_number)
            logger.info(f"Downloaded info table XML ({len(info_table_xml)} bytes)")
        except ValueError as e:
            logger.warning(f"No info table found: {e}")
            # Fall back to metadata-only (existing behavior)
            with db.session_scope() as session:
                filer = session.query(Filer).filter(Filer.cik == cik).first()

                if filer is None:
                    raise ValueError(f"Filer not found for CIK: {cik}")

                filing_record = Filing(
                    filer_id=filer.id,
                    accession_number=filing.accession_number,
                    filing_date=filing.filing_date,
                    period_of_report=filing.report_date,
                    processed=False
                )

                session.add(filing_record)
                session.flush()

                filing_id = filing_record.id
                logger.info(
                    f"Stored filing metadata only: {filing.accession_number} "
                    f"(ID: {filing_id}, processed=False)"
                )

            return filing_id

        # Parse info table
        summary, holdings = parse_13f_info_table(info_table_xml)
        logger.info(f"Parsed {summary.holdings_count} holdings, total ${summary.total_value:,}")

        # Store everything in one transaction
        with db.session_scope() as session:
            # Get filer
            filer = session.query(Filer).filter(Filer.cik == cik).first()
            if filer is None:
                raise ValueError(f"Filer not found for CIK: {cik}")

            # Create Filing record
            filing_record = Filing(
                filer_id=filer.id,
                accession_number=filing.accession_number,
                filing_date=filing.filing_date,
                period_of_report=filing.report_date,
                processed=False
            )
            session.add(filing_record)
            session.flush()

            filing_id = filing_record.id

            # Load holdings
            load_holdings(session, filing_id, holdings)

            # Update summary
            update_filing_summary(session, filing_id, summary)

            # Calculate position changes (Phase 5)
            position_changes_count = calculate_position_changes(session, filing_id)

            logger.info(
                f"Stored filing {filing.accession_number} with {len(holdings)} holdings "
                f"and {position_changes_count} position changes"
            )

        return filing_id

    finally:
        client.close()
