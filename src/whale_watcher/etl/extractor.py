"""Data extraction module for fetching 13F filings from SEC EDGAR."""

from typing import List, Optional, Set

from sqlalchemy.orm import Session

from whale_watcher.clients.sec_edgar import SECEdgarClient, FilingMetadata
from whale_watcher.config import Config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.models import Filer, Filing
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

        # Apply limit if specified (for testing)
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
    db: DatabaseConnection
) -> int:
    """Download filing XML and store metadata in database.

    Phase 2: This function downloads the XML to verify it exists but does not
    parse it yet. It only stores the filing metadata (accession number, dates).
    Phase 3 will add XML parsing and holdings loading.

    Args:
        cik: Central Index Key
        name: Filer name
        filing: FilingMetadata to download and store
        config: Application configuration
        db: Database connection

    Returns:
        Filing ID (database primary key) of the stored filing

    Raises:
        ValueError: If filer doesn't exist in database
    """
    client = SECEdgarClient(config)

    try:
        # Download XML (for Phase 2, we validate it downloads but don't parse)
        xml_content = client.download_filing_xml(
            cik,
            filing.accession_number,
            filing.primary_document
        )

        logger.info(
            f"Downloaded {len(xml_content)} bytes for "
            f"{filing.accession_number}"
        )

        # Store metadata in database
        with db.session_scope() as session:
            filer = session.query(Filer).filter(Filer.cik == cik).first()

            if filer is None:
                raise ValueError(f"Filer not found for CIK: {cik}")

            # Create Filing record (metadata only - Phase 2)
            filing_record = Filing(
                filer_id=filer.id,
                accession_number=filing.accession_number,
                filing_date=filing.filing_date,
                period_of_report=filing.report_date,
                processed=False  # Will be True after Phase 3 (XML parsing)
            )

            session.add(filing_record)
            session.flush()

            filing_id = filing_record.id
            logger.info(
                f"Stored filing metadata: {filing.accession_number} "
                f"(ID: {filing_id})"
            )

        return filing_id

    finally:
        client.close()
