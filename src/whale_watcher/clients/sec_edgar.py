"""SEC EDGAR API client for fetching 13F filings."""

import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional

import requests

from whale_watcher.config import Config
from whale_watcher.utils.logger import get_logger


@dataclass
class FilingMetadata:
    """Represents 13F filing metadata from SEC submissions.

    Attributes:
        accession_number: Unique SEC filing identifier (e.g., "0001067983-25-000005")
        filing_date: Date the filing was submitted to SEC
        report_date: Quarter-end date for the filing (period_of_report)
        primary_document: Filename of the XML document containing holdings
        form_type: SEC form type (should be "13F-HR" for holdings reports)
    """

    accession_number: str
    filing_date: date
    report_date: date
    primary_document: str
    form_type: str


class SECEdgarClient:
    """Client for SEC EDGAR API with rate limiting and error handling.

    This client handles all interactions with the SEC EDGAR API including:
    - Fetching submission metadata for institutional investors
    - Filtering for 13F-HR filings
    - Downloading XML filing documents
    - Rate limiting to comply with SEC requirements (5 requests/second)

    All requests include the required User-Agent header as mandated by SEC.
    """

    BASE_URL = "https://data.sec.gov"
    ARCHIVES_BASE = "https://www.sec.gov/Archives/edgar/data"

    def __init__(self, config: Config):
        """Initialize SEC EDGAR client with configuration.

        Args:
            config: Application configuration containing user_agent and rate_limit settings
        """
        self.config = config
        self.logger = get_logger(__name__)
        self._last_request_time: float = 0.0
        self._min_interval = 1.0 / config.requests_per_second

        # Create session with required User-Agent header
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.user_agent,
            'Accept': 'application/json'
        })

        self.logger.info(
            f"Initialized SEC EDGAR client with rate limit: "
            f"{config.requests_per_second} req/sec"
        )

    def _rate_limit(self) -> None:
        """Enforce rate limiting between requests.

        Ensures minimum interval between requests to comply with SEC rate limits.
        Sleeps if necessary to maintain the configured requests_per_second.
        """
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            sleep_time = self._min_interval - elapsed
            self.logger.debug(f"Rate limiting: sleeping {sleep_time:.3f} seconds")
            time.sleep(sleep_time)
        self._last_request_time = time.time()

    def get_submissions(self, cik: str) -> dict:
        """Fetch submission metadata for a CIK from SEC EDGAR API.

        Args:
            cik: Central Index Key (CIK) for the filer. Can be with or without
                leading zeros - will be normalized to 10 digits.

        Returns:
            JSON response as dict containing filer metadata and recent filings

        Raises:
            requests.HTTPError: If API request fails (403, 404, 500, etc.)
            requests.Timeout: If request times out
        """
        # Normalize CIK to 10 digits with leading zeros
        normalized_cik = cik.zfill(10)
        url = f"{self.BASE_URL}/submissions/CIK{normalized_cik}.json"

        self._rate_limit()
        self.logger.info(f"Fetching submissions for CIK {normalized_cik}")

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            self.logger.error(f"HTTP error fetching submissions for CIK {normalized_cik}: {e}")
            raise
        except requests.Timeout as e:
            self.logger.error(f"Timeout fetching submissions for CIK {normalized_cik}: {e}")
            raise

    def get_13f_filings(
        self,
        cik: str,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None
    ) -> List[FilingMetadata]:
        """Get list of 13F-HR filings for a CIK, filtered by year.

        Args:
            cik: Central Index Key for the filer
            start_year: Filter filings from this year onwards (uses config if None)
            end_year: Filter filings up to this year (uses config if None)

        Returns:
            List of FilingMetadata objects for 13F-HR filings in the date range,
            ordered as returned by SEC (typically most recent first)
        """
        submissions = self.get_submissions(cik)

        # Use config defaults if not specified
        start_year = start_year or self.config.start_year
        end_year = end_year or self.config.end_year

        filings = []
        recent = submissions.get('filings', {}).get('recent', {})

        # SEC submissions API returns columnar data
        accession_numbers = recent.get('accessionNumber', [])
        filing_dates = recent.get('filingDate', [])
        report_dates = recent.get('reportDate', [])
        primary_documents = recent.get('primaryDocument', [])
        form_types = recent.get('form', [])

        for i in range(len(accession_numbers)):
            form_type = form_types[i]

            # Filter for 13F-HR only
            if form_type != '13F-HR':
                continue

            report_date_str = report_dates[i]
            report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()

            # Filter by year based on report date
            if not (start_year <= report_date.year <= end_year):
                continue

            filing_date_str = filing_dates[i]
            filing_date = datetime.strptime(filing_date_str, '%Y-%m-%d').date()

            filings.append(FilingMetadata(
                accession_number=accession_numbers[i],
                filing_date=filing_date,
                report_date=report_date,
                primary_document=primary_documents[i],
                form_type=form_type
            ))

        self.logger.info(
            f"Found {len(filings)} 13F-HR filings for CIK {cik} "
            f"({start_year}-{end_year})"
        )

        return filings

    def download_filing_xml(
        self,
        cik: str,
        accession_number: str,
        primary_document: str
    ) -> str:
        """Download XML content for a specific filing.

        Constructs the SEC Archives URL and downloads the filing document.

        Args:
            cik: Central Index Key (for URL construction)
            accession_number: Accession number in format "0001234567-22-000123"
            primary_document: Primary document filename (e.g., "form13fInfoTable.xml")

        Returns:
            XML content as string

        Raises:
            requests.HTTPError: If download fails
            requests.Timeout: If request times out
        """
        # Remove dashes from accession number for URL
        accession_no_dashes = accession_number.replace('-', '')

        # Remove leading zeros from CIK for Archives URL
        cik_no_leading_zeros = cik.lstrip('0')

        # Construct URL: https://www.sec.gov/Archives/edgar/data/{CIK}/{ACCESSION}/{document}
        url = (
            f"{self.ARCHIVES_BASE}/{cik_no_leading_zeros}/"
            f"{accession_no_dashes}/{primary_document}"
        )

        self._rate_limit()
        self.logger.info(f"Downloading filing XML: {accession_number}")
        self.logger.debug(f"Download URL: {url}")

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            self.logger.info(
                f"Downloaded {len(response.text)} bytes for {accession_number}"
            )

            return response.text
        except requests.HTTPError as e:
            self.logger.error(f"HTTP error downloading {accession_number}: {e}")
            raise
        except requests.Timeout as e:
            self.logger.error(f"Timeout downloading {accession_number}: {e}")
            raise

    def get_filing_documents(self, cik: str, accession_number: str) -> List[str]:
        """Get list of document filenames in a filing.

        Fetches the filing index page and extracts all document filenames.

        Args:
            cik: Central Index Key
            accession_number: Accession number in format "0001234567-22-000123"

        Returns:
            List of document filenames (e.g., ["primary_doc.xml", "form13fInfoTable.xml"])

        Raises:
            requests.HTTPError: If index page fetch fails
        """
        # Remove dashes from accession number and leading zeros from CIK
        accession_no_dashes = accession_number.replace('-', '')
        cik_no_leading_zeros = cik.lstrip('0')

        # Construct index page URL
        url = (
            f"{self.ARCHIVES_BASE}/{cik_no_leading_zeros}/"
            f"{accession_no_dashes}/"
        )

        self._rate_limit()
        self.logger.debug(f"Fetching filing documents from {url}")

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            # Extract all .xml filenames from href attributes
            # Pattern matches: href="filename.xml" or href="/path/filename.xml"
            pattern = r'href="(?:[^"]*/)?([\w\-\.]+\.xml)"'
            documents = re.findall(pattern, response.text, re.IGNORECASE)

            # Remove duplicates while preserving order
            unique_docs = list(dict.fromkeys(documents))

            self.logger.info(f"Found {len(unique_docs)} documents for {accession_number}")
            return unique_docs

        except requests.HTTPError as e:
            self.logger.error(f"HTTP error fetching documents for {accession_number}: {e}")
            raise

    def find_info_table_document(self, cik: str, accession_number: str) -> Optional[str]:
        """Find the information table XML document in a filing.

        Parses the filing -index.html page to find documents with
        Type="INFORMATION TABLE" as labeled by SEC. This is more reliable
        than filename pattern matching since SEC doesn't enforce naming conventions.

        Args:
            cik: Central Index Key
            accession_number: Accession number

        Returns:
            Info table filename if found, None otherwise
        """
        # Remove dashes from accession number and leading zeros from CIK
        accession_no_dashes = accession_number.replace('-', '')
        cik_no_leading_zeros = cik.lstrip('0')

        # Construct URL to -index.html file (contains document table with Type column)
        url = (
            f"{self.ARCHIVES_BASE}/{cik_no_leading_zeros}/"
            f"{accession_no_dashes}/{accession_number}-index.html"
        )

        self._rate_limit()
        self.logger.debug(f"Fetching filing index to find info table: {url}")

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            # Parse HTML table to find document with Type="INFORMATION TABLE"
            # Table structure: <tr>...<td>Document</td>...<td>Type</td>...</tr>
            # We need to extract the document filename from rows where Type contains "INFORMATION TABLE"

            # Pattern to match table rows and extract cells
            # This regex finds <tr> tags and captures content between <td> tags
            table_row_pattern = r'<tr[^>]*>(.*?)</tr>'
            cell_pattern = r'<td[^>]*>(.*?)</td>'

            info_table_candidates = []

            for row_match in re.finditer(table_row_pattern, response.text, re.IGNORECASE | re.DOTALL):
                row_html = row_match.group(1)
                cells = re.findall(cell_pattern, row_html, re.IGNORECASE | re.DOTALL)

                # Skip rows without enough cells (header rows, etc.)
                if len(cells) < 4:
                    continue

                # Typical structure: [Seq, Description, Document, Type, Size]
                # Document is usually 3rd column (index 2), Type is 4th (index 3)
                # Extract filename from href in Document column
                doc_cell = cells[2] if len(cells) > 2 else ""
                type_cell = cells[3] if len(cells) > 3 else ""

                # Check if Type column contains "INFORMATION TABLE"
                if "INFORMATION TABLE" in type_cell.upper():
                    # Extract filename from href in document cell
                    href_pattern = r'href="([^"]+)"'
                    href_match = re.search(href_pattern, doc_cell)
                    if href_match:
                        filename = href_match.group(1).split('/')[-1]  # Get filename without path
                        info_table_candidates.append(filename)
                        self.logger.debug(f"Found INFORMATION TABLE document: {filename}")

            # Prefer .xml over .html if multiple candidates
            xml_candidates = [f for f in info_table_candidates if f.lower().endswith('.xml')]
            if xml_candidates:
                self.logger.info(f"Found info table: {xml_candidates[0]}")
                return xml_candidates[0]
            elif info_table_candidates:
                self.logger.info(f"Found info table: {info_table_candidates[0]}")
                return info_table_candidates[0]

            self.logger.warning(f"No info table document found for {accession_number}")
            return None

        except requests.HTTPError as e:
            self.logger.error(f"HTTP error fetching index for {accession_number}: {e}")
            raise

    def download_info_table_xml(self, cik: str, accession_number: str) -> str:
        """Download the information table XML for a 13F filing.

        Finds and downloads the information table document containing holdings data.

        Args:
            cik: Central Index Key
            accession_number: Accession number

        Returns:
            XML content as string

        Raises:
            ValueError: If no info table document found
            requests.HTTPError: If download fails
        """
        info_table_doc = self.find_info_table_document(cik, accession_number)

        if info_table_doc is None:
            raise ValueError(
                f"No information table document found for filing {accession_number}"
            )

        return self.download_filing_xml(cik, accession_number, info_table_doc)

    def close(self) -> None:
        """Close the requests session and release resources."""
        self.logger.debug("Closing SEC EDGAR client session")
        self.session.close()
