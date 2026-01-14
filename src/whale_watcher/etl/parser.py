"""Parser for 13F-HR XML filings."""

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, List, Tuple

from whale_watcher.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class HoldingData:
    """Represents a single aggregated holding from 13F filing.

    Attributes:
        cusip: 9-character CUSIP identifier
        security_name: Name of the security/issuer
        shares: Total number of shares held
        market_value: Total market value in dollars
        voting_authority_sole: Shares with sole voting authority
        voting_authority_shared: Shares with shared voting authority
        voting_authority_none: Shares with no voting authority
    """

    cusip: str
    security_name: str
    shares: int
    market_value: int
    voting_authority_sole: int
    voting_authority_shared: int
    voting_authority_none: int


@dataclass
class FilingSummary:
    """Summary statistics for a 13F filing.

    Attributes:
        total_value: Total portfolio value in dollars
        holdings_count: Number of distinct securities held
    """

    total_value: int
    holdings_count: int


def parse_13f_info_table(xml_content: str) -> Tuple[FilingSummary, List[HoldingData]]:
    """Parse 13F information table XML and aggregate holdings by CUSIP.

    Args:
        xml_content: XML string containing 13F information table

    Returns:
        Tuple of (FilingSummary, List of HoldingData) with holdings aggregated by CUSIP

    Raises:
        ET.ParseError: If XML is malformed
    """
    # Parse XML
    root = ET.fromstring(xml_content)

    # Define namespace
    ns = {'ns': 'http://www.sec.gov/edgar/document/thirteenf/informationtable'}

    # Dictionary to aggregate holdings by CUSIP
    holdings_by_cusip: Dict[str, HoldingData] = {}

    # Parse each infoTable entry
    for info_table in root.findall('.//ns:infoTable', ns):
        # Extract required fields
        cusip_elem = info_table.find('ns:cusip', ns)
        name_elem = info_table.find('ns:nameOfIssuer', ns)
        value_elem = info_table.find('ns:value', ns)
        shares_elem = info_table.find('.//ns:sshPrnamt', ns)

        if cusip_elem is None or name_elem is None or value_elem is None or shares_elem is None:
            logger.warning("Skipping entry with missing required fields")
            continue

        cusip = cusip_elem.text
        security_name = name_elem.text
        market_value = int(value_elem.text)
        shares = int(shares_elem.text)

        # Extract voting authority (may be missing)
        voting_auth = info_table.find('ns:votingAuthority', ns)
        if voting_auth is not None:
            sole_elem = voting_auth.find('ns:Sole', ns)
            shared_elem = voting_auth.find('ns:Shared', ns)
            none_elem = voting_auth.find('ns:None', ns)

            voting_sole = int(sole_elem.text) if sole_elem is not None and sole_elem.text else 0
            voting_shared = int(shared_elem.text) if shared_elem is not None and shared_elem.text else 0
            voting_none = int(none_elem.text) if none_elem is not None and none_elem.text else 0
        else:
            voting_sole = 0
            voting_shared = 0
            voting_none = 0

        # Aggregate by CUSIP
        if cusip in holdings_by_cusip:
            # Add to existing holding
            existing = holdings_by_cusip[cusip]
            existing.shares += shares
            existing.market_value += market_value
            existing.voting_authority_sole += voting_sole
            existing.voting_authority_shared += voting_shared
            existing.voting_authority_none += voting_none
        else:
            # Create new holding
            holdings_by_cusip[cusip] = HoldingData(
                cusip=cusip,
                security_name=security_name,
                shares=shares,
                market_value=market_value,
                voting_authority_sole=voting_sole,
                voting_authority_shared=voting_shared,
                voting_authority_none=voting_none
            )

    # Convert to list
    holdings_list = list(holdings_by_cusip.values())

    # Calculate summary
    total_value = sum(h.market_value for h in holdings_list)
    holdings_count = len(holdings_list)

    summary = FilingSummary(
        total_value=total_value,
        holdings_count=holdings_count
    )

    logger.info(
        f"Parsed {holdings_count} holdings with total value ${total_value:,}"
    )

    return summary, holdings_list
