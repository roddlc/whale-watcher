"""Tests for 13F XML parser."""

import pytest

from whale_watcher.etl.parser import parse_13f_info_table, HoldingData, FilingSummary


class TestParse13FInfoTable:
    """Test 13F information table XML parsing."""

    def test_parses_single_holding(self) -> None:
        """Test parsing a single holding entry."""
        xml_content = """
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
            <infoTable>
                <nameOfIssuer>ALLY FINL INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip>02005N100</cusip>
                <value>463886547</value>
                <shrsOrPrnAmt>
                    <sshPrnamt>12719675</sshPrnamt>
                    <sshPrnamtType>SH</sshPrnamtType>
                </shrsOrPrnAmt>
                <investmentDiscretion>DFND</investmentDiscretion>
                <votingAuthority>
                    <Sole>12719675</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
        </informationTable>
        """

        summary, holdings = parse_13f_info_table(xml_content)

        assert len(holdings) == 1
        holding = holdings[0]
        assert holding.cusip == "02005N100"
        assert holding.security_name == "ALLY FINL INC"
        assert holding.shares == 12719675
        assert holding.market_value == 463886547
        assert holding.voting_authority_sole == 12719675
        assert holding.voting_authority_shared == 0
        assert holding.voting_authority_none == 0

    def test_aggregates_holdings_by_cusip(self) -> None:
        """Test that multiple entries with same CUSIP are aggregated."""
        xml_content = """
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
            <infoTable>
                <nameOfIssuer>ALLY FINL INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip>02005N100</cusip>
                <value>463886547</value>
                <shrsOrPrnAmt>
                    <sshPrnamt>12719675</sshPrnamt>
                    <sshPrnamtType>SH</sshPrnamtType>
                </shrsOrPrnAmt>
                <votingAuthority>
                    <Sole>12719675</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
            <infoTable>
                <nameOfIssuer>ALLY FINL INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip>02005N100</cusip>
                <value>102257321</value>
                <shrsOrPrnAmt>
                    <sshPrnamt>2803875</sshPrnamt>
                    <sshPrnamtType>SH</sshPrnamtType>
                </shrsOrPrnAmt>
                <votingAuthority>
                    <Sole>2803875</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
        </informationTable>
        """

        summary, holdings = parse_13f_info_table(xml_content)

        # Should aggregate to single holding
        assert len(holdings) == 1
        holding = holdings[0]
        assert holding.cusip == "02005N100"
        assert holding.security_name == "ALLY FINL INC"
        assert holding.shares == 12719675 + 2803875  # 15523550
        assert holding.market_value == 463886547 + 102257321  # 566143868
        assert holding.voting_authority_sole == 12719675 + 2803875
        assert holding.voting_authority_shared == 0
        assert holding.voting_authority_none == 0

    def test_parses_multiple_different_securities(self) -> None:
        """Test parsing multiple different securities (different CUSIPs)."""
        xml_content = """
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
            <infoTable>
                <nameOfIssuer>ALLY FINL INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip>02005N100</cusip>
                <value>463886547</value>
                <shrsOrPrnAmt>
                    <sshPrnamt>12719675</sshPrnamt>
                    <sshPrnamtType>SH</sshPrnamtType>
                </shrsOrPrnAmt>
                <votingAuthority>
                    <Sole>12719675</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
            <infoTable>
                <nameOfIssuer>AMAZON COM INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip>023135106</cusip>
                <value>1469568240</value>
                <shrsOrPrnAmt>
                    <sshPrnamt>7724000</sshPrnamt>
                    <sshPrnamtType>SH</sshPrnamtType>
                </shrsOrPrnAmt>
                <votingAuthority>
                    <Sole>7724000</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
        </informationTable>
        """

        summary, holdings = parse_13f_info_table(xml_content)

        assert len(holdings) == 2
        assert holdings[0].cusip == "02005N100"
        assert holdings[0].security_name == "ALLY FINL INC"
        assert holdings[1].cusip == "023135106"
        assert holdings[1].security_name == "AMAZON COM INC"

    def test_calculates_filing_summary(self) -> None:
        """Test that filing summary is calculated correctly."""
        xml_content = """
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
            <infoTable>
                <nameOfIssuer>ALLY FINL INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip>02005N100</cusip>
                <value>100000000</value>
                <shrsOrPrnAmt>
                    <sshPrnamt>1000000</sshPrnamt>
                    <sshPrnamtType>SH</sshPrnamtType>
                </shrsOrPrnAmt>
                <votingAuthority>
                    <Sole>1000000</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
            <infoTable>
                <nameOfIssuer>AMAZON COM INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip>023135106</cusip>
                <value>200000000</value>
                <shrsOrPrnAmt>
                    <sshPrnamt>500000</sshPrnamt>
                    <sshPrnamtType>SH</sshPrnamtType>
                </shrsOrPrnAmt>
                <votingAuthority>
                    <Sole>500000</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
        </informationTable>
        """

        summary, holdings = parse_13f_info_table(xml_content)

        assert summary.total_value == 300000000
        assert summary.holdings_count == 2

    def test_handles_missing_voting_authority(self) -> None:
        """Test parsing handles missing voting authority fields."""
        xml_content = """
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
            <infoTable>
                <nameOfIssuer>ALLY FINL INC</nameOfIssuer>
                <titleOfClass>COM</titleOfClass>
                <cusip>02005N100</cusip>
                <value>463886547</value>
                <shrsOrPrnAmt>
                    <sshPrnamt>12719675</sshPrnamt>
                    <sshPrnamtType>SH</sshPrnamtType>
                </shrsOrPrnAmt>
                <votingAuthority>
                    <Sole>0</Sole>
                    <Shared>0</Shared>
                    <None>0</None>
                </votingAuthority>
            </infoTable>
        </informationTable>
        """

        summary, holdings = parse_13f_info_table(xml_content)

        assert len(holdings) == 1
        holding = holdings[0]
        assert holding.voting_authority_sole == 0
        assert holding.voting_authority_shared == 0
        assert holding.voting_authority_none == 0

    def test_handles_empty_info_table(self) -> None:
        """Test parsing handles empty information table."""
        xml_content = """
        <informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
        </informationTable>
        """

        summary, holdings = parse_13f_info_table(xml_content)

        assert summary.total_value == 0
        assert summary.holdings_count == 0
        assert len(holdings) == 0


class TestHoldingData:
    """Test HoldingData dataclass."""

    def test_holding_data_creation(self) -> None:
        """Test HoldingData can be created with all fields."""
        holding = HoldingData(
            cusip="02005N100",
            security_name="ALLY FINL INC",
            shares=12719675,
            market_value=463886547,
            voting_authority_sole=12719675,
            voting_authority_shared=0,
            voting_authority_none=0
        )

        assert holding.cusip == "02005N100"
        assert holding.security_name == "ALLY FINL INC"
        assert holding.shares == 12719675
        assert holding.market_value == 463886547


class TestFilingSummary:
    """Test FilingSummary dataclass."""

    def test_filing_summary_creation(self) -> None:
        """Test FilingSummary can be created with all fields."""
        summary = FilingSummary(
            total_value=258701144516,
            holdings_count=110
        )

        assert summary.total_value == 258701144516
        assert summary.holdings_count == 110
