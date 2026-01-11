# Whale Watcher 🐋

Track institutional investor positions and portfolio changes over time using SEC EDGAR 13F filings.

## Overview

Whale Watcher pulls and analyzes quarterly 13F filings from the SEC EDGAR database to track how large institutional investors ("whales") are positioning their portfolios. This provides insights into:

- Which stocks institutional investors are buying or selling
- Portfolio concentration and position sizing changes
- New positions and complete exits
- Historical trends in whale investment behavior

## Why This Matters

Institutional investors with over $100M in assets under management are required to file 13F forms quarterly, disclosing their equity holdings. By tracking these filings over time, we can:

- Identify smart money trends before they become mainstream
- See which stocks are gaining or losing institutional support
- Learn from the portfolio construction of successful investors
- Spot potential investment opportunities based on whale activity

## Tech Stack

- **Python 3.12+** - Core language
- **Pandas** - Data manipulation and analysis
- **PostgreSQL** - Persistent storage for historical filings
- **SQLAlchemy** - Database ORM and query builder
- **SEC EDGAR API** - Data source for 13F filings
- **Docker Compose** - Containerized database environment

## Setup

1. **Install dependencies:**
   ```bash
   uv sync
   ```

2. **Start the PostgreSQL database:**
   ```bash
   docker compose up -d
   ```

3. **Run the application:**
   ```bash
   uv run python main.py
   ```

## Data Sources

This project uses the SEC EDGAR system to fetch:
- **13F-HR filings** - Quarterly holdings reports from institutional investment managers
- **Filer information** - CIK numbers, names, and filing history
- **Historical data** - Multi-quarter position tracking for trend analysis

## Project Status

🚧 **In Development**
- This project is in active development for hobby purposes and will evolve over the coming weeks / months.
- This project will start with four large institutional investors (Warren Buffett, Ray Dalio, Cathy Wood, Jim Simons) as a proof of concept.
- This will begin as a command line tool, but will be extended to a web application in the future.

## Roadmap

- [ ] SEC EDGAR API integration
- [ ] Database schema for filers and holdings
- [ ] ETL pipeline for 13F filings
- [ ] Position change analysis
- [ ] Historical tracking and trend detection
- [ ] Data quality checks and validation
- [ ] Export and visualization capabilities
- [ ] LONG TERM: Web application for interactive analysis

---

*Note: This project is for educational and research purposes. Always conduct your own due diligence before making investment decisions.*
