
""" 
script to fetch a single filing for investor warren buffett 
written to test progress at end of phase 2 in local/checklist.md
"""

from whale_watcher.etl.extractor import extract_new_filings, download_and_store_filing_metadata
from whale_watcher.config import Config
from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.utils.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

# consolidate human code here for testing
def execute_test(cik: str, name: str, db: DatabaseConnection, config: Config) -> None:
      # Get whale config for all metadata
      whale = next(w for w in config.whales if w["cik"] == cik)

      # Step 1: Get new filings (limit=1 for testing)
      new_filings = extract_new_filings(
          cik=whale["cik"],
          name=whale["name"],
          description=whale["description"],
          category=whale["category"],
          config=config,
          db=db,
          limit=1
      )

      logger.info(new_filings)

      if not new_filings:
          logger.info("No new filings found")
          return


      # Step 2: Download and store metadata
      filing_id = download_and_store_filing_metadata(
          cik=whale["cik"],
          name=whale["name"],
          filing=new_filings[0],
          config=config,
          db=db,
          save_xml_path="local/berkshire_13f_sample.xml"
      )

      print(f"Stored filing with ID: {filing_id}")
    

if __name__ == "__main__":
    config = Config()
    with DatabaseConnection(config.database_url) as db:
        execute_test("0001067983", "Warren Buffett", db, config)

