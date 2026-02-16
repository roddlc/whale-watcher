"""
Whale Watcher ETL Pipeline - Main Entry Point.

This module orchestrates the complete ETL workflow for processing 13F filings
from institutional investors (whales):
  1. Extract: Download new filings from SEC EDGAR
  2. Parse: Parse XML holdings data
  3. Load: Insert holdings into database
  4. Analyze: Calculate quarter-over-quarter position changes

Usage:
    # Process all enabled whales
    uv run python main.py

    # Process specific whale
    uv run python main.py --whale "Berkshire Hathaway"

    # Filter by CIK
    uv run python main.py --cik 0001067983

    # Limit filings per whale (for testing)
    uv run python main.py --limit 1

    # Enable debug logging
    uv run python main.py --verbose
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

from whale_watcher.utils.logger import get_logger

logger = get_logger(__name__)


def validate_config_path(config_path: str) -> None:
    """
    Validate config file path for security.

    Args:
        config_path: Path to config file

    Raises:
        FileNotFoundError: Config file doesn't exist
        ValueError: Invalid file extension or not a file
    """
    path = Path(config_path).resolve()  # Resolve symlinks

    # Check extension
    if path.suffix not in [".yaml", ".yml"]:
        raise ValueError(f"Config file must be .yaml or .yml, got: {path.suffix}")

    # Check file exists and is readable
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    if not path.is_file():
        raise ValueError(f"Config path is not a file: {path}")


def parse_arguments() -> argparse.Namespace:
    """
    Parse and validate command-line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Whale Watcher ETL Pipeline - Process 13F filings from institutional investors",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all enabled whales
  uv run python main.py

  # Process specific whale by name
  uv run python main.py --whale "Berkshire Hathaway"

  # Process specific whale by CIK
  uv run python main.py --cik 0001067983

  # Process multiple whales
  uv run python main.py --whale "Berkshire Hathaway" --whale "ARK Invest"

  # Limit filings per whale (useful for testing)
  uv run python main.py --limit 1

  # Enable verbose debug logging
  uv run python main.py --verbose

  # Skip database initialization (faster re-runs)
  uv run python main.py --skip-init

  # Use custom config file
  uv run python main.py --config config/custom.yaml
        """,
    )

    parser.add_argument(
        "--whale",
        action="append",
        dest="whale",
        help="Filter to specific whale name(s). Case-insensitive, exact match required. "
        'Can be specified multiple times. Example: --whale "Berkshire Hathaway"',
    )

    parser.add_argument(
        "--cik",
        action="append",
        dest="cik",
        help="Filter to specific CIK(s). Numeric only, will be normalized to 10 digits. "
        "Can be specified multiple times. Example: --cik 0001067983 or --cik 1067983",
    )

    parser.add_argument(
        "--limit",
        type=int,
        help="Limit number of filings to process per whale (must be >= 1). "
        "Useful for testing. Example: --limit 1",
    )

    parser.add_argument(
        "--skip-init",
        action="store_true",
        help="Skip database table creation. Use this flag for faster re-runs when "
        "tables already exist. Will fail if tables don't exist.",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable DEBUG level logging for detailed output",
    )

    parser.add_argument(
        "--config",
        type=str,
        default="config/whales.yaml",
        help="Path to config file. Must be .yaml or .yml extension. "
        "Default: config/whales.yaml",
    )

    return parser.parse_args()


def main() -> int:
    """
    Main entry point for Whale Watcher ETL pipeline.

    Returns:
        Exit code: 0 for success, 1 for any errors
    """
    # Parse arguments
    args = parse_arguments()

    # TODO: Implement main orchestration logic
    logger.info("Main function stub - implementation in progress")
    logger.info(f"Arguments: {args}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
