"""Logging configuration for whale-watcher application."""

import logging
import sys


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """
    Configure root logger for the application.

    Args:
        level: Logging level (default: logging.INFO)

    Returns:
        Configured root logger instance
    """
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Create formatter with fixed-width columns
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(name)-32s | Line %(lineno)4s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)

    # Add handler to root logger
    root_logger.addHandler(console_handler)

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a module.

    Args:
        name: Logger name (typically __name__ of the module)

    Returns:
        Logger instance
    """
    return logging.getLogger(name)
