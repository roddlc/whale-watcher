"""Tests for logging setup."""

import logging
from pathlib import Path

import pytest

from whale_watcher.utils.logger import get_logger, setup_logging


class TestSetupLogging:
    """Test setup_logging function."""

    def test_setup_logging_creates_logger(self) -> None:
        """Test setup_logging creates and configures root logger."""
        logger = setup_logging(level=logging.DEBUG)
        assert logger is not None
        assert logger.level == logging.DEBUG

    def test_setup_logging_with_default_level(self) -> None:
        """Test setup_logging uses INFO level by default."""
        logger = setup_logging()
        assert logger.level == logging.INFO

    def test_setup_logging_configures_handlers(self) -> None:
        """Test setup_logging adds console handler."""
        logger = setup_logging()
        # Should have at least one handler (console)
        assert len(logger.handlers) > 0

    def test_setup_logging_handler_format(self) -> None:
        """Test setup_logging configures handler with proper format."""
        logger = setup_logging()
        # Check that handler has a formatter
        for handler in logger.handlers:
            assert handler.formatter is not None

    def test_setup_logging_different_levels(self) -> None:
        """Test setup_logging respects different log levels."""
        debug_logger = setup_logging(level=logging.DEBUG)
        assert debug_logger.level == logging.DEBUG

        # Clean up and test WARNING level
        for handler in debug_logger.handlers[:]:
            debug_logger.removeHandler(handler)

        warning_logger = setup_logging(level=logging.WARNING)
        assert warning_logger.level == logging.WARNING


class TestGetLogger:
    """Test get_logger function."""

    def test_get_logger_returns_logger(self) -> None:
        """Test get_logger returns a Logger instance."""
        logger = get_logger(__name__)
        assert isinstance(logger, logging.Logger)

    def test_get_logger_with_name(self) -> None:
        """Test get_logger creates logger with specified name."""
        logger = get_logger("test.module")
        assert logger.name == "test.module"

    def test_get_logger_inherits_root_config(self) -> None:
        """Test get_logger inherits configuration from root logger."""
        # Setup root logger
        setup_logging(level=logging.DEBUG)

        # Get a child logger
        logger = get_logger("test.child")

        # Child logger should inherit from root
        assert logger.name == "test.child"

    def test_get_logger_multiple_calls_same_name(self) -> None:
        """Test get_logger returns same logger instance for same name."""
        logger1 = get_logger("test.same")
        logger2 = get_logger("test.same")

        # Should be the same object
        assert logger1 is logger2


class TestLoggerUsage:
    """Test actual logging functionality."""

    def test_logger_can_log_messages(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test logger can log messages at different levels."""
        logger = get_logger("test.usage")

        with caplog.at_level(logging.INFO):
            logger.info("Test info message")
            logger.warning("Test warning message")
            logger.error("Test error message")

        assert "Test info message" in caplog.text
        assert "Test warning message" in caplog.text
        assert "Test error message" in caplog.text

    def test_logger_respects_level(self) -> None:
        """Test logger respects configured log level."""
        # Setup with WARNING level
        root_logger = setup_logging(level=logging.WARNING)
        logger = get_logger("test.level")

        # Verify root logger is set to WARNING
        assert root_logger.level == logging.WARNING

        # Verify logger inherits from root and would respect WARNING level
        # Child loggers inherit from root, so they'll respect the WARNING level
        assert logger.getEffectiveLevel() == logging.WARNING

    def test_logger_with_exception_info(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test logger can log exceptions."""
        logger = get_logger("test.exception")

        with caplog.at_level(logging.ERROR):
            try:
                raise ValueError("Test exception")
            except ValueError:
                logger.exception("An error occurred")

        assert "An error occurred" in caplog.text
        assert "ValueError: Test exception" in caplog.text
