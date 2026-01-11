"""Pytest configuration and fixtures."""

import tempfile
from pathlib import Path
from typing import Generator

import pytest
import yaml


@pytest.fixture
def sample_config() -> dict:
    """Return a sample configuration dictionary."""
    return {
        'user_agent': 'TestAgent/1.0 (test@example.com)',
        'rate_limit': {
            'requests_per_second': 10,
            'max_retries': 5
        },
        'date_range': {
            'start_year': 2024,
            'end_year': 2025
        },
        'whales': [
            {
                'name': 'Test Whale 1',
                'cik': '0001234567',
                'description': 'Test whale 1',
                'category': 'test',
                'enabled': True
            },
            {
                'name': 'Test Whale 2',
                'cik': '0009876543',
                'description': 'Test whale 2',
                'category': 'test',
                'enabled': False
            }
        ],
        'database': {
            'host': 'testhost',
            'port': 5433,
            'name': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }
    }


@pytest.fixture
def config_file(sample_config: dict) -> Generator[Path, None, None]:
    """Create a temporary config file with sample data."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(sample_config, f)
        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    temp_path.unlink()


@pytest.fixture
def empty_config_file() -> Generator[Path, None, None]:
    """Create an empty config file."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    temp_path.unlink()
