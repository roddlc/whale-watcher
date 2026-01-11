"""Configuration loader for whale-watcher application."""

from pathlib import Path
from typing import Any, Dict, List

import yaml


class Config:
    """Configuration manager for whale-watcher application."""

    def __init__(self, config_path: str | None = None):
        """
        Initialize configuration from YAML file.

        Args:
            config_path: Path to config file. If None, uses default config/whales.yaml
        """
        if config_path is None:
            # Default to config/whales.yaml relative to project root
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "whales.yaml"

        self.config_path = Path(config_path)
        self._config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load and parse YAML configuration file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)

        if not config:
            raise ValueError(f"Empty or invalid config file: {self.config_path}")

        return config

    @property
    def user_agent(self) -> str:
        """Get SEC EDGAR API user agent string."""
        return self._config.get('user_agent', 'WhaleWatcher/1.0')

    @property
    def rate_limit(self) -> Dict[str, int]:
        """Get rate limiting configuration."""
        return self._config.get('rate_limit', {
            'requests_per_second': 5,
            'max_retries': 3
        })

    @property
    def requests_per_second(self) -> int:
        """Get requests per second limit."""
        return self.rate_limit.get('requests_per_second', 5)

    @property
    def max_retries(self) -> int:
        """Get maximum retry attempts."""
        return self.rate_limit.get('max_retries', 3)

    @property
    def date_range(self) -> Dict[str, int]:
        """Get date range configuration."""
        return self._config.get('date_range', {
            'start_year': 2025,
            'end_year': 2025
        })

    @property
    def start_year(self) -> int:
        """Get start year for filing extraction."""
        return self.date_range.get('start_year', 2025)

    @property
    def end_year(self) -> int:
        """Get end year for filing extraction."""
        return self.date_range.get('end_year', 2025)

    @property
    def whales(self) -> List[Dict[str, Any]]:
        """Get list of whale (filer) configurations."""
        return self._config.get('whales', [])

    @property
    def enabled_whales(self) -> List[Dict[str, Any]]:
        """Get list of enabled whale configurations only."""
        return [whale for whale in self.whales if whale.get('enabled', True)]

    @property
    def database(self) -> Dict[str, Any]:
        """Get database configuration."""
        return self._config.get('database', {})

    @property
    def db_host(self) -> str:
        """Get database host."""
        return self.database.get('host', 'localhost')

    @property
    def db_port(self) -> int:
        """Get database port."""
        return self.database.get('port', 5432)

    @property
    def db_name(self) -> str:
        """Get database name."""
        return self.database.get('name', 'whale_watcher')

    @property
    def db_user(self) -> str:
        """Get database user."""
        return self.database.get('user', 'admin')

    @property
    def db_password(self) -> str:
        """Get database password."""
        return self.database.get('password', '')

    @property
    def database_url(self) -> str:
        """Get SQLAlchemy database URL."""
        return (
            f"postgresql://{self.db_user}:{self.db_password}@"
            f"{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def get_whale_by_cik(self, cik: str) -> Dict[str, Any] | None:
        """
        Get whale configuration by CIK number.

        Args:
            cik: CIK number (with or without leading zeros)

        Returns:
            Whale configuration dictionary or None if not found
        """
        # Normalize CIK to 10 digits with leading zeros
        normalized_cik = cik.zfill(10)

        for whale in self.whales:
            whale_cik = whale.get('cik', '').zfill(10)
            if whale_cik == normalized_cik:
                return whale

        return None

    def get_whale_by_name(self, name: str) -> Dict[str, Any] | None:
        """
        Get whale configuration by name.

        Args:
            name: Whale name (case-insensitive)

        Returns:
            Whale configuration dictionary or None if not found
        """
        name_lower = name.lower()
        for whale in self.whales:
            if whale.get('name', '').lower() == name_lower:
                return whale

        return None


def load_config(config_path: str | None = None) -> Config:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to config file. If None, uses default config/whales.yaml

    Returns:
        Config instance
    """
    return Config(config_path)
