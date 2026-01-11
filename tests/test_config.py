"""Tests for configuration loader."""

from pathlib import Path

import pytest

from whale_watcher.config import Config, load_config


class TestConfigInitialization:
    """Test Config class initialization."""

    def test_init_with_valid_config_file(self, config_file: Path) -> None:
        """Test initialization with a valid config file."""
        config = Config(str(config_file))
        assert config.config_path == config_file
        assert config._config is not None

    def test_init_with_nonexistent_file(self) -> None:
        """Test initialization with a nonexistent config file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            Config("/nonexistent/path/to/config.yaml")

    def test_init_with_empty_file(self, empty_config_file: Path) -> None:
        """Test initialization with an empty config file raises ValueError."""
        with pytest.raises(ValueError, match="Empty or invalid config file"):
            Config(str(empty_config_file))

    def test_init_with_none_uses_default_path(self) -> None:
        """Test initialization with None uses default config path."""
        # Assumes config/whales.yaml exists in the project
        config = Config(None)
        assert config.config_path.name == 'whales.yaml'
        assert 'config' in str(config.config_path)


class TestUserAgentProperty:
    """Test user_agent property."""

    def test_user_agent_from_config(self, config_file: Path) -> None:
        """Test user_agent property returns value from config."""
        config = Config(str(config_file))
        assert config.user_agent == 'TestAgent/1.0 (test@example.com)'

    def test_user_agent_default_when_missing(self, config_file: Path, sample_config: dict) -> None:
        """Test user_agent returns default when not in config."""
        # Remove user_agent from config
        sample_config.pop('user_agent')

        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(sample_config, f)

        config = Config(str(config_file))
        assert config.user_agent == 'WhaleWatcher/1.0'


class TestRateLimitProperties:
    """Test rate limiting properties."""

    def test_rate_limit_property(self, config_file: Path) -> None:
        """Test rate_limit property returns dict."""
        config = Config(str(config_file))
        assert config.rate_limit == {
            'requests_per_second': 10,
            'max_retries': 5
        }

    def test_requests_per_second_property(self, config_file: Path) -> None:
        """Test requests_per_second property."""
        config = Config(str(config_file))
        assert config.requests_per_second == 10

    def test_max_retries_property(self, config_file: Path) -> None:
        """Test max_retries property."""
        config = Config(str(config_file))
        assert config.max_retries == 5

    def test_rate_limit_defaults(self, config_file: Path, sample_config: dict) -> None:
        """Test rate limit defaults when not in config."""
        sample_config.pop('rate_limit')

        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(sample_config, f)

        config = Config(str(config_file))
        assert config.requests_per_second == 5
        assert config.max_retries == 3


class TestDateRangeProperties:
    """Test date range properties."""

    def test_date_range_property(self, config_file: Path) -> None:
        """Test date_range property returns dict."""
        config = Config(str(config_file))
        assert config.date_range == {
            'start_year': 2024,
            'end_year': 2025
        }

    def test_start_year_property(self, config_file: Path) -> None:
        """Test start_year property."""
        config = Config(str(config_file))
        assert config.start_year == 2024

    def test_end_year_property(self, config_file: Path) -> None:
        """Test end_year property."""
        config = Config(str(config_file))
        assert config.end_year == 2025

    def test_date_range_defaults(self, config_file: Path, sample_config: dict) -> None:
        """Test date range defaults when not in config."""
        sample_config.pop('date_range')

        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(sample_config, f)

        config = Config(str(config_file))
        assert config.start_year == 2025
        assert config.end_year == 2025


class TestWhalesProperties:
    """Test whale configuration properties."""

    def test_whales_property(self, config_file: Path) -> None:
        """Test whales property returns list of all whales."""
        config = Config(str(config_file))
        assert len(config.whales) == 2
        assert config.whales[0]['name'] == 'Test Whale 1'
        assert config.whales[1]['name'] == 'Test Whale 2'

    def test_enabled_whales_property(self, config_file: Path) -> None:
        """Test enabled_whales property returns only enabled whales."""
        config = Config(str(config_file))
        enabled = config.enabled_whales
        assert len(enabled) == 1
        assert enabled[0]['name'] == 'Test Whale 1'
        assert enabled[0]['enabled'] is True

    def test_whales_default_empty_list(self, config_file: Path, sample_config: dict) -> None:
        """Test whales returns empty list when not in config."""
        sample_config.pop('whales')

        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(sample_config, f)

        config = Config(str(config_file))
        assert config.whales == []
        assert config.enabled_whales == []


class TestDatabaseProperties:
    """Test database configuration properties."""

    def test_database_property(self, config_file: Path) -> None:
        """Test database property returns dict."""
        config = Config(str(config_file))
        assert config.database == {
            'host': 'testhost',
            'port': 5433,
            'name': 'testdb',
            'user': 'testuser',
            'password': 'testpass'
        }

    def test_db_host_property(self, config_file: Path) -> None:
        """Test db_host property."""
        config = Config(str(config_file))
        assert config.db_host == 'testhost'

    def test_db_port_property(self, config_file: Path) -> None:
        """Test db_port property."""
        config = Config(str(config_file))
        assert config.db_port == 5433

    def test_db_name_property(self, config_file: Path) -> None:
        """Test db_name property."""
        config = Config(str(config_file))
        assert config.db_name == 'testdb'

    def test_db_user_property(self, config_file: Path) -> None:
        """Test db_user property."""
        config = Config(str(config_file))
        assert config.db_user == 'testuser'

    def test_db_password_property(self, config_file: Path) -> None:
        """Test db_password property."""
        config = Config(str(config_file))
        assert config.db_password == 'testpass'

    def test_database_url_property(self, config_file: Path) -> None:
        """Test database_url property generates correct SQLAlchemy URL."""
        config = Config(str(config_file))
        expected = 'postgresql://testuser:testpass@testhost:5433/testdb'
        assert config.database_url == expected

    def test_database_defaults(self, config_file: Path, sample_config: dict) -> None:
        """Test database defaults when not in config."""
        sample_config.pop('database')

        with open(config_file, 'w') as f:
            import yaml
            yaml.dump(sample_config, f)

        config = Config(str(config_file))
        assert config.db_host == 'localhost'
        assert config.db_port == 5432
        assert config.db_name == 'whale_watcher'
        assert config.db_user == 'admin'
        assert config.db_password == ''


class TestWhaleHelperMethods:
    """Test whale helper methods."""

    def test_get_whale_by_cik_found(self, config_file: Path) -> None:
        """Test get_whale_by_cik returns whale when found."""
        config = Config(str(config_file))
        whale = config.get_whale_by_cik('0001234567')
        assert whale is not None
        assert whale['name'] == 'Test Whale 1'
        assert whale['cik'] == '0001234567'

    def test_get_whale_by_cik_normalized(self, config_file: Path) -> None:
        """Test get_whale_by_cik normalizes CIK to 10 digits."""
        config = Config(str(config_file))
        # Search with shorter CIK (without leading zeros)
        whale = config.get_whale_by_cik('1234567')
        assert whale is not None
        assert whale['name'] == 'Test Whale 1'

    def test_get_whale_by_cik_not_found(self, config_file: Path) -> None:
        """Test get_whale_by_cik returns None when not found."""
        config = Config(str(config_file))
        whale = config.get_whale_by_cik('9999999999')
        assert whale is None

    def test_get_whale_by_name_found(self, config_file: Path) -> None:
        """Test get_whale_by_name returns whale when found."""
        config = Config(str(config_file))
        whale = config.get_whale_by_name('Test Whale 1')
        assert whale is not None
        assert whale['name'] == 'Test Whale 1'

    def test_get_whale_by_name_case_insensitive(self, config_file: Path) -> None:
        """Test get_whale_by_name is case-insensitive."""
        config = Config(str(config_file))
        whale = config.get_whale_by_name('test whale 1')
        assert whale is not None
        assert whale['name'] == 'Test Whale 1'

    def test_get_whale_by_name_not_found(self, config_file: Path) -> None:
        """Test get_whale_by_name returns None when not found."""
        config = Config(str(config_file))
        whale = config.get_whale_by_name('Nonexistent Whale')
        assert whale is None


class TestLoadConfigFunction:
    """Test load_config helper function."""

    def test_load_config_returns_config_instance(self, config_file: Path) -> None:
        """Test load_config returns a Config instance."""
        config = load_config(str(config_file))
        assert isinstance(config, Config)
        assert config.user_agent == 'TestAgent/1.0 (test@example.com)'

    def test_load_config_with_none(self) -> None:
        """Test load_config with None uses default path."""
        # Assumes config/whales.yaml exists in the project
        config = load_config(None)
        assert isinstance(config, Config)
        assert config.config_path.name == 'whales.yaml'
