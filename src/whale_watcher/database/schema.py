"""Database schema creation and management utilities."""

from sqlalchemy import Engine, create_engine

from whale_watcher.database.models import Base
from whale_watcher.utils.logger import get_logger

logger = get_logger(__name__)


def create_tables(engine: Engine) -> None:
    """
    Create all tables defined in the database models.

    This function is idempotent - it's safe to call multiple times.
    Tables that already exist will not be recreated.

    Args:
        engine: SQLAlchemy engine instance
    """
    logger.info("Creating database tables...")
    Base.metadata.create_all(engine)
    logger.info("Database tables created successfully")


def drop_tables(engine: Engine) -> None:
    """
    Drop all tables defined in the database models.

    WARNING: This will delete all data in the database.

    Args:
        engine: SQLAlchemy engine instance
    """
    logger.warning("Dropping all database tables...")
    Base.metadata.drop_all(engine)
    logger.info("Database tables dropped successfully")


def init_database(database_url: str, drop_existing: bool = False) -> None:
    """
    Initialize database by creating all tables.

    Args:
        database_url: SQLAlchemy database URL
        drop_existing: If True, drop existing tables before creating (default: False)
    """
    logger.info(f"Initializing database: {database_url}")

    engine = create_engine(database_url)

    try:
        if drop_existing:
            drop_tables(engine)

        create_tables(engine)
        logger.info("Database initialization complete")

    finally:
        engine.dispose()
