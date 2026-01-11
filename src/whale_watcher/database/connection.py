"""Database connection management for whale-watcher."""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker


class DatabaseConnection:
    """Database connection manager with session handling."""

    def __init__(self, database_url: str, echo: bool = False):
        """
        Initialize database connection.

        Args:
            database_url: SQLAlchemy database URL
            echo: Whether to echo SQL statements (for debugging)
        """
        self.database_url = database_url
        self.engine: Engine = create_engine(database_url, echo=echo)
        self.SessionLocal = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        )

    def get_session(self) -> Session:
        """
        Get a database session.

        Returns:
            Scoped session instance
        """
        return self.SessionLocal()

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope around a series of operations.

        Yields:
            Database session

        Example:
            with db.session_scope() as session:
                filer = Filer(cik="123", name="Test")
                session.add(filer)
                # Commit happens automatically on exit
        """
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def close(self) -> None:
        """Close all database connections and dispose of engine."""
        self.SessionLocal.remove()
        self.engine.dispose()

    def __enter__(self) -> "DatabaseConnection":
        """Support context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        """Support context manager protocol."""
        self.close()
