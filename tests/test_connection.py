"""Tests for database connection and schema utilities."""

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session

from whale_watcher.database.connection import DatabaseConnection
from whale_watcher.database.models import Base, Filer, Filing, Holding, PositionChange
from whale_watcher.database.schema import create_tables, drop_tables, init_database


class TestDatabaseConnection:
    """Test DatabaseConnection class."""

    def test_create_connection(self) -> None:
        """Test creating a database connection."""
        db_url = "sqlite:///:memory:"
        db = DatabaseConnection(db_url)

        assert db.engine is not None
        assert db.SessionLocal is not None
        db.close()

    def test_get_session(self) -> None:
        """Test getting a database session."""
        db_url = "sqlite:///:memory:"
        db = DatabaseConnection(db_url)

        session = db.get_session()
        assert isinstance(session, Session)

        session.close()
        db.close()

    def test_session_context_manager(self) -> None:
        """Test using session as context manager."""
        db_url = "sqlite:///:memory:"
        db = DatabaseConnection(db_url)

        with db.session_scope() as session:
            assert isinstance(session, Session)
            # Session should be active
            assert session.is_active

        # Session should be closed after context
        db.close()

    def test_close_connection(self) -> None:
        """Test closing database connection."""
        db_url = "sqlite:///:memory:"
        db = DatabaseConnection(db_url)

        session = db.get_session()
        session.close()

        # Should not raise exception
        db.close()

    def test_multiple_sessions(self) -> None:
        """Test that get_session returns same scoped session."""
        db_url = "sqlite:///:memory:"
        db = DatabaseConnection(db_url)

        session1 = db.get_session()
        session2 = db.get_session()

        # Scoped sessions should return same instance
        assert session1 is session2

        session1.close()
        db.close()


class TestSchemaUtilities:
    """Test schema creation and management utilities."""

    def test_create_tables(self) -> None:
        """Test creating all tables."""
        engine = create_engine("sqlite:///:memory:")
        create_tables(engine)

        inspector = inspect(engine)
        tables = inspector.get_table_names()

        assert "filers" in tables
        assert "filings" in tables
        assert "holdings" in tables
        assert "position_changes" in tables

        engine.dispose()

    def test_drop_tables(self) -> None:
        """Test dropping all tables."""
        engine = create_engine("sqlite:///:memory:")
        create_tables(engine)

        # Verify tables exist
        inspector = inspect(engine)
        assert len(inspector.get_table_names()) > 0

        # Drop tables
        drop_tables(engine)

        # Verify tables are gone
        inspector = inspect(engine)
        assert len(inspector.get_table_names()) == 0

        engine.dispose()

    def test_create_tables_idempotent(self) -> None:
        """Test that create_tables can be called multiple times safely."""
        engine = create_engine("sqlite:///:memory:")

        # Create tables twice
        create_tables(engine)
        create_tables(engine)  # Should not raise exception

        inspector = inspect(engine)
        tables = inspector.get_table_names()

        assert "filers" in tables
        assert "filings" in tables
        assert "holdings" in tables
        assert "position_changes" in tables

        engine.dispose()

    def test_init_database(self) -> None:
        """Test database initialization."""
        # Use temp file for SQLite so it persists across engine instances
        import tempfile
        import os

        # Create temp file
        fd, temp_db = tempfile.mkstemp(suffix=".db")
        os.close(fd)

        try:
            db_url = f"sqlite:///{temp_db}"

            # Initialize database (creates tables)
            init_database(db_url)

            # Verify tables were created with new engine
            engine = create_engine(db_url)
            inspector = inspect(engine)
            tables = inspector.get_table_names()

            assert "filers" in tables
            assert "filings" in tables
            assert "holdings" in tables
            assert "position_changes" in tables

            engine.dispose()
        finally:
            # Cleanup temp file
            if os.path.exists(temp_db):
                os.unlink(temp_db)

    def test_table_indexes_created(self) -> None:
        """Test that indexes are created on tables."""
        engine = create_engine("sqlite:///:memory:")
        create_tables(engine)

        inspector = inspect(engine)

        # Check filers indexes
        filers_indexes = inspector.get_indexes("filers")
        index_names = [idx["name"] for idx in filers_indexes]
        # SQLite creates index for unique constraint automatically
        assert any("cik" in name for name in index_names)

        # Check filings indexes
        filings_indexes = inspector.get_indexes("filings")
        index_names = [idx["name"] for idx in filings_indexes]
        assert any("accession_number" in name for name in index_names)

        # Check holdings indexes
        holdings_indexes = inspector.get_indexes("holdings")
        index_names = [idx["name"] for idx in holdings_indexes]
        # Should have composite index on (filing_id, cusip)
        assert any("filing_cusip" in name or "filing_id" in name for name in index_names)

        engine.dispose()

    def test_table_foreign_keys_created(self) -> None:
        """Test that foreign key constraints are created."""
        engine = create_engine("sqlite:///:memory:")
        create_tables(engine)

        inspector = inspect(engine)

        # Check filings foreign keys
        filings_fks = inspector.get_foreign_keys("filings")
        assert len(filings_fks) > 0
        assert any(fk["referred_table"] == "filers" for fk in filings_fks)

        # Check holdings foreign keys
        holdings_fks = inspector.get_foreign_keys("holdings")
        assert len(holdings_fks) > 0
        assert any(fk["referred_table"] == "filings" for fk in holdings_fks)

        # Check position_changes foreign keys
        pc_fks = inspector.get_foreign_keys("position_changes")
        assert len(pc_fks) > 0
        assert any(fk["referred_table"] == "filers" for fk in pc_fks)
        assert any(fk["referred_table"] == "filings" for fk in pc_fks)

        engine.dispose()


class TestDatabaseIntegration:
    """Integration tests for database connection and schema."""

    def test_full_database_workflow(self) -> None:
        """Test complete database setup and usage workflow."""
        # Create connection first, then use its engine
        db_url = "sqlite:///:memory:"
        db = DatabaseConnection(db_url)

        # Initialize database using the same engine
        create_tables(db.engine)

        # Get session and create data
        with db.session_scope() as session:
            filer = Filer(
                cik="0001067983",
                name="Berkshire Hathaway",
                category="value_investing",
                enabled=True
            )
            session.add(filer)
            session.commit()

            # Verify filer was created
            assert filer.id is not None

            # Create filing
            from datetime import date
            filing = Filing(
                filer_id=filer.id,
                accession_number="0001193125-25-001234",
                filing_date=date(2025, 2, 14),
                period_of_report=date(2024, 12, 31)
            )
            session.add(filing)
            session.commit()

            assert filing.id is not None

        # Verify data persists in new session
        with db.session_scope() as session:
            filers = session.query(Filer).all()
            assert len(filers) == 1
            assert filers[0].name == "Berkshire Hathaway"

        db.close()

    def test_database_rollback_on_error(self) -> None:
        """Test that transactions rollback on error."""
        db_url = "sqlite:///:memory:"
        db = DatabaseConnection(db_url)

        # Initialize database using the same engine
        create_tables(db.engine)

        try:
            with db.session_scope() as session:
                filer = Filer(
                    cik="0001067983",
                    name="Test Filer",
                    category="test",
                    enabled=True
                )
                session.add(filer)
                session.commit()

                # Try to add duplicate CIK (should fail)
                filer2 = Filer(
                    cik="0001067983",
                    name="Duplicate Filer",
                    category="test",
                    enabled=True
                )
                session.add(filer2)
                session.commit()
        except Exception:
            pass  # Expected to fail

        # Verify only one filer exists
        with db.session_scope() as session:
            filers = session.query(Filer).all()
            assert len(filers) == 1

        db.close()
