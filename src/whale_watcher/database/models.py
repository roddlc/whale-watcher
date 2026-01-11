"""SQLAlchemy ORM models for whale-watcher database."""

import enum
from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all database models."""

    pass


class TimestampMixin:
    """Mixin for created_at and updated_at timestamps."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )


class ChangeType(str, enum.Enum):
    """Position change type enumeration."""

    NEW = "NEW"
    CLOSED = "CLOSED"
    INCREASED = "INCREASED"
    DECREASED = "DECREASED"
    UNCHANGED = "UNCHANGED"


class Filer(Base, TimestampMixin):
    """Institutional investor (whale) model."""

    __tablename__ = "filers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    cik: Mapped[str] = mapped_column(String(10), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Relationships
    filings: Mapped[List["Filing"]] = relationship(
        "Filing",
        back_populates="filer",
        cascade="all, delete-orphan"
    )
    position_changes: Mapped[List["PositionChange"]] = relationship(
        "PositionChange",
        back_populates="filer",
        foreign_keys="[PositionChange.filer_id]",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Filer(cik='{self.cik}', name='{self.name}')>"


class Filing(Base, TimestampMixin):
    """13F filing metadata model."""

    __tablename__ = "filings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filer_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("filers.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    accession_number: Mapped[str] = mapped_column(
        String(20),
        unique=True,
        nullable=False,
        index=True
    )
    filing_date: Mapped[date] = mapped_column(Date, nullable=False)
    period_of_report: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    total_value: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    holdings_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    processed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Relationships
    filer: Mapped["Filer"] = relationship("Filer", back_populates="filings")
    holdings: Mapped[List["Holding"]] = relationship(
        "Holding",
        back_populates="filing",
        cascade="all, delete-orphan"
    )

    # Constraints
    __table_args__ = (
        UniqueConstraint("filer_id", "period_of_report", name="uq_filer_period"),
    )

    def __repr__(self) -> str:
        return f"<Filing(accession='{self.accession_number}', period='{self.period_of_report}')>"


class Holding(Base, TimestampMixin):
    """Individual stock position within a filing."""

    __tablename__ = "holdings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filing_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("filings.id", ondelete="CASCADE"),
        nullable=False
    )
    cusip: Mapped[str] = mapped_column(String(9), nullable=False, index=True)
    security_name: Mapped[str] = mapped_column(String(200), nullable=False)
    shares: Mapped[int] = mapped_column(BigInteger, nullable=False)
    market_value: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="Market value in thousands of dollars"
    )
    voting_authority_sole: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    voting_authority_shared: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    voting_authority_none: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    discretion: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Relationships
    filing: Mapped["Filing"] = relationship("Filing", back_populates="holdings")

    # Indexes
    __table_args__ = (
        Index("ix_holdings_filing_cusip", "filing_id", "cusip"),
    )

    def __repr__(self) -> str:
        return f"<Holding(cusip='{self.cusip}', name='{self.security_name}', shares={self.shares})>"


class PositionChange(Base, TimestampMixin):
    """Quarter-over-quarter position change analysis."""

    __tablename__ = "position_changes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filer_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("filers.id", ondelete="CASCADE"),
        nullable=False
    )
    cusip: Mapped[str] = mapped_column(String(9), nullable=False)
    security_name: Mapped[str] = mapped_column(String(200), nullable=False)

    # Previous quarter data
    prev_filing_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("filings.id", ondelete="SET NULL"),
        nullable=True
    )
    prev_period: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    prev_shares: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    prev_market_value: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Market value in thousands of dollars"
    )

    # Current quarter data
    curr_filing_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("filings.id", ondelete="CASCADE"),
        nullable=False
    )
    curr_period: Mapped[date] = mapped_column(Date, nullable=False)
    curr_shares: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    curr_market_value: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Market value in thousands of dollars"
    )

    # Calculated changes
    shares_change: Mapped[int] = mapped_column(BigInteger, nullable=False)
    shares_change_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    value_change: Mapped[int] = mapped_column(BigInteger, nullable=False)
    change_type: Mapped[ChangeType] = mapped_column(
        Enum(ChangeType, native_enum=False),
        nullable=False
    )

    # Relationships
    filer: Mapped["Filer"] = relationship(
        "Filer",
        back_populates="position_changes",
        foreign_keys=[filer_id]
    )
    prev_filing: Mapped[Optional["Filing"]] = relationship(
        "Filing",
        foreign_keys=[prev_filing_id]
    )
    curr_filing: Mapped["Filing"] = relationship(
        "Filing",
        foreign_keys=[curr_filing_id]
    )

    # Indexes
    __table_args__ = (
        Index("ix_position_changes_filer_period_type", "filer_id", "curr_period", "change_type"),
    )

    def __repr__(self) -> str:
        return (
            f"<PositionChange(cusip='{self.cusip}', "
            f"type={self.change_type}, shares_change={self.shares_change})>"
        )
