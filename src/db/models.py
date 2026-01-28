# The purpose of this file is to define ORM table definitions for Postgres used by src/api/routes.py.
# These models describe the shape of gold features, regimes, and forecasts.
# The reason why we use SQLAlchemy ORM is to provide a clear and structured way to interact with the 
# database using Python classes.
# SQLAlchemy is a popular SQL toolkit and Object-Relational Mapping (ORM) library for Python.
# It is unique in that it provides a full suite of well-known enterprise-level persistence patterns,
# designed for efficient and high-performing database access, adapted into a simple and Pythonic domain language

# Walkthrough Step 7:
# Define storage schema for the artifacts you want to serve (features, regimes, forecasts).
# This becomes the contract between pipelines and the API.

from datetime import datetime

from sqlalchemy import Date, DateTime, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    # Declarative base shared by all ORM models in this project.
    # This keeps table metadata centralized for migrations/DDL.
    # That is, we can manage database schema changes in one place and abstracts away some of the boilerplate.
    pass

class GoldFeature(Base):
    __tablename__ = 'gold_features'

    # !! - the following are: ORM mapped attributes will contain a specific type of object

    # Mapped[...] tells SQLAlchemy this is an ORM-mapped attribute.
    # Integer maps to a SQL INTEGER type for primary keys.
    id: Mapped[int] = mapped_column(Integer, primary_key = True, autoincrement = True)
    # String(32) maps to VARCHAR(32); index = True asks the database (db) to index this column
    commodity: Mapped[str] = mapped_column(String(32), index = True)
    # Date maps to a date-only column (w/ no time component)
    week: Mapped[datetime] = mapped_column(Date, index = True)

    # These have downstream uses in: /signals/{commodity} route in src/api/routes.py
    # Each row is one feature/value for one week

    # String(128) stores the feature name; index speeds up name filtering.
    feature_name: Mapped[str] = mapped_column(String(128), index = True)
    # Float maps to a floating-point numeric column for feature values
    feature_value: Mapped[float] = mapped_column(Float)
    # DateTime maps to a timestamp; default supplies a value on insert
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now((datetime.timezone.utc)))


class RegimeState(Base): 
    __tablename__ = 'regime_states'

    id: Mapped[int] = mapped_column(Integer, primary_key = True, autoincrement = True)
    commodity: Mapped[int] = mapped_column(String(32), index = True)
    week: Mapped[datetime] = mapped_column(Date, index = True)

    # Latest value is served by /regime/{commodity}
    # regime_label is a string-like 'tight-suply' or 'risk_off'
    # String(64) holds the regime label text, a sort of short descriptor
    regime_label: Mapped[str] = mapped_column(String(64), index = True)
    # Float type with nullable = True allows for missing score; Mapped[float | None] matches that
    regime_score: Mapped[float|None] = mapped_column(Float, nullable=True)
    # again, DateTime maps to a timestamp; default supplies a value on insert
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now((datetime.timezone.utc)))

class Forecast(Base):
    __tablename__ = 'forecasts'

    id: Mapped[int] = mapped_column(Integer, primary_key = True, autoincrement = True)
    commodity: Mapped[str] = mapped_column(String(32), index = True)
    week: Mapped[datetime] = mapped_column(Date, index = True)

    # Integer stores horizon in weeks for easy filtering and ordering
    horizon_weeks: Mapped[int] = mapped_column(Integer, index = True)

    # /forecast/{commodity} returns the most recent matching horizon
    # horizon_weeks let's us store multiple horizons in one table
    forecast_value: Mapped[Float] = mapped_column(Float)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now((datetime.timezone.utc)))

