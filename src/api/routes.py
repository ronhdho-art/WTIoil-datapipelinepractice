# Ok, we've routed data into Postgres tables using src/db/models.py and src/db/session.py.
# Note: we should fill more here in later about what these files do and connect

# The goal now to serve regimes, signals, and forecasts to users and apps via API.
# We build HTTP endpoints here that read PostgreSQL tables defined in src/db/models.py.
# The db session is provided by src/db/session.py, making routes thin and focused.

# Walkthrough Step 9:
# We expose curated read endpoints so users/apps can retrieve regimes, signals, and forecasts
# without touching raw tables directly.
# get_regime: pull the most recent regime record for this commodity
# get_signals: return the last N features for the commodity
# get_forecast: retrieve the most recent forecast for a given commodity and horizon_weeks

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

# recall, Forecast, GoldFeature, RegimeState are ORM models defined in src/db/models.py
# Forecast is the table for model forecasts
# RegimeState is the table for regime data
# GoldFeature is the table for gold features
from src.db.models import Forecast, GoldFeature, RegimeState

# recall, get_session is the dependency that provides a DB session per request
from src.db.session import get_session

# APIRouter groups endpoints so main.py can include them at once
router = APIRouter()

@router.get("/health")
def health() -> dict:
    # sanity status check endpoint used by deploys and local checks
    return {"status": "ok"}

# ie: what get_regime does is: pull the most recent regime record for this commodity
@router.get("/regime/{commodity}")
def get_regime(commodity: str, session: Session = Depends(get_session)) -> dict:
    # ie: Depends(get_session) injects a DB session created in src/db/session.py.
    # select() builds a SQLAlchemy Select object (not executed yet).

    # Pull the most recent regime record for this commodity.
    query = (
        select(RegimeState)
        .where(RegimeState.commodity == commodity) # SQL WHERE commodity = :commodity
        .order_by(desc(RegimeState.week)) # SQL ORDER BY week, DESC
        .limit(1) # SQL LIMIT 1/see first record only
    )

    # recall, execute() runs the SQL; scalar_one_or_none() unwraps a single ORM row or None.
    # session.execute() sends SQL to the DB; scalar_one_or_none() unwraps one row.
    row = session.execute(query).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="No regime data found")
    return {
        "commodity": row.commodity,
        "week": row.week.isoformat(), # date -> ISO string for JSON, that is, isoformat() is a datetime method that converts date to ISO 8601 string
        "regime_label": row.regime_label,
        "regime_score": row.regime_score,
    }

# ie: what get_signals does is: return the last N features for the commodity
@router.get("/signals/{commodity}")
def get_signals(commodity: str, limit: int = 52, session: Session = Depends(get_session)) -> dict:
    # return the last N features for the commodity
    
    # recall, select() builds a SQL statement, session.execute runs it.
    # select() + where/order_by/limit forms the SQL query for recent features.
    query = (
        select(GoldFeature)
        .where(GoldFeature.commodity == commodity) # filter for one commodity
        .order_by(desc(GoldFeature.week)) # newest weeks first
        .limit(limit) # limit query to N rows
    )

    # recall, scalars() turns Row objects into model instances; all() materializes the list.
    # scalars() unwraps ORM objects; all() materializes a list in memory.
    rows = session.execute(query).scalars().all()
    if not rows:
        raise HTTPException(status_code = 404, detail = "No signals found")
    return {
        "commodity": commodity,
        "signals": [
            {
                "week": row.week.isoformat(), # recall, isoformat() converts date to ISO 8601 string
                "feature_name": row.feature_name, 
                "feature_value": row.feature_value,
            }
            for row in rows
        ],
    }

# ie: what get_forecast does is: retrieve the most recent forecast for a given commodity and horizon_weeks
@router.get("/forecasts/{commodity}")
def get_forecast(commodity: str, horizon_weeks: int = 4, session: Session = Depends(get_session)) -> dict:
    # select most recent forecast for a horizon
    # The filter includes both commodity and horizon_weeks to get the right forecast.
    # This query filters by commodity AND horizon_weeks, then takes the latest week.
    query = (
        select(Forecast)
        .where(
            Forecast.commodity == commodity, 
            Forecast.horizon_weeks == horizon_weeks,
        )
        .order_by(desc(Forecast.week))
        .limit(1)
    )
    row = session.execute(query).scalar_one_or_none() # recall execute() runs SQL; scalar_one_or_none() unwraps single ORM row or None
    if row is None:
        raise HTTPException(status_code=404, detail = "No forecast found")
    return {
        "commodity": row.commodity,
        "week": row.week.isoformat(), 
        "horizon_weeks": row.horizon_weeks,
        "forecast_value": row.forecast_value
    }
