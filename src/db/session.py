# The purpose of this file is to centralize db connectivity for API routes in src/api/routes.py
# This would isolate connection details and keep per-request sessions consistent. 
# ie: provide reliable db sessions to API route handlers without leaking connections.

# Walkthrough Step 8:
# We create a reusable db session factory so the API can safely access PostgreSQL without 
# opening/closing connections manually per query.

import os
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

def _build_database_url() -> str:
    # reads db_url from the environment so the API can point at dev/prod PostgreSQL without changing code
    # os.getenv fetches environment variables; returns None if missing
    url = os.getenv('db_url')
    if not url:
        raise ValueError("db_url is required for database access")
    return url

# Create a SQLAlchemy Engine once. It manages connection pooling internally. 
# create_engine creates the database connection pool and DBAPI/database api adapter
engine = create_engine(_build_database_url(), pool_pre_ping = True)

# A central sesison factory used by the API dependency in src/api/routes.py
# SessionLocal creates a short-lived/temporary db session per request
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush= False) 

def get_session() -> Generator[Session, None, None]:
    # Note that FastAPI depends on this generator to open/close db sessions per request
    # SessionLocal() returns a Session object tied to the Engine's connection pool
    # It does not open a connection until the first query is executed
    session = SessionLocal()
    try: 
        yield session # yields control back to the route handler
    finally:
        session.close() # always closes, even if the route raises something

        
