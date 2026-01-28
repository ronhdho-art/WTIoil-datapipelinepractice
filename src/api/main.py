from fastapi import FastAPI

from src.api.routes import router

# Walkthrough Step 10: 
# We provide a single ASGI entrypoint for deployment (Uvicorn/Gunicorn).
# importing src.api.routes to get the router object that contains all API endpoints


# The purpose of this FastAPI application is to serve as the entrypoint for the API.
# It wires together the routes defined in src/api/routes.py into a FastAPI app instance.
# Keeps startup minimal so ASGI servers can import the app quickly.

# FastAPI() constructs the ASGI application object used by Uvicorn/Gunicorn.
app = FastAPI(title="Oil Regime API", version = "0.1.0")

# Routes are defined in src/api/routes.py and wired here.
# This file is intentionally small so app creation is isolated for ASGI servers.
# include_router() registers all endpoints from routes.py onto this app.
app.include_router(router) # recall, include_router is a FastAPI method that adds routes from an APIRouter instance