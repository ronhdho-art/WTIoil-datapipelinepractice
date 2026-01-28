# Note that eia_supply, eia_prices, eia_storage later feeds into bronze_ingest.py which orchestrates all three.
# Each ingestion script is thin and focused on a single source + data domain.
# That is, the goal of bronze_ingest.py will be to orchestrate/run all three ingestion scripts together in databricks or local ru

import os

import pandas as pd

from pipelines.bronze._bronze_writer import write_bronze
from src.io.eia_client import fetch_series, series_to_frame

crude_inventory_series = "PET.WCESTUS1.W"  # EIA series ID for US crude oil inventories, "PET"=petroleum, "WCESTUS1"=weekly US crude inventory, "W"=weekly

# Walkthrough step 3 (includes eia_supply.py, eia_prices.py, eia_storage.py): 
# The purpose of this file is to fetch US crude inventory series (storage) and write bronze output
# It will use shared IO client + writer for consistency across ingestion scripts

def load_storage(api_key: str | None = None) -> pd.DataFrame: # recall, | None = None means the arg is optional
    # weekly US crude inventory levels for storage dynamics
    # fetch_series hits the EIA API and returns a dict payload for this series_id 
    series = fetch_series(crude_inventory_series, api_key=api_key)
    df = series_to_frame(series)

    # source_type labels help route the data to the correct feature logic
    df["source_type"] = "storage"
    return df

def main() -> None:
    # os.getenv reads environment variables; returns None if not set
    api_key = os.getenv("EIA_API_KEY") # recall,  .getenv() reads environment variables (that we set elsewhere), and it looks for an env var named "EIA_API_KEY"
    df = load_storage(api_key=api_key)
    path, fmt = write_bronze(df, "bronze_eia_storage")
    print(f"Wrote {len(df)} rows to {path} ({fmt})")   # logging: how many rows we wrote, where, and in what format

if __name__ == "__main__":
    main()
