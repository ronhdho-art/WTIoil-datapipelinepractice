# Note that eia_supply, eia_prices, eia_storage later feeds into bronze_ingest.py which orchestrates all three.
# Each ingestion script is thin and focused on a single source + data domain.
# That is, the goal of bronze_ingest.py will be to orchestrate/run all three ingestion scripts together in databricks or local runs

import os

import pandas as pd

from pipelines.bronze._bronze_writer import write_bronze
from src.io.eia_client import fetch_series, series_to_frame

us_crude_production_series = "PET.MCRFPUS2.W"  # EIA series ID for US crude oil production, "PET"=petroleum, "MCRFPUS2"=weekly US crude production, "W"=weekly

# The purpose of this file is  to fetch US crude oil production series (supply) from EIA and write bronze output.
# Uses shared IO client + writer for consistency across ingestion scripts

# Walkthrough step 3 (includes eia_supply.py, eia_prices.py, eia_storage.py): 

def load_supply(api_key: str | None = None) -> pd.DataFrame: 
    # weekly US crude production series for supply leg of the model
    
    # as before, we have:
    series = fetch_series(us_crude_production_series, api_key=api_key)
    df = series_to_frame(series)
    df["source_type"] = "supply"

    return df

def main() -> None:
    api_key = os.getenv("EIA_API_KEY")  
    df = load_supply(api_key=api_key)
    path, fmt = write_bronze(df, "bronze_eia_supply")
    print(f"Wrote {len(df)} rows to {path} ({fmt})")        

if __name__ == "__main__":
    main()

