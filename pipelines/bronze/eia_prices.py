import os

import pandas as pd

from pipelines.bronze._bronze_writer import write_bronze
from src.io.eia_client import fetch_series, series_to_frame

wti_spot_series = "PET.RWTC.D" # this is the EIA series ID for WTI spot prices, "PET" means petroleum, "RWTC" means regular WTI crude, "D" means daily

# The purpose of this file is to fetch WTI spot prices from EIA and write to bronze storage. 
# Recall, we kept API-specific logic in src/io/eia_client.py isolated from pipeline orchestration.
# We are now orchestrating the ingestion here.
# We use src/io/eia_client.py for API retrieval and _bronze_writer.py for output

# 
# Note that eia_supply, eia_prices, eia_storage later feeds into bronze_ingest.py which orchestrates all three.
# Each ingestion script is thin and focused on a single source + data domain.
# That is, the goal of bronze_ingest.py will be to orchestrate/run all three ingestion scripts together in databricks or local ru

# Walkthrough step 3 (includes eia_supply.py, eia_prices.py, eia_storage.py): 
# We build a thin ingestion script per source that calls the client and writes a standardized bronze output
# This will be raw + append-only.

def load_prices(api_key: str | None = None) -> pd.DataFrame: # recall, | None = None means the arg is optional
    # fetch raw series JSON and convert into a tidy 3-column df.
    # we use fetch_series to hit the EIA API and return a dict payload for this series_id
    series = fetch_series(wti_spot_series, api_key=api_key)
    df = series_to_frame(series)
    # tagging the source_type lets downstream dispatch choose feature logic
    df["source_type"] = "prices"
    return df

def main() -> None:
    # pull API key from env to avoid hardcoding secrets in code
    # os.getenv reads environment variables; returns None if not set
    api_key = os.getenv("EIA_API_KEY") # recall,  .getenv() reads environment variables (that we set elsewhere), and it looks for an env var named "EIA_API_KEY"
    
    # load the prices data from EIA into a df
    df = load_prices(api_key=api_key)

    # write to bronze storage with a time-stamped file name
    path, fmt = write_bronze(df, "bronze_eia_prices") # we pass in the df and the table name
    print(f"Wrote {len(df)} rows to {path} ({fmt})") # logging: how many rows we wrote, where, and in what format


# standard Python idiom to allow or prevent parts of code from being run when the modules are imported
if __name__ == '__main__':
    main() 
