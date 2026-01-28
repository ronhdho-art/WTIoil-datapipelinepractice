import json
import os
from typing import Dict, List
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

# The purpose here is to fetch raw EIA time series and convert them into a clean dataframe
# for the bronze ingestion scripts in the pipelines/bronze/*.py files, which then write to storage.
# This keeps API-specific logic isolated from pipeline orchestration.

# Walkthrough Step 1:
# Implement a source client that knows how to call an external API and return
# clean tabular data (you'll reuse this pattern for any new source).

base_url = "https://api.eia.gov/series/" # note that error encountered is fine and is expected. 
# here we hit the endpoint without any query params so API correctly rejects it because we have yet to authenticate and specified the data we want
# eia.gov is actually the official US govt source for energy data
# so we'll build out urls based on what we need. Our project specifically uses oil prices, storage levels, and supply data. 
# Hence, we'll have eia_prices.py, eia_storage.py, eia_supply.py files in the io/ directory

def fetch_series(series_id: str, api_key: str | None = None) -> Dict[str, List[List[str]]]:
    if api_key is None:
        api_key = os.getenv("EIA_API_KEY") # env lookup returns None if unset
    if not api_key:
        raise ValueError("EIA_API_KEY is required")
    
    # urlencode() builds the query string like "api_key=...&series_id=..."
    query = urlencode({"api_key": api_key, "series_id": series_id})
    url = f"{base_url}?{query}"
    with urlopen(url) as response:
        payload = json.loads(response.read().decode("utf-8")) # the reason why we use utf-8 is because the API returns data in utf-8 encoding.
    
    series_list = payload.get("series", []) # recall, dict.get returns default [] if key missing

    if not series_list: # ie: defaulted to []
        raise ValueError(f"No series data returned for {series_id}")
    return series_list[0] # return the first entry in the list

def series_to_frame(series: Dict[str, List[List[str]]]) -> pd.DataFrame:
    # Why Dict[str, List[List[str]]]? Because the series dict has keys that are strings, and the values are lists of lists of strings (the data field).
    # and List[List[str]] represents a list of rows, where each row is a list of strings (date and value).
    # the data field contains rows like ["2024-01-05", 75.12]
    data = series.get("data", []) # list of [date, value] pairs

    # note that we pass the following single series object from the EIA API response: 
    #     {
    #     "series_id": "PET.MCREXUS1.M",
    #     "name": "Crude Oil WTI Spot Price",
    #     "units": "Dollars per Barrel",
    #     "data": [
    #         ["2024-01-05", "75.12"],
    #         ["2024-01-04", "74.50"],
    #         ...
    #     ]
    # }
    # ie: we are guaranteed to have a "data" field in the series dict 

    # pd.DataFrame builds a table; columns assigns names to each position in data
    df = pd.DataFrame(data, columns = ["date", "value"]) # converting to df and naming the columns of 'data'

    # pd.to_datetime converts date strings to datetime; errors="coerce" -> NaT on bad input
    df["date"] = pd.to_datetime(df['date'], errors = 'coerce') 

    # keep the series_id so downstream joins know which signal it is
    df["series_id"] = series.get("series_id") # metadata copied into each row

    # df[["col1", "col2"]] selects a subset of columns in a specific order.
    # Using double brackets returns a DataFrame (not a Series), which keeps the
    # tabular shape consistent for downstream merges and exports.
    return df[["date", "series_id", "value"]]

    # Beautiful! We've successfully built the EIA client that can fetch series data and convert it to a clean dataframe.
    # We can now move on to building the bronze ingestion scripts that will use this client to fetch data and write it to storage.
    # The dataframe will have the following structure:  
    #         date       |   series_id        |   value
    #     ---------------------------------------------
    #     2024-01-05   | PET.MCREXUS1.M    |   75.12
    #     2024-01-04   | PET.MCREXUS1.M    |   74.50
    #     ...          | ...               |   ...