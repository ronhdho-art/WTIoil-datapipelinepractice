# Ok, now that we've made an ingestion pipeline to get data into bronze tables,
# and made silver tables that had standardized data, 
# we can build a transformation pipeline to create gold features which consumes silver outputs 
# and emits regime-ready features, purpose built for modeling, analytics, and API serving.

# This is a databricks notebook that builds gold features like returns and volatilities from silver tables.
# That is, we make standardized data specific to purpose. 
# We read silver tables, compute features, and write gold tables.

# Walkthrough Step 6: 
# We build gold features and regime aggregates that the API and models will serve.

from __future__ import annotations  # for forward compatibility with future Python versions

from pathlib import Path 

import pandas as pd

silver_root = Path("data/silver")  # directory where silver tables are stored
gold_root = Path("data/gold")  # directory where gold tables will be stored

# recall, this is is exactly the same helper function as in silver_clean.py
# We're reusing it here to find the latest silver files but we won't import it from silver for clarity.
# If we happen to use it in many places, we could refactor it into a shared utils module.
def _latest_file(table_dir: Path) -> Path:
    files = sorted(table_dir.glob("*.parquet")) or sorted(table_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No silver files found in {table_dir}")
    return files[-1] # return the last file in sorted order, which is the most recent

# reading silver tables (similar to _read_bronze in silver_clean.py)
def _read_silver(table: str) -> pd.DataFrame:
    table_dir = silver_root/table # construct the path to the silver table directory
    latest = _latest_file(table_dir)
    if latest.suffix == ".parquet":
        return pd.read_parquet(latest) 
    return pd.read_csv(latest) # read csv file into a df, as a fallback.

def _write_gold(df: pd.DataFrame, table: str) -> Path:
    gold_root.mkdir(parents=True, exist_ok=True) # ensure the gold root directory exists
    out_dir = gold_root/table # construct the path to the gold table directory
    out_dir.mkdir(parents=True, exist_ok=True) # ensure the gold table directory exists
    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ") 
    path = out_dir/f"{timestamp}.parquet" # construct the output file path with the timestamp
    df.to_parquet(path, index = False)
    return path

# feature engineering functions for each data source (to make data purpose-built)
def _price_features(prices: pd.DataFrame) -> pd.DataFrame:
    prices = prices.copy()
    prices = prices.sort_values("date") # .sort_values returns a new df sorted by date
    prices["return_1w"] = prices["value"].pct_change() # compute 1-week percentage change
    prices["vol_4w"] = prices["return_1w"].rolling(4).std() # compute 4-week rolling volatility
    return prices[["date", "value", "return_1w", "vol_4w"]] # recall, [[]] returns a df with selected columns

def _supply_features(supply: pd.DataFrame) -> pd.DataFrame:
    supply = supply.copy()
    supply = supply.sort_values("date")
    supply["supply_delta"] = supply["value"].diff() # compute week-over-week difference
    return supply[["date", "value", "supply_delta"]] 

def _storage_features(storage: pd.DataFrame) -> pd.DataFrame:
    storage = storage.copy()
    storage = storage.sort_values("date")
    storage["inventory_delta"] = storage["value"].diff() # compute week-over-week difference
    return storage[["date", "value", "inventory_delta"]]

def main() -> None:
    prices = _read_silver("silver_eia_prices")
    supply = _read_silver("silver_eia_supply")
    storage = _read_silver("silver_eia_storage")

    gold_prices = _price_features(prices) # note: is not gold commodity but refers to the gold data tier
    gold_supply = _supply_features(supply)
    gold_storage = _storage_features(storage)

    # output gold feature tables
    out_prices = _write_gold(gold_prices, "gold_eia_prices")
    out_supply = _write_gold(gold_supply, "gold_eia_supply")
    out_storage = _write_gold(gold_storage, "gold_eia_storage")

    print(f"Logged gold prices to {out_prices}")
    print(f"Logged gold supply to {out_supply}")
    print(f"Logged gold storage to {out_storage}")

if __name__ == "__main__":
    main()