# We've just completed bronze_ingest.py to ingest EIA data into bronze tables.
# Next, we'll build silver_clean.py to standardize and align the data into silver tables.

# This is a databricks notebook that is the silver-layer cleaning logic: we read the latest bronze outputs
# and standardize them for gold features. 

# Walkthrough Step 5:
# We implement silver transforms to normalize units and align timestamps across sources
# so features can be safely joined.
# We effectively write methods like _latest_file, _read_bronze, _align_weekly, _write_silver to process ingested bronze data.
# then we orchestrate these methods in main() to read bronze, clean/align, and write silver tables in one step. 

from __future__ import annotations # for forward compatibility with future Python versions

from pathlib import Path # recall, Path helps with filesystem paths by abstracting OS differences

import pandas as pd

# Mechanics note: Path(...).glob returns a generator of matching paths.
bronze_root = Path("data/bronze") # directory where bronze tables are stored
silver_root = Path("data/silver") # directory where silver tables will be stored

def _latest_file(table_dir: Path) -> Path:
    # Pick the most recent file by name (timestamps are in the filename)
    files = sorted(table_dir.glob("*.parquet")) or sorted(table_dir.glob("*.csv")) # recall, glob matches files by pattern: "*.parquet" means all parquet files
    if not files:
        raise FileNotFoundError(f"No bronze files found in {table_dir}")
    return files[-1] # return the last file in sorted order, which is the most recent

def _read_bronze(table: str) -> pd.DataFrame:
    table_dir = bronze_root/table # construct the path to the bronze table directory
    latest = _latest_file(table_dir)
    if latest.suffix == ".parquet":
        return pd.read_parquet(latest) # read parquet file into a df
    return pd.read_csv(latest) # read csv file into a df, as a fallback

def _align_weekly(df: pd.DataFrame) -> pd.DataFrame:
    # Align all sources to weekly frequency (say, Friday) for consistent joins
    df = df.copy() # avoid modifying the original df
    df["date"] = pd.to_datetime(df["date"]) # ensure the date column is in datetime format
    df = df.sort_values("date") # sort by date and returns new df, set to asc by default. sorts by axis=0 (rows) by default
    df = df.set_index("date") # set date column as index
    # resample to weekly frequency and forward-fill short gaps. recall, .resample() changes the frequency of the time series data
    df = df.resample("W-FRI").ffill() # "W-FRI" is weekly frequency on Fridays and ffill is forward fill missing values
    df = df.reset_index() # reset index to turn date index back into a column. This is important for saving the df later
    return df


def _write_silver(df: pd.DataFrame, table: str) -> Path:
    silver_root.mkdir(parents=True, exist_ok=True) # ensure the silver root directory exists
    out_dir = silver_root/table # construct the path to the silver table directory
    out_dir.mkdir(parents=True, exist_ok=True) # ensure the silver table directory exists
    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ") # get current UTC timestamp in a specific format, recall strftime formats the timestamp according to "%Y%m%dT%H%M%SZ" meaning year, month, day, T, hour, minute, second, Z
    path = out_dir/f"{timestamp}.parquet" # construct the output file path with the timestamp
    df.to_parquet(path, index = False) # write the df to a parquet file, setting index to False to avoid writing the index as a column
    return path

# Orchestrate the silver cleaning process
def main() -> None:
    # read latest bronze tables
    prices = _read_bronze("bronze_eia_prices")
    supply = _read_bronze("bronze_eia_supply")
    storage = _read_bronze("bronze_eia_storage")

    # standardize and align units
    prices = _align_weekly(prices)
    supply = _align_weekly(supply)
    storage = _align_weekly(storage)

    # write silver outputs
    out_prices = _write_silver(prices, "silver_eia_prices")
    out_supply = _write_silver(supply, "silver_eia_supply")
    out_storage = _write_silver(storage, "silver_eia_storage")


    print(f"Logged silver prices to {out_prices}")
    print(f"Logged silver supply to {out_supply}")
    print(f"Logged silver storage to {out_storage}")

if __name__ == "__main__":
    main()