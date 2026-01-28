from __future__ import annotations

from pathlib import Path # path object for platform-safe paths
from typing import Tuple # Tuple type hint

import pandas as pd

# The purpose here is to have a shared writer for all bronze tables used by pipelines/bronze/*.py.
# Centralizing write logic keeps ingestion scripts minimal and consistent.

# The function write_bronze() takes a df and a table name to ingest and store data as a type parquet or csv.
# We include an 'ingested_at' timestamp column for auditability.
# We create two paths: parquet and csv, one as a preferred format and the other as a fallback.

# Walkthrough Step 2:
# Create a single writer utility so all ingestion jobs store data with the same 
# file naming, schema, and audit fields.

# This part (bronze_writer.py) is imported and used by pipelines/bronze/eia_prices.py, eia_storage.py, eia_supply.py, etc.

def write_bronze(df: pd.DataFrame, table_name: str, output_root: str = "data/bronze") -> Tuple[str, str]:
    # Timestamp used to version the output file and keep writes append-only. 
    # pd.Timestamp.utcnow() returns a timezone-neutral UTC timestamp
    timestamp = pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%SZ") # .strftime formats the timestamp to a string like "20240105T153045Z"
    df = df.copy() # avoid mutating the input df passed by callers
    df['ingested_at'] = pd.Timestamp.utcnow() # add load time for auditability
    
    # Recall, Path(...) builds a platform-safe path object; "/" joins subpaths
    # That is, Path(...) builds a path object; "/" joins subpaths across platforms
    output_dir = Path(output_root) / table_name # this has type Path which is like a string but safer
    output_dir.mkdir(parents=True, exist_ok= True) # create folders if missing, parents=True makes parent dirs as needed, and exist_ok=True avoids error if dir already exists

    # making parquet and csv paths
    parquet_path = output_dir/f"{timestamp}.parquet"
    csv_path = output_dir/f"{timestamp}.csv"

    try:

        # parquet is preferred over csv for size/speed, but requires optional dependencies
        # df.to_parquet writes columnar storage; index=False omits row index
        df.to_parquet(parquet_path, index = False)

        return str(parquet_path), "parquet" # return the path as a string and the format
        # ie: we converted df to parquet then returned the path for where we wrote it

    except Exception: # the fallback
        # fallback to csv keeps pipeline runnable in minimal environments
        # df.to_csv writes plain text csv for environments lacking parquet deps
        df_to_csv(csv_path, index = False)
        return str(csv_path), "csv" # return the path as a string and the format
        # ie: we converted df to csv then returned the path for where we wrote it
    

