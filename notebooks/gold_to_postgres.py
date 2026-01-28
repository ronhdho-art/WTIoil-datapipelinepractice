# Ok, now we have bronze tables ingested from raw data, and silver tables cleaned and aligned, 
# a gold step that had emitted purpose-built features for modeling. 
# Finally, we can build gold_to_postgres.py to load gold features into Postgres for serving.

# This is a databricks notebook that loads gold features into Postgres tables.
# After gold features exist, load them into Postgres so the API can query them.
# That is, read gold tables (data/gold/*) and insert rows into src/db/models.py tables.

# Walkthrough (optional) Step 9:
# We read gold tables, reshape them into long format, and insert into Postgres.

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.db.models import Base
from src.db.session import engine

gold_root = Path("data/gold")

def _latest_file(table_dir: Path) -> Path:
    files = sorted(table_dir.glob("*.parquet")) or sorted(table_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No gold files found in {table_dir}")
    return files[-1]

def _read_gold(table: str) -> pd.DataFrame:
    # get table_dir by appending desired table to gold_root directory
    # then retrieve latest file
    # if latest file is a parquet format, then we read it with pandas' .read_parquet to parse file
    # otherwise, fallback to reading with csv via .read_csv
    table_dir = gold_root/table
    latest = _latest_file(table_dir)
    if latest.suffix == '*.parquet': 
        return pd.read_parquet(latest)
    return pd.read_csv(latest)

def _to_long_features(df: pd.DataFrame, feature_cols: list[str]) -> pd.DataFrame:
    # Convert wide feature columns into long rows for gold_features table
    # recall, df.melt is a pandas function that reshapes dataframes from wide to long format
    long_df = df.melt(
        id_vars = ["date"],
        value_vars = feature_cols,
        var_name = "feature_name",
        value_name = "feature_value",
    ) 
    long_df = long_df.rename(columns={"date": "week"}) # rename "date" column to "week" for consistency with database schema
    long_df["commodity"] = "wti" # add a new column "commodity" with constant value "wti"
    return long_df[["commodity", "week", "feature_name", "feature_value"]]

# Orchestrater function to read gold tables, transform, and load into Postgres
def main() -> None:
    # ensure tables exist before loading (since we're using ORM metadata)
    Base.metadata.createall(bind=engine)

    gold_prices = _read_gold("gold_prices") # note that gold_prices is a dataframe from gold_features.py
    gold_supply = _read_gold("gold_supply")
    gold_storage = _read_gold("gold_storage")

    price_features = _to_long_features(gold_prices, ["value", "return_1w", "vol_4w"])
    supply_features = _to_long_features(gold_supply, ["value", "supply_delta"])
    storage_features = _to_long_features(gold_storage, ["value", "inventory_delta"])

    # we use pd.concat to concatenate multiple dataframes into a single dataframe
    all_features = pd.concat([price_features, supply_features, storage_features], ignore_index=True)


    # now that we've constructed all_features dataframe, we can write it to Postgres table
    all_features.to_sql("gold_features", engine, if_exists="append", index=False)

    print(f"Loaded {len(all_features)} gold feature rows into Postgres.") # generic logging statement

if __name__ == "__main__":
    main()

    