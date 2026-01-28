# Great! Now that we have eia_supply.py, eia_prices.py, eia_storage.py, 
# we can build bronze_ingest.py to orchestrate all three ingestion scripts together.

import os 

from pipelines.bronzer import eia_prices, eia_storage, eia_supply

# This is a databricks notebook that functions as an entrypoint to run all bronze ingestion scripts.
# It wires widget inputs into env vars and delegates to pipelines/bronze/*. 

# Walkthrough Step 4: 
# Create a thin notebook or job entrypoint that orchestrates multiple ingestion
# scripts so they run together in a scheduled pipeline.

# Databricks-friendly: allow widgets to override the API key if desired.
try:
    # dbutils.widgets.text defines a text widget; get(...) retrieves its value.
    # where a text widget is a text input box in databricks UI
    dbutils.widget.text("EIA_API_KEY", "")
    widget_key = dbutils.widgets.get("EIA_API_KEY") # returns "" if empty
except Exception:
    widget_key = "" # local runs won't have dbutils

if widget_key:
    # os.environ reads environment variables; returns None if not set
    # Setting os.environ updates the process environment for downstream calls.
    os.environ["EIA_API_KEY"] = widget_key # set env so pipelines can read it.
    
# Call each ingestion script; each writes a bronze table under data/bronze/.
eia_prices.main()
eia_supply.main()
eia_storage.main()



