import requests

from .bc2pg import bc2pg
from .bcdc import get_table_definition, get_table_name
from .wcs import get_dem
from .wfs import (
    define_requests,
    get_count,
    get_data,
    get_features,
    get_sortkey,
    list_tables,
    validate_name,
)

PRIMARY_KEY_DB_URL = (
    "https://raw.githubusercontent.com/smnorris/bcdata/main/data/primary_keys.json"
)

# BCDC does not indicate which column in the schema is the primary key.
# In this absence, bcdata maintains its own dictionary of {table: primary_key},
# served via github. Retrieve the dict with this function"""
response = requests.get(PRIMARY_KEY_DB_URL)
if response.status_code == 200:
    primary_keys = response.json()
else:
    raise Exception(f"Failed to download primary key database at {PRIMARY_KEY_DB_URL}")
    primary_keys = {}

__version__ = "0.12.3"
