from .bc2pg import bc2pg, get_primary_keys
from .bcdc import get_table_definition, get_table_name
from .wcs import get_dem
from .wfs import (
    define_requests,
    get_count,
    get_data,
    get_features,
    list_tables,
    validate_name,
)

PRIMARY_KEY_DB_URL = (
    "https://raw.githubusercontent.com/smnorris/bcdata/main/data/primary_keys.json"
)

__version__ = "0.10.0dev0"
