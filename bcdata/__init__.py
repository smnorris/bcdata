from .bc2pg import bc2pg
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

__version__ = "0.10.0dev0"
