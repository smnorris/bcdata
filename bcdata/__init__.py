from .bcdc import get_table_name
from .bcdc import get_table_definition
from .wfs import get_data
from .wfs import get_features
from .wfs import get_count
from .wfs import list_tables
from .wfs import validate_name
from .wfs import define_request
from .wfs import get_type
from .wfs import bc2pg
from .wcs import get_dem

from sqlalchemy.dialects.postgresql import DATE, NUMERIC, VARCHAR

__version__ = "0.6.4dev0"


BCDC_API_URL = "https://catalogue.data.gov.bc.ca/api/3/action/"
WFS_URL = "https://openmaps.gov.bc.ca/geo/pub/wfs"
OWS_URL = "http://openmaps.gov.bc.ca/geo/ows"
WCS_URL = "https://openmaps.gov.bc.ca/om/wcs"

# supported oracle/wfs to postgres types
DATABASE_TYPES = {
    "NUMBER": NUMERIC,
    "VARCHAR2": VARCHAR,
    "DATE": DATE
}