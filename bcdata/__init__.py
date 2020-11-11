from .wfs import get_table_name
from .wfs import get_data
from .wfs import get_features
from .wfs import get_count
from .wfs import list_tables
from .wfs import validate_name
from .wfs import define_request
from .wcs import get_dem

__version__ = "0.4.3"


BCDC_API_URL = "https://catalogue.data.gov.bc.ca/api/3/action/"
WFS_URL = "https://openmaps.gov.bc.ca/geo/pub/wfs"
OWS_URL = "http://openmaps.gov.bc.ca/geo/ows"
WCS_URL = "https://openmaps.gov.bc.ca/om/wcs"
