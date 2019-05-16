from .wfs import bcdc_package_show
from .wfs import get_data
from .wfs import get_features
from .wfs import get_count
from .wfs import list_tables
from .wfs import validate_name
from .wfs import define_request
from .wcs import get_dem

import logging

__version__ = "0.3.3"


BCDC_API_URL = "https://catalogue.data.gov.bc.ca/api/3/action/"
WFS_URL = "https://openmaps.gov.bc.ca/geo/pub/wfs"
OWS_URL = "http://openmaps.gov.bc.ca/geo/ows"
WCS_URL = "http://delivery.openmaps.gov.bc.ca/om/wcs"


def configure_logging():
    logger = logging.getLogger()
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(levelname)-8s %(message)s")
    logger.setLevel(logging.INFO)

    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(formatter)
    streamhandler.setLevel(logging.INFO)
    logger.addHandler(streamhandler)
