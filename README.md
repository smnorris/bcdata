# bcdata

Python and command line tools for quick access to DataBC geo-data available via WFS.

[![Build Status](https://travis-ci.org/smnorris/bcdata.svg?branch=master)](https://travis-ci.org/smnorris/bcdata)
[![Coverage Status](https://coveralls.io/repos/github/smnorris/bcdata/badge.svg?branch=master)](https://coveralls.io/github/smnorris/bcdata?branch=master)

There is a [wealth of British Columbia geographic information available as open
data](https://catalogue.data.gov.bc.ca/dataset?download_audience=Public),
but direct file download urls are not available and the syntax to accesss WFS via `ogr2ogr` and/or `curl` can be awkward.

This Python module and CLI attempts to make downloads of BC geographic data quick and easy.


**Notes**

- this tool is for my convenience, it is in no way endorsed by the Province of Britsh Columbia or DataBC
- use with care, please don't overload the service
- data are generally licensed as [OGL-BC](http://www2.gov.bc.ca/gov/content/governments/about-the-bc-government/databc/open-data/open-government-license-bc), but it is up to the user to check the licensing for any data downloaded


## Installation

    $ pip install bcdata

## Usage

Find data of interest manually using the [DataBC Catalogue](https://catalogue.data.gov.bc.ca/dataset?download_audience=Public). Once you have found your data of interest, note either the `id` (the last portion of the url, also known as the package name) or the `Object Name` (Under `Object Description`). For example, for [BC Airports]( https://catalogue.data.gov.bc.ca/dataset/bc-airports), either of these keys will work:

- `bc-airports` (id)
- `WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW` (object name)

**Python module**

    >>> import bcdata
    >>> geojson = bcdata.get_data('bc-airports', query="AIRPORT_NAME='Terrace (Northwest Regional) Airport'")
    >>> geojson
    {'type': 'FeatureCollection', 'features': [{'type': 'Feature', 'id': 'WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW.fid-f0cdbe4_16811fe142b_-6f34', 'geometry': {'type': 'Point', ...

**CLI**

    $ bcdata --help
    Usage: bcdata [OPTIONS] COMMAND [ARGS]...

    Options:
      --help  Show this message and exit.

    Commands:
      dump  Dump a data layer from DataBC WFS
      info  Print basic info about a DataBC WFS layer
      list  List DataBC layers available to dump

Common uses might look something like this:

    $ bcdata info bc-airports
    {
      "name": "WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW",
      "count": 455,
      "schema": {
        "properties": {
          "CUSTODIAN_ORG_DESCRIPTION": "string",
          "BUSINESS_CATEGORY_CLASS": "string",
          "BUSINESS_CATEGORY_DESCRIPTION": "string",
          "OCCUPANT_TYPE_DESCRIPTION": "string",
          ...etc...
        },
        "geometry": "GeometryCollection",
        "geometry_column": "SHAPE"
      }
    }
    $ bcdata dump bc-airports -o bc-airports.geojson
    $ bcdata dump \
      WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW \
      -o terrace_airport.geojson \
      --query "AIRPORT_NAME='Terrace (Northwest Regional) Airport'"


## Projections / CRS

Data are downloaded as either BC Albers (`EPSG:3005`) (default) or WGS84 (`EPSG:4326`).


## Development and testing

    $ mkdir bcdata_env
    $ virtualenv bcdata_env
    $ source bcdata_env/bin/activate
    (bcdata_env)$ git clone git@github.com:smnorris/bcdata.git
    (bcdata_env)$ cd bcdata
    (bcdata_env)$ pip install -e .[test]
    (bcdata_env)$ py.test


## Other implementations
- [bdata R package](https://github.com/bcgov/bcdata)
- [OWSLib](https://github.com/geopython/OWSLib) has basic WFS capabilities
- GDAL

        # list all layers
        # querying the endpoint this way doesn't seem to work with `VERSION=2.0.0`
        ogrinfo WFS:http://openmaps.gov.bc.ca/geo/ows?VERSION=1.1.0

        # describe airports
        ogrinfo "https://openmaps.gov.bc.ca/geo/pub/WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW&outputFormat=json&SRSNAME=epsg%3A3005" OGRGeoJSON -so

        # airports to geojson
        ogr2ogr \
          -f GeoJSON \
          airports.geojson \
          "https://openmaps.gov.bc.ca/geo/pub/WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW&outputFormat=json&SRSNAME=epsg%3A3005"

        # airports to postgres
        ogr2ogr \
          -f PostgreSQL \
          PG:"host=localhost user=postgres dbname=postgis password=postgres" \
          -lco SCHEMA=whse_imagery_and_base_maps \
          -lco GEOMETRY_NAME=geom \
          -nln gsr_airports_svw \
          "https://openmaps.gov.bc.ca/geo/pub/WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW&outputFormat=json&SRSNAME=epsg%3A3005"

        # ungulate winter range to shape
        # this only grabs the first 10,000 records
        ogr2ogr \
          uwr.shp \
          -dsco OGR_WFS_PAGING_ALLOWED=ON \
          "https://openmaps.gov.bc.ca/geo/pub/WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP&outputFormat=json&SRSNAME=epsg%3A3005"

        # get the data via wget, still 10k limit
        wget -O t.gml "https://openmaps.gov.bc.ca/geo/pub/WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP&SRSNAME=epsg%3A3005"

