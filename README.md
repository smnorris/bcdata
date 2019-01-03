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

Find data of interest using the [DataBC Catalogue](https://catalogue.data.gov.bc.ca/dataset?download_audience=Public). When you have found your data of interest, note either the last portion of the url or the `Object Name`. For example, for [BC Airports]( https://catalogue.data.gov.bc.ca/dataset/bc-airports), either of these keys will work:

- `bc-airports`
- `WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW`

**Python module**

    >>> import bcdata
    >>> geojson = bcdata.get_data('bc-airports', query="AIRPORT_NAME='Terrace (Northwest Regional) Airport'")
    >>> geojson
    {'type': 'FeatureCollection', 'features': [{'type': 'Feature', 'id': 'WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW.fid-f0cdbe4_16811fe142b_-6f34', 'geometry': {'type': 'Point', ...

Download times will vary based mainly on the size of your requested data.

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
    {"name": "WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW", "count": 455}
    $ bcdata dump bc-airports -o bc-airports.geojson
    $ bcdata dump \
      WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW \
      -o terrace_airport.geojson \
      --query "AIRPORT_NAME='Terrace (Northwest Regional) Airport'"

## Projections / CRS

While the DataBC WFS supports multiple projections, this tool does not. All data is downloaded as `EPSG:3005` (BC Albers).


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
- GDAL (doesn't seem to work with `VERSION=2.0.0`)

        # list all layers
        ogrinfo WFS:http://openmaps.gov.bc.ca/geo/ows?VERSION=1.1.0

        # download airports to geojson
        ogr2ogr \
          -f GeoJSON \
          airports.geojson \
          WFS:http://openmaps.gov.bc.ca/geo/ows?VERSION=1.1.0 \
          pub:WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW