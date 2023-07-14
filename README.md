# bcdata

Python and command line tools for quick access to DataBC geo-data available via WFS/WCS.

There is a [wealth of British Columbia geographic information available as open
data](https://catalogue.data.gov.bc.ca/dataset?download_audience=Public),
but direct file download urls are not available and the syntax to accesss WFS via `ogr2ogr` and/or `curl/wget` can be awkward.

This Python module and CLI attempts to simplify downloads of BC geographic data and smoothly integrate with PostGIS and Python GIS tools like `geopandas`, `fiona` and `rasterio`.


**Disclaimer**  
*It is the user's responsibility to check the licensing for any downloads, data are generally licensed as [OGL-BC](http://www2.gov.bc.ca/gov/content/governments/about-the-bc-government/databc/open-data/open-government-license-bc)*

## Installation

`bcdata` has several dependencies that may not easily be installed via `pip` (`gdal`, `fiona`, `geopandas`, `rasterio`)
Installing via `miniconda`, the [conda package manager](https://conda.io/en/latest/miniconda.html) is recommended on Windows or if you do not wish to install these dependencies yourself.  Once `conda` is installed, download the provided `environment.yml` file to your system, open the `Anaconda Prompt` command line from the start menu, navigate to the folder where you saved `environment.yml` and create/actvate the environment.

    conda env create -f environment.yml
    conda activate bcdataenv

Once requirements are installed, install with pip:

    $ pip install bcdata


### Configuration

#### Default PostgreSQL database

The default target database connection (used by `bc2pg`) can be set via the `DATABASE_URL` environment variable (the password parameter should not be required if using a [.pgpass file](https://www.postgresql.org/docs/current/libpq-pgpass.html))

Linux/Mac: `export DATABASE_URL=postgresql://{username}:{password}@{hostname}:{port}/{database}`

Windows:   `SET DATABASE_URL=postgresql://{username}:{password}@{hostname}:{port}/{database}`


#### Layer list / layer schema cache

To reduce the volume of requests, information about data requested is cached locally:
 - the WFS GetCapabilities response xml (listing all datasets available via the service) is cached as `capabilities.xml`
 - schemas of individual layers that have previously been requested are cached with the cache file name matching the object/table name

`capabilities.xml` is automatically refreshed if it is more than a day old. The layer definition files are refreshed if more than 30 days old. These cache files are stored by default in `~/.bcdata`. Modify this location by  setting the the `$BCDATA_CACHE` environment variable:

`export BCDATA_CACHE=/path/to/bcdata_cache`

Force a cache refresh by deleting the files in the cache or the entire cache folder.

## Usage

Typical usage will involve a manual search of the [DataBC Catalogue](https://catalogue.data.gov.bc.ca/dataset?download_audience=Public) to find a layer of interest. Once a dataset of interest is found, note the key with which to retreive it. This can be either the `id`/`package name` (the last portion of the url) or the `Object Name` (Under `Object Description`).

For example, for [BC Airports]( https://catalogue.data.gov.bc.ca/dataset/bc-airports), either of these keys will work:

- id/package name: `bc-airports`
- object name: `WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW`

Note that some packages [may have more than one layer](https://catalogue.data.gov.bc.ca/dataset/forest-development-units) - if you request a package like this, `bcdata` will prompt you with a list of valid object/table names to use instead of the package name.


### Python module

```python
import bcdata

# get a feature as geojson
geojson = bcdata.get_data(
    'bc-airports',
    query="AIRPORT_NAME='Terrace (Northwest Regional) Airport'"
)
geojson
{'type': 'FeatureCollection', 'features': [{'type': 'Feature', 'id': 'WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW.fid-f0cdbe4_16811fe142b_-6f34', 'geometry': {'type': 'Point', ...

# optionally, load data as a geopandas GeoDataFrame
gdf = bcdata.get_data(
    'bc-airports',
    query="AIRPORT_NAME='Terrace (Northwest Regional) Airport'",
    as_gdf=True
)
gdf.head()
AERODROME_STATUS AIRCRAFT_ACCESS_IND                          AIRPORT_NAME                ...                TC_LID_CODE WEBSITE_URL                          geometry
0        Certified                   Y  Terrace (Northwest Regional) Airport                ...                       None        None  POINT (-128.5783333 54.46861111)
```

### CLI
Commands available via the bcdata command line interface are documented with the `--help` option:

```

$ bcdata --help

Usage: bcdata [OPTIONS] COMMAND [ARGS]...

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  bc2pg  Download a DataBC WFS layer to postgres
  cat    Write DataBC features to stdout as GeoJSON feature objects.
  dem    Dump BC DEM to TIFF
  dump   Write DataBC features to stdout as GeoJSON feature collection.
  info   Print basic metadata about a DataBC WFS layer as JSON.
  list   List DataBC layers available via WFS
```

#### bc2pg

```
$ bcdata bc2pg --help

Usage: bcdata bc2pg [OPTIONS] DATASET

  Download a DataBC WFS layer to postgres

   $ bcdata bc2pg bc-airports --db_url postgresql://postgres:postgres@localhost:5432/postgis

Options:
  -db, --db_url TEXT      Target database url, defaults to $DATABASE_URL
                          environment variable if set
  --table TEXT            Destination table name
  --schema TEXT           Destination schema name
  --geometry_type TEXT    Spatial type of geometry column
  --query TEXT            A valid CQL or ECQL query
  -c, --count INTEGER     Total number of features to load
  -p, --pagesize INTEGER  Maximum request size
  -k, --primary_key TEXT  Primary key of dataset
  -s, --sortby TEXT       Name of sort field
  -e, --schema_only       Create empty table from catalogue schema
  -a, --append            Append to existing table
  -t, --no_timestamp      Do not log download to bcdata.log
  -v, --verbose           Increase verbosity.
  -q, --quiet             Decrease verbosity.
  --help                  Show this message and exit.
```

#### cat

```
$ bcdata cat --help

Usage: bcdata cat [OPTIONS] DATASET

  Write DataBC features to stdout as GeoJSON feature objects.

Options:
  --query TEXT                    A valid CQL or ECQL query
  --bounds TEXT                   Bounds: "left bottom right top" or "[left,
                                  bottom, right, top]". Coordinates are BC
                                  Albers (default) or --bounds_crs
  --indent INTEGER                Indentation level for JSON output
  --compact / --not-compact       Use compact separators (',', ':').
  --dst-crs, --dst_crs TEXT       Destination CRS
  -p, --pagesize INTEGER          Maximum request size
  -s, --sortby TEXT               Name of sort field
  --bounds-crs, --bounds_crs TEXT
                                  CRS of provided bounds
  -l, --lowercase                 Write column/properties names as lowercase
  -v, --verbose                   Increase verbosity.
  -q, --quiet                     Decrease verbosity.
  --help                          Show this message and exit.
```

#### dem 

```
$ bcdata dem --help

Usage: bcdata dem [OPTIONS]

  Dump BC DEM to TIFF

Options:
  -o, --out_file TEXT             Output file
  --bounds TEXT                   Bounds: "left bottom right top" or "[left,
                                  bottom, right, top]". Coordinates are BC
                                  Albers (default) or --bounds_crs  [required]
  --dst-crs, --dst_crs TEXT       Destination CRS
  --bounds-crs, --bounds_crs TEXT
                                  CRS of provided bounds
  -r, --resolution INTEGER
  -a, --align                     Align provided bounds to provincial standard
  -i, --interpolation [nearest|bilinear|bicubic]
  -v, --verbose                   Increase verbosity.
  -q, --quiet                     Decrease verbosity.
  --help                          Show this message and exit.
```

#### dump 

```
$ bcdata dump --help

Usage: bcdata dump [OPTIONS] DATASET

  Write DataBC features to stdout as GeoJSON feature collection.

    $ bcdata dump bc-airports
    $ bcdata dump bc-airports --query "AIRPORT_NAME='Victoria Harbour (Shoal Point) Heliport'"
    $ bcdata dump bc-airports --bounds xmin ymin xmax ymax

   It can also be combined to read bounds of a feature dataset using Fiona:
   $ bcdata dump bc-airports --bounds $(fio info aoi.shp --bounds)

Options:
  --query TEXT                    A valid CQL or ECQL query
  -o, --out_file TEXT             Output file
  --bounds TEXT                   Bounds: "left bottom right top" or "[left,
                                  bottom, right, top]". Coordinates are BC
                                  Albers (default) or --bounds_crs
  --bounds-crs, --bounds_crs TEXT
                                  CRS of provided bounds
  -l, --lowercase                 Write column/properties names as lowercase
  -v, --verbose                   Increase verbosity.
  -q, --quiet                     Decrease verbosity.
  --help                          Show this message and exit.
```

#### info

```
$ bcdata info --help

Usage: bcdata info [OPTIONS] DATASET

  Print basic metadata about a DataBC WFS layer as JSON.

  Optionally print a single metadata item as a string.

Options:
  --indent INTEGER  Indentation level for JSON output
  --count           Print the count of features.
  --name            Print the table name of the dateset.
  -v, --verbose     Increase verbosity.
  -q, --quiet       Decrease verbosity.
  --help            Show this message and exit.
```

#### list

```
$ bcdata list --help

Usage: bcdata list [OPTIONS]

  List DataBC layers available via WFS

Options:
  -r, --refresh  Refresh the cached list
  --help         Show this message and exit.
```

#### CLI notes

Note that `bc2pg` creates `bcdata.log` and logs the most recent download date for each table downloaded.
Disable with the switch `--no_timestamp` if you do not wish to create this table.

Example of a record in `bcdata.log`:

```
mydb=# select * from bcdata.log;
                 table_name                  |        date_downloaded
---------------------------------------------+-------------------------------
 whse_imagery_and_base_maps.gsr_airports_svw | 2021-02-17 11:50:34.044481-08
```


#### CLI examples

Search the data listing for airports:

      $ bcdata list | grep AIRPORTS
      WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW

Describe a dataset. Note that if we know the id of a dataset, we can use that rather than the object name:

    $ bcdata info bc-airports --indent 2
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

The JSON output can be manipulated with [jq](https://stedolan.github.io/jq/). For example, to show only the fields available in the dataset:

    $ bcdata info bc-airports | jq '.schema.properties'
    {
      "CUSTODIAN_ORG_DESCRIPTION": "string",
      "BUSINESS_CATEGORY_CLASS": "string",
      "BUSINESS_CATEGORY_DESCRIPTION": "string",
      "OCCUPANT_TYPE_DESCRIPTION": "string",
      etc...
    }

Dump data to geojson ([`EPSG:4326` only](https://tools.ietf.org/html/rfc7946#section-4)):

    $ bcdata dump bc-airports > bc-airports.geojson

Get a single feature and send it to geojsonio (requires [geojson-cli](https://github.com/mapbox/geojsonio-cli)).  Note the double quotes  required around a CQL FILTER provided to the `--query` option.

    $ bcdata dump \
      WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW \
      --query "AIRPORT_NAME='Terrace (Northwest Regional) Airport'" \
       | geojsonio

Save a layer to a geopackage in BC Albers:

    $ bcdata cat bc-airports --dst-crs EPSG:3005 \
      | fio collect \
      | fio load -f GPKG --dst-crs EPSG:3005 airports.gpkg

Note that this will not work if the source data has mixed geometry types.

Load data to postgres and run a spatial query:

    $ bcdata bc2pg bc-airports \
        --db_url postgresql://postgres:postgres@localhost:5432/postgis

    $ bcdata bc2pg WHSE_LEGAL_ADMIN_BOUNDARIES.ABMS_MUNICIPALITIES_SP \
        --db_url postgresql://postgres:postgres@localhost:5432/postgis

    $ psql -c \
      "SELECT airport_name
       FROM whse_imagery_and_base_maps.gsr_airports_svw a
       INNER JOIN whse_legal_admin_boundaries.abms_municipalities_sp m
       ON ST_Intersects(a.geom, m.geom)
       WHERE admin_area_name LIKE '%Victoria%'"
                               airport_name
    ------------------------------------------------------------------
     Victoria Harbour (Camel Point) Heliport
     Victoria Inner Harbour Airport (Victoria Harbour Water Airport)
     Victoria Harbour (Shoal Point) Heliport
    (3 rows)

## Projections / CRS

**CLI**

`bcdata dump` returns GeoJSON in WGS84 (`EPSG:4326`).

`bcdata cat` provides the `--dst-crs` option, use any CRS the WFS server supports.

`bcdata bc2pg` loads data to PostgreSQL in BC Albers (`EPSG:3005`).


**Python module**

`bcdata.get_data()` defaults to `EPSG:4236` but any CRS can be specified (that the server will accept).


## Development and testing

`bc2pg` tests require database `postgresql://postgres@localhost:5432/test_bcdata` exists and has PostGIS installed:

    psql -c "create database test_bcdata"
    psql test_bcdata -c "create extension postgis"

Create virtualenv and install `bcdata` in development mode:

    $ mkdir bcdata_env
    $ virtualenv bcdata_env
    $ source bcdata_env/bin/activate
    (bcdata_env)$ git clone git@github.com:smnorris/bcdata.git
    (bcdata_env)$ cd bcdata
    (bcdata_env)$ pip install -e .[test]
    (bcdata_env)$ py.test


## Other implementations
- [bcdata R package](https://github.com/bcgov/bcdata)
- [OWSLib](https://github.com/geopython/OWSLib) has basic WFS capabilities
- [pgsql-ogr-fdw](https://github.com/pramsey/pgsql-ogr-fdw) - read WFS data with postgres foreign data wrapper
- GDAL / curl / wget:

        # list all layers
        # querying the endpoint this way doesn't seem to work with `VERSION=2.0.0`
        ogrinfo WFS:http://openmaps.gov.bc.ca/geo/ows?VERSION=1.1.0

        # define a request url for airports
        airports_url="https://openmaps.gov.bc.ca/geo/pub/WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW&outputFormat=json&SRSNAME=epsg%3A3005"

        # describe airports
        ogrinfo -so $airports_url OGRGeoJSON

        # dump airports to geojson
        ogr2ogr \
          -f GeoJSON \
          airports.geojson \
          $airports_url

        # load airports to postgres
        ogr2ogr \
          -f PostgreSQL \
          PG:"host=localhost user=postgres dbname=postgis password=postgres" \
          -lco SCHEMA=whse_imagery_and_base_maps \
          -lco GEOMETRY_NAME=geom \
          -nln gsr_airports_svw \
          $airports_url

        # Try requesting a larger dataset - ungulate winter range
        uwr_url="https://openmaps.gov.bc.ca/geo/pub/WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP&outputFormat=json&SRSNAME=epsg%3A3005"

        # The request only returns the first 10,000 records
        ogr2ogr \
          uwr.shp \
          -dsco OGR_WFS_PAGING_ALLOWED=ON \
          $uwr_url

        # wget works too, but still only 10k records
        wget -O uwr.geojson $uwr_url
