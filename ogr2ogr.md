# WFS via ogr2ogr/curl

Making WFS requests and dumping to file/postgres is fairly straightforward with existing command line tools:

- list all layers:

        $ ogrinfo WFS:http://openmaps.gov.bc.ca/geo/ows

- define a data request url:

        $ airports_url="https://openmaps.gov.bc.ca/geo/pub/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW&outputFormat=json&SRSNAME=epsg%3A3005"

- describe the data:
  
        $ ogrinfo -so $airports_url OGRGeoJSON

- making the request and dumping to file can be done different ways:
  
        $ ogr2ogr \
          -f GeoJSON \
          airports.geojson \
          $airports_url

        $ curl $airports_url > airports.geojson

- request a bigger dataset - gdal docs say [paging is automatically supported for WFS 2.0.0](https://gdal.org/drivers/vector/wfs.html#paging-with-wfs-2-0):

        $ uwr="https://openmaps.gov.bc.ca/geo/pub/ows?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_WILDLIFE_MANAGEMENT.WCP_UNGULATE_WINTER_RANGE_SP&outputFormat=json&SRSNAME=epsg%3A3005"
        $ curl $uwr > uwr.geojson
        $ ogrinfo uwr.geojson uwr -so

        INFO: Open of `uwr.geojson'
              using driver `GeoJSON' successful.

        Layer name: uwr
        Geometry: Unknown (any)
        Feature Count: 10000

    only 10k features... a client needs to request the total number of features and define the per-page requests

- load to postgres:
    
        $ ogr2ogr \
          -f PostgreSQL \
          PG:"host=localhost user=postgres dbname=postgis password=postgres" \
          -lco SCHEMA=whse_imagery_and_base_maps \
          -lco GEOMETRY_NAME=geom \
          -nln gsr_airports_svw \
          $airports_url


`bcdata` smooths the above processes by:

- providing a CLI for basic/typical request options
- generating paged requests
- handling typical minor issues (eg mixed geometry types)
- creating postgres tables that mirror the table schema as defined in BCGW (rather than using ogr default types)