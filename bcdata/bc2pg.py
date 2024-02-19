import logging
import os

import geopandas as gpd
import numpy
import stamina
from geoalchemy2 import Geometry
from shapely.geometry.linestring import LineString
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon

import bcdata
from bcdata.database import Database
from bcdata.wfs import BCWFS

log = logging.getLogger(__name__)

SUPPORTED_TYPES = [
    "POINT",
    "POINTZ",
    "MULTIPOINT",
    "MULTIPOINTZ",
    "LINESTRING",
    "LINESTRINGZ",
    "MULTILINESTRING",
    "MULTILINESTRINGZ",
    "POLYGON",
    "MULTIPOLYGON",
]

PRIMARY_KEYS = {
    "whse_admin_boundaries.clab_indian_reserves": "clab_id",
    "whse_admin_boundaries.clab_national_parks": "national_park_id",
    "whse_admin_boundaries.fadm_designated_areas": "feature_id",
    "whse_admin_boundaries.fadm_tfl_all_sp": "tfl_all_sysid",
    "whse_basemapping.bcgs_20k_grid": "map_tile",
    "whse_basemapping.dbm_mof_50k_grid": "map_tile",
    "whse_basemapping.gba_local_reg_greenspaces_sp": "local_reg_greenspace_id",
    "whse_basemapping.gba_railway_structure_lines_sp": "railway_structure_line_id",
    "whse_basemapping.gba_railway_tracks_sp": "railway_track_id",
    "whse_basemapping.gba_transmission_lines_sp": "transmission_line_id",
    "whse_basemapping.gns_geographical_names_sp": "geographical_names_id",
    "whse_basemapping.nts_250k_grid": "map_tile",
    "whse_basemapping.trim_cultural_lines": "objectid",
    "whse_basemapping.trim_cultural_points": "objectid",
    "whse_basemapping.trim_ebm_airfields": "objectid",
    "whse_basemapping.trim_ebm_ocean": "objectid",
    "whse_basemapping.utmg_utm_zones_sp": "utm_zone",
    "whse_environmental_monitoring.envcan_hydrometric_stn_sp": "hydrometric_station_id",
    "whse_fish.fiss_stream_sample_sites_sp": "stream_sample_site_id ",
    "whse_fish.pscis_assessment_svw": "stream_crossing_id",
    "whse_forest_tenure.ften_managed_licence_poly_svw": "objectid",
    "whse_forest_tenure.ften_range_poly_svw": "objectid",
    "whse_forest_tenure.ften_range_poly_svw": "objectid",
    "whse_forest_vegetation.ogsr_priority_def_area_cur_sp": "ogsr_pdac_sysid",
    "whse_imagery_and_base_maps.mot_road_structure_sp": "hwy_structure_class_id",
    "whse_legal_admin_boundaries.abms_municipalities_sp": "lgl_admin_area_id",
    "whse_mineral_tenure.mta_acquired_tenure_svw": "tenure_number_id",
    "whse_mineral_tenure.og_petrlm_dev_rds_pre06_pub_sp": "og_petrlm_dev_rd_pre06_pub_id",
    "whse_mineral_tenure.og_road_segment_permit_sp": "og_road_segment_permit_id",
    "whse_tantalis.ta_conservancy_areas_svw": "admin_area_sid",
    "whse_tantalis.ta_crown_tenures_svw": "objectid",
    "whse_tantalis.ta_park_ecores_pa_svw": "admin_area_sid",
    "whse_wildlife_management.wcp_ungulate_winter_range_sp": "ungulate_winter_range_id",
}


def bc2pg(  # noqa: C901
    dataset,
    db_url,
    table=None,
    schema=None,
    geometry_type=None,
    query=None,
    count=None,
    sortby=None,
    primary_key=None,
    timestamp=True,
    schema_only=False,
    append=False,
    refresh=False,
):
    """Request table definition from bcdc and replicate in postgres"""
    if append and refresh:
        raise ValueError("Options append and refresh are not compatible")

    dataset = bcdata.validate_name(dataset)
    schema_name, table_name = dataset.lower().split(".")
    if schema:
        schema_name = schema.lower()
    if table:
        table_name = table.lower()

    # connect to target db
    db = Database(db_url)

    # create wfs service interface instance
    WFS = BCWFS()

    # define requests
    urls = bcdata.define_requests(
        dataset,
        query=query,
        count=count,
        sortby=sortby,
        crs="epsg:3005",
    )

    df = None  # just for tracking if first download is done by geometry type check

    # if appending or refreshing, get column names from db, make sure table exists
    if append or refresh:
        if schema_name + "." + table_name not in db.tables:
            raise ValueError(f"{schema_name}.{table_name} does not exist")
        column_names = db.get_columns(schema_name, table_name)

    # clear existing data if directed by refresh option
    if refresh:
        db.truncate(schema_name, table_name)

    # if not appending/refreshing, define and create table
    if not append or refresh:
        # get info about the table from catalogue
        table_definition = bcdata.get_table_definition(dataset)

        if not table_definition["schema"]:
            raise ValueError(
                "Cannot create table, schema details not found via bcdc api"
            )

        # if geometry type is not provided, determine type by making the first request
        if not geometry_type:
            df = WFS.make_requests(
                [urls[0]], as_gdf=True, crs="epsg:3005", lowercase=True
            )
            geometry_type = df.geom_type.unique()[0]  # keep only the first type
            if numpy.any(
                df.has_z.unique()[0]
            ):  # geopandas does not include Z in geom_type string
                geometry_type = geometry_type + "Z"

        # if geometry type is still not populated try the last request
        # (in case all entrys with geom are near the bottom)
        if not geometry_type:
            if not urls[-1] == urls[0]:
                df_temp = WFS.make_requests(
                    [urls[-1]],
                    as_gdf=True,
                    crs="epsg:3005",
                    lowercase=True,
                    silent=True,
                )
                geometry_type = df_temp.geom_type.unique()[
                    0
                ]  # keep only the first type
                if numpy.any(
                    df_temp.has_z.unique()[0]
                ):  # geopandas does not include Z in geom_type string
                    geometry_type = geometry_type + "Z"
                # drop the last request dataframe to free up memory
                del df_temp

        # ensure geom type is valid
        geometry_type = geometry_type.upper()
        if geometry_type not in SUPPORTED_TYPES:
            raise ValueError("Geometry type {geometry_type} is not supported")

        # if primary key is not supplied, use default (if present in list)
        if not primary_key and dataset.lower() in PRIMARY_KEYS:
            primary_key = PRIMARY_KEYS[dataset.lower()]

        if primary_key and primary_key.upper() not in [
            c["column_name"].upper() for c in table_definition["schema"]
        ]:
            raise ValueError(
                "Column {primary_key} specified as primary_key does not exist in source"
            )

        # build the table definition and create table
        table = db.define_table(
            schema_name,
            table_name,
            table_definition["schema"],
            geometry_type,
            table_definition["comments"],
            primary_key,
        )
        column_names = [c.name for c in table.columns]

    # check if column provided in sortby option is present in dataset
    if sortby and sortby.lower() not in column_names:
        raise ValueError(
            f"Specified sortby column {sortby} is not present in {dataset}"
        )

    # load the data
    if not schema_only:
        # loop through the requests
        for n, url in enumerate(urls):
            # if first url not downloaded above when checking geom type, do now
            if df is None:
                df = WFS.make_requests(
                    [url], as_gdf=True, crs="epsg:3005", lowercase=True
                )
            # tidy the resulting dataframe
            df = df.rename_geometry("geom")
            # lowercasify
            df.columns = df.columns.str.lower()
            # retain only columns matched in table definition
            df = df[column_names]
            # extract features with no geometry
            df_nulls = df[df["geom"].isna()]
            # keep this df for loading with pandas
            df_nulls = df_nulls.drop(columns=["geom"])
            # remove rows with null geometry from geodataframe
            df = df[df["geom"].notna()]
            # cast to everything multipart because responses can have mixed types
            # geopandas does not have a built in function:
            # https://gis.stackexchange.com/questions/311320/casting-geometry-to-multi-using-geopandas
            df["geom"] = [
                MultiPoint([feature]) if isinstance(feature, Point) else feature
                for feature in df["geom"]
            ]
            df["geom"] = [
                MultiLineString([feature])
                if isinstance(feature, LineString)
                else feature
                for feature in df["geom"]
            ]
            df["geom"] = [
                MultiPolygon([feature]) if isinstance(feature, Polygon) else feature
                for feature in df["geom"]
            ]

            # run the load in two parts, one with geoms, one with no geoms
            log.info(f"Writing {dataset} to database as {schema_name}.{table_name}")
            df.to_postgis(table_name, db.engine, if_exists="append", schema=schema_name)
            df_nulls.to_sql(
                table_name,
                db.engine,
                if_exists="append",
                schema=schema_name,
                index=False,
            )
            df = None

        # once load complete, note date/time of load completion in bcdata.log
        if timestamp:
            log.info("Logging download date to bcdata.log")
            db.execute(
                """CREATE SCHEMA IF NOT EXISTS bcdata;
                   CREATE TABLE IF NOT EXISTS bcdata.log (
                     table_name text PRIMARY KEY,
                     latest_download timestamp WITH TIME ZONE
                   );
                """
            )
            db.execute(
                """INSERT INTO bcdata.log (table_name, latest_download)
                   SELECT %s as table_name, NOW() as latest_download
                   ON CONFLICT (table_name) DO UPDATE SET latest_download = NOW();
                """,
                (schema_name + "." + table_name,),
            )

    return schema_name + "." + table_name
