import sys
import json
import logging
import os
import re
import subprocess
from urllib.parse import urlencode
from functools import partial
from multiprocessing.dummy import Pool
from subprocess import call

import click
from cligj import indent_opt
from cligj import compact_opt
from cligj import verbose_opt, quiet_opt

from owslib.wfs import WebFeatureService
from psycopg2 import sql

from bcdata.database import Database

import bcdata


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level)


# bounds handling direct from rasterio
# https://github.com/mapbox/rasterio/blob/master/rasterio/rio/options.py
# https://github.com/mapbox/rasterio/blob/master/rasterio/rio/clip.py


def from_like_context(ctx, param, value):
    """Return the value for an option from the context if the option
    or `--all` is given, else return None."""
    if ctx.obj and ctx.obj.get("like") and (value == "like" or ctx.obj.get("all_like")):
        return ctx.obj["like"][param.name]
    else:
        return None


def bounds_handler(ctx, param, value):
    """Handle different forms of bounds."""
    retval = from_like_context(ctx, param, value)
    if retval is None and value is not None:
        try:
            value = value.strip(", []")
            retval = tuple(float(x) for x in re.split(r"[,\s]+", value))
            assert len(retval) == 4
            return retval
        except Exception:
            raise click.BadParameter(
                "{0!r} is not a valid bounding box representation".format(value)
            )
    else:  # pragma: no cover
        return retval


bounds_opt = click.option(
    "--bounds",
    default=None,
    callback=bounds_handler,
    help='Bounds: "left bottom right top" or "[left, bottom, right, top]". Coordinates are BC Albers (default) or --bounds_crs',
)

bounds_opt_dem = click.option(
    "--bounds",
    required=True,
    default=None,
    callback=bounds_handler,
    help='Bounds: "left bottom right top" or "[left, bottom, right, top]". Coordinates are BC Albers (default) or --bounds_crs',
)

dst_crs_opt = click.option("--dst-crs", "--dst_crs", help="Destination CRS.")


@click.group()
@click.version_option(version=bcdata.__version__, message="%(version)s")
def cli():
    pass


@cli.command()
@click.option("--refresh", "-r", is_flag=True, help="Refresh the cached list")
def list(refresh):
    """List DataBC layers available via WFS"""
    # This works too, but is much slower:
    # ogrinfo WFS:http://openmaps.gov.bc.ca/geo/ows?VERSION=1.1.0
    for table in bcdata.list_tables(refresh):
        click.echo(table)


@cli.command()
@click.argument("dataset", type=click.STRING)
@indent_opt
# Options to pick out a single metadata item and print it as
# a string.
@click.option(
    "--count", "meta_member", flag_value="count", help="Print the count of features."
)
@click.option(
    "--name", "meta_member", flag_value="name", help="Print the datasource's name."
)
@verbose_opt
@quiet_opt
def info(dataset, indent, meta_member, verbose, quiet):
    """Print basic metadata about a DataBC WFS layer as JSON.

    Optionally print a single metadata item as a string.
    """
    verbosity = verbose - quiet
    configure_logging(verbosity)
    table = bcdata.validate_name(dataset)
    wfs = WebFeatureService(url=bcdata.OWS_URL, version="2.0.0")
    info = {}
    info["name"] = table
    info["count"] = bcdata.get_count(table)
    info["schema"] = wfs.get_schema("pub:" + table)
    if meta_member:
        click.echo(info[meta_member])
    else:
        click.echo(json.dumps(info, indent=indent))


@cli.command()
@click.option("--out_file", "-o", help="Output file", default="dem25.tif")
@bounds_opt_dem
@dst_crs_opt
@click.option(
    "--bounds-crs", "--bounds_crs", help="CRS of provided bounds", default="EPSG:3005"
)
@click.option("--resolution", "-r", type=int, default=25)
@click.option(
    "--align", "-a", is_flag=True, help="Align provided bounds to provincial standard"
)
@click.option(
    "--interpolation",
    "-i",
    type=click.Choice(["nearest", "bilinear", "bicubic"], case_sensitive=False),
)
@verbose_opt
@quiet_opt
def dem(
    bounds,
    bounds_crs,
    align,
    dst_crs,
    out_file,
    resolution,
    interpolation,
    verbose,
    quiet,
):
    """Dump BC DEM to TIFF"""
    verbosity = verbose - quiet
    configure_logging(verbosity)
    if not dst_crs:
        dst_crs = "EPSG:3005"
    bcdata.get_dem(
        bounds,
        out_file=out_file,
        align=align,
        src_crs=bounds_crs,
        dst_crs=dst_crs,
        resolution=resolution,
        interpolation=interpolation,
    )


@cli.command()
@click.argument("dataset", type=click.STRING)
@click.option(
    "--query",
    help="A valid CQL or ECQL query, quote enclosed (https://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)",
)
@click.option("--out_file", "-o", help="Output file")
@bounds_opt
@click.option(
    "--bounds-crs", "--bounds_crs", help="CRS of provided bounds", default="EPSG:3005"
)
@verbose_opt
@quiet_opt
def dump(dataset, query, out_file, bounds, bounds_crs, verbose, quiet):
    """Write DataBC features to stdout as GeoJSON feature collection.

    \b
      $ bcdata dump bc-airports
      $ bcdata dump bc-airports --query "AIRPORT_NAME='Victoria Harbour (Shoal Point) Heliport'"
      $ bcdata dump bc-airports --bounds xmin ymin xmax ymax

     It can also be combined to read bounds of a feature dataset using Fiona:
    \b
      $ bcdata dump bc-airports --bounds $(fio info aoi.shp --bounds)

    """
    verbosity = verbose - quiet
    configure_logging(verbosity)
    table = bcdata.validate_name(dataset)
    data = bcdata.get_data(table, query=query, bounds=bounds, bounds_crs=bounds_crs)
    if out_file:
        with open(out_file, "w") as sink:
            sink.write(json.dumps(data))
    else:
        sink = click.get_text_stream("stdout")
        sink.write(json.dumps(data))


@cli.command()
@click.argument("dataset", type=click.STRING)
@click.option(
    "--query",
    help="A valid `CQL` or `ECQL` query (https://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)",
)
@bounds_opt
@indent_opt
@compact_opt
@dst_crs_opt
@click.option(
    "--pagesize", "-p", default=10000, help="Max number of records to request"
)
@click.option("--sortby", "-s", help="Name of sort field")
@click.option(
    "--bounds-crs", "--bounds_crs", help="CRS of provided bounds", default="EPSG:3005"
)
@click.option(
    "--max_workers", "-w", default=2, help="Max number of concurrent requests"
)
@verbose_opt
@quiet_opt
def cat(
    dataset,
    query,
    bounds,
    bounds_crs,
    indent,
    compact,
    dst_crs,
    pagesize,
    sortby,
    max_workers,
    verbose,
    quiet,
):
    """Write DataBC features to stdout as GeoJSON feature objects."""
    # Note that cat does not concatenate!
    verbosity = verbose - quiet
    configure_logging(verbosity)
    dump_kwds = {"sort_keys": True}
    if sortby:
        sortby = sortby.upper()
    if indent:
        dump_kwds["indent"] = indent
    if compact:
        dump_kwds["separators"] = (",", ":")
    table = bcdata.validate_name(dataset)
    for feat in bcdata.get_features(
        table,
        query=query,
        bounds=bounds,
        bounds_crs=bounds_crs,
        sortby=sortby,
        crs=dst_crs,
        pagesize=pagesize,
        max_workers=max_workers
    ):
        click.echo(json.dumps(feat, **dump_kwds))


@cli.command()
@click.argument("dataset", type=click.STRING)
@click.option(
    "--db_url",
    "-db",
    help="SQLAlchemy database url",
    default=os.environ.get("DATABASE_URL"),
)
@click.option("--table", help="Destination table name")
@click.option("--schema", help="Destination schema name")
@click.option(
    "--query",
    help="A valid `CQL` or `ECQL` query (https://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)",
)
@bounds_opt
@click.option(
    "--bounds-crs", "--bounds_crs", help="CRS of provided bounds", default="EPSG:3005"
)
@click.option(
    "--pagesize", "-p", default=10000, help="Max number of records to request"
)
@click.option(
    "--max_workers", "-w", default=2, help="Max number of concurrent requests"
)
@click.option(
    "--dim",
    default=None,
    help="Force the coordinate dimension to val (valid values are XY, XYZ)",
)
@click.option("--fid", default=None, help="Primary key of dataset")
@click.option(
    "--append",
    is_flag=True,
    help="Append data to existing table (--fid must be specified)",
)
@click.option("--promote_to_multi", is_flag=True, help="Promote features to multipart")
@click.option(
    "--no_timestamp",
    is_flag=True,
    help="Do not add download timestamp to bcdata meta table",
)
@click.option(
    "--makevalid",
    is_flag=True,
    help="run OGR's MakeValid() to ensure geometries are valid simple features",
)
@verbose_opt
@quiet_opt
def bc2pg(
    dataset,
    db_url,
    table,
    schema,
    query,
    bounds,
    bounds_crs,
    pagesize,
    max_workers,
    dim,
    fid,
    append,
    promote_to_multi,
    makevalid,
    no_timestamp,
    verbose,
    quiet,
):
    """Download a DataBC WFS layer to postgres - an ogr2ogr wrapper.

     \b
      $ bcdata bc2pg bc-airports --db_url postgresql://postgres:postgres@localhost:5432/postgis

    The default target database can be specified by setting the $DATABASE_URL
    environment variable.
    https://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls
    """

    # for this command, default to INFO level logging
    # (echo the ogr2ogr commands by default)
    verbosity = verbose - quiet
    log_level = max(10, 20 - 10 * verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level)
    log = logging.getLogger(__name__)
    # if using --append option, --fid is required
    # (we are managing the primary keys ourselves, so we want to be sure it is
    # the correct column, not simply relying on the best guess from bcdata.get_sortkey()
    if append and not fid:
        raise click.BadParameter("--fid must be provided when using --append")
    src = bcdata.validate_name(dataset)
    src_schema, src_table = [i.lower() for i in src.split(".")]
    if not schema:
        schema = src_schema
    if not table:
        table = src_table
    # always upper
    if fid:
        fid = fid.upper()
    # create schema if it does not exist
    db = Database(db_url)
    if schema not in db.schemas:
        click.echo("Schema {} does not exist, creating it".format(schema))
        dbq = sql.SQL("CREATE SCHEMA {schema}").format(schema=sql.Identifier(schema))
        db.execute(dbq)

    # if --append option provided, make sure table actually exists
    # if it does not exist, remove the -append option from ogr command
    if schema + "." + table not in db.tables and append:
        append = False
        click.echo("Table does not exist, creating")

    # build parameters for each required request
    param_dicts = bcdata.define_request(
        src,
        query=query,
        sortby=fid,
        pagesize=pagesize,
        bounds=bounds,
        bounds_crs=bounds_crs,
    )

    # define the url of request
    payload = urlencode(param_dicts[0], doseq=True)
    url = bcdata.WFS_URL + "?" + payload

    # build the ogr2ogr command
    command = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        db.ogr_string,
        "-t_srs",
        "EPSG:3005",
        "-nln",
        schema + "." + table,
        url,
    ]
    if append:
        command = command + ["-append"]
    else:
        command = command + ["-overwrite", "-lco", "GEOMETRY_NAME=geom"]
    if dim:
        command = command + ["-dim", dim]
    # if provided fid, assign it on layer creation
    if fid and not append:
        command = command + ["-lco", "FID=" + fid]
    # if appending to existing table, remove existing primary key constraint
    if fid and append:
        db.drop_pk(schema.lower(), table.lower(), fid.lower())
    # for speed with big loads - unlogged, no spatial index
    if not append:
        command = command + ["-lco", "UNLOGGED=ON"]
        command = command + ["-lco", "SPATIAL_INDEX=NONE"]
    if promote_to_multi:
        command = command + ["-nlt", "PROMOTE_TO_MULTI"]
    if makevalid:
        command = command + ["-makevalid"]
    log.info(" ".join(command))
    subprocess.run(command)

    # after initial load, drop the ogr created fid constraints if fid specified
    # (ogr fids create problems with updates and concurrent loads)
    if fid and not append:
        db.drop_pk(schema.lower(), table.lower(), fid.lower())

    # write to additional separate tables if data is larger than 10k recs
    temp_tables = [table + "_" + str(n) for n, paramdict in enumerate(param_dicts[1:])]

    try:
        if len(param_dicts) > 1:
            commands = []
            for n, paramdict in enumerate(param_dicts[1:]):
                # create table to load to (so types are identical)
                dbq = sql.SQL(
                    """
                    CREATE TABLE {schema}.{table_new}
                    (LIKE {schema}.{table}
                    INCLUDING ALL)
                    """
                ).format(
                    schema=sql.Identifier(schema),
                    table_new=sql.Identifier(table + "_" + str(n)),
                    table=sql.Identifier(table),
                )
                db.execute(dbq)
                payload = urlencode(paramdict, doseq=True)
                url = bcdata.WFS_URL + "?" + payload
                command = [
                    "ogr2ogr",
                    "-update",
                    "-append",
                    "-f",
                    "PostgreSQL",
                    db.ogr_string + " active_schema=" + schema,
                    "-t_srs",
                    "EPSG:3005",
                    "-nln",
                    table + "_" + str(n),
                    url,
                ]
                if dim:
                    command = command + ["-dim", dim]
                if promote_to_multi:
                    command = command + ["-nlt", "PROMOTE_TO_MULTI"]
                commands.append(command)
            # log all requests, not just the first one
            for c in commands:
                log.info(c)
            # https://stackoverflow.com/questions/14533458
            pool = Pool(max_workers)
            with click.progressbar(
                pool.imap(partial(call), commands), length=len(param_dicts)
            ) as bar:
                for returncode in bar:
                    if returncode != 0:
                        click.echo("Command failed: {}".format(returncode))

            # once loaded, combine & drop
            for n, _x in enumerate(param_dicts[1:]):
                temp_table = table + "_" + str(n)
                dbq = sql.SQL(
                    """
                    INSERT INTO {schema}.{table} SELECT * FROM {schema}.{temp_table}
                    """
                ).format(
                    schema=sql.Identifier(schema),
                    table=sql.Identifier(table),
                    temp_table=sql.Identifier(temp_table),
                )
                db.execute(dbq)
                dbq = sql.SQL("DROP TABLE {schema}.{temp_table}").format(
                    schema=sql.Identifier(schema), temp_table=sql.Identifier(temp_table)
                )
                db.execute(dbq)
    except:
        # if above fails for any reason, try and delete the temp tables
        for t in temp_tables:
            db.execute(
                sql.SQL("DROP TABLE IF EXISTS {schema}.{table}").format(
                    schema=sql.Identifier(schema), table=sql.Identifier(t)
                )
            )
        raise RuntimeError("Loading to or from temp tables failed")

    # when data loaded to multiple tables concurrently, the ogr generated
    # pk will not be unique when putting the data back together. Drop the
    # autognerated column and recreate a unique column
    if len(param_dicts) > 1 and not append and not fid:
        # drop existing non-unique ogc_fid
        dbq = sql.SQL(
            "ALTER TABLE {schema}.{table} DROP COLUMN ogc_fid CASCADE"
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))
        db.execute(dbq)
        # create new column
        dbq = sql.SQL(
            """
            ALTER TABLE {schema}.{table}
            ADD COLUMN ogc_fid SERIAL PRIMARY KEY
            """
        ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))
        db.execute(dbq)

    # once complete, set the table to logged and index geom
    if not append:
        db.execute(
            sql.SQL("ALTER TABLE {}.{} SET LOGGED").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        )
        log.info("Indexing geometry")
        db.execute("CREATE INDEX ON {}.{} USING GIST (geom)".format(schema, table))

    # if provided with a fid to use as pk, assign it here when closing out
    if fid:
        dbq = sql.SQL("ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({fid})").format(
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
            fid=sql.Identifier(fid.lower()),
        )
        db.execute(dbq)

    # once complete, note date/time of completion in public.bcdata
    if not no_timestamp:
        db.execute(
            "CREATE TABLE IF NOT EXISTS public.bcdata (table_name text PRIMARY KEY, date_downloaded timestamp WITH TIME ZONE);"
        )
        db.execute(
            """INSERT INTO public.bcdata (table_name, date_downloaded)
                        SELECT %s as table_name, NOW() as date_downloaded
                        ON CONFLICT (table_name) DO UPDATE SET date_downloaded = NOW();
                     """,
            (schema + "." + table,),
        )

    log.info(
        "Load of {} to {} in {} complete".format(src, schema + "." + table, db_url)
    )
