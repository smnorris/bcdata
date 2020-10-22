import sys
import json
import logging
import os
import re
import subprocess
from urllib.parse import urlencode
from urllib.parse import urlparse
from functools import partial
from multiprocessing.dummy import Pool
from subprocess import call

import click
from cligj import indent_opt
from cligj import compact_opt
from cligj import verbose_opt, quiet_opt

from owslib.wfs import WebFeatureService

import pgdata

import bcdata


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level)


def parse_db_url(db_url):
    """provided a db url, return a dict with connection properties
    """
    u = urlparse(db_url)
    db = {}
    db["database"] = u.path[1:]
    db["user"] = u.username
    db["host"] = u.hostname
    db["port"] = u.port
    db["password"] = u.password
    return db


def get_objects(ctx, args, incomplete):
    return [k for k in bcdata.list_tables() if incomplete.upper() in k]


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
    """List DataBC layers available via WFS
    """
    # This works too, but is much slower:
    # ogrinfo WFS:http://openmaps.gov.bc.ca/geo/ows?VERSION=1.1.0
    for table in bcdata.list_tables(refresh):
        click.echo(table)


@cli.command()
@click.argument("dataset", type=click.STRING, autocompletion=get_objects)
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
    "--interpolation",
    "-i",
    type=click.Choice(["nearest", "bilinear", "bicubic"], case_sensitive=False),
)
@verbose_opt
@quiet_opt
def dem(
    bounds, bounds_crs, dst_crs, out_file, resolution, interpolation, verbose, quiet
):
    """Dump BC DEM to TIFF
    """
    verbosity = verbose - quiet
    configure_logging(verbosity)
    if not dst_crs:
        dst_crs = "EPSG:3005"
    bcdata.get_dem(
        bounds,
        out_file=out_file,
        src_crs=bounds_crs,
        dst_crs=dst_crs,
        resolution=resolution,
        interpolation=interpolation,
    )


@cli.command()
@click.argument("dataset", type=click.STRING, autocompletion=get_objects)
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
@click.argument("dataset", type=click.STRING, autocompletion=get_objects)
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
    verbose,
    quiet,
):
    """Write DataBC features to stdout as GeoJSON feature objects.
    """
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
    ):
        click.echo(json.dumps(feat, **dump_kwds))


@cli.command()
@click.argument("dataset", type=click.STRING, autocompletion=get_objects)
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
@click.option("--append", is_flag=True, help="Append data to existing table")
@click.option("--promote_to_multi", is_flag=True, help="Promote features to multipart")
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
    conn = pgdata.connect(db_url)
    if schema not in conn.schemas:
        click.echo("Schema {} does not exist, creating it".format(schema))
        conn.create_schema(schema)

    # if table does not exist already, remove the -append flag
    if schema + "." + table not in conn.tables and append:
        append = False
        click.echo("Table does not exist, creating")

    # build parameters for each required request
    param_dicts = bcdata.define_request(
        dataset,
        query=query,
        sortby=fid,
        pagesize=pagesize,
        bounds=bounds,
        bounds_crs=bounds_crs,
    )

    # run the first request / load
    payload = urlencode(param_dicts[0], doseq=True)
    url = bcdata.WFS_URL + "?" + payload
    db = parse_db_url(db_url)
    db_string = "PG:host={h} user={u} dbname={db} port={port}".format(
        h=db["host"], u=db["user"], db=db["database"], port=db["port"],
    )
    if db["password"]:
        db_string = db_string + " password={pwd}".format(pwd=db["password"])
    # create the table
    command = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        db_string,
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
    # for speed with big loads - unlogged, no spatial index
    if not append:
        command = command + ["-lco", "UNLOGGED=ON"]
        command = command + ["-lco", "SPATIAL_INDEX=NONE"]
    if promote_to_multi:
        command = command + ["-nlt", "PROMOTE_TO_MULTI"]
    log.info(" ".join(command))
    subprocess.run(command)

    # write to additional separate tables if data is larger than 10k recs
    if len(param_dicts) > 1:
        commands = []
        for n, paramdict in enumerate(param_dicts[1:]):
            # create table to load to (so types are identical)
            sql = """
            CREATE TABLE {schema}.{table}_{n}
            (LIKE {schema}.{table}
            INCLUDING ALL)
            """.format(
                schema=schema, table=table, n=str(n)
            )
            conn.execute(sql)
            payload = urlencode(paramdict, doseq=True)
            url = bcdata.WFS_URL + "?" + payload
            command = [
                "ogr2ogr",
                "-update",
                "-append",
                "-f",
                "PostgreSQL",
                db_string + " active_schema=" + schema,
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
            sql = """INSERT INTO {schema}.{table} SELECT * FROM {schema}.{table}_{n}""".format(
                schema=schema, table=table, n=str(n)
            )
            conn.execute(sql)
            sql = "DROP TABLE {}.{}_{}".format(schema, table, n)
            conn.execute(sql)

    # Deal with primary key
    # First, drop ogc_fid - becaue we load to many tables, it is not unique
    sql = "ALTER TABLE {}.{} DROP COLUMN ogc_fid CASCADE".format(schema, table)
    conn.execute(sql)

    # if provided with a fid to use as pk, assign it
    if fid:
        sql = "ALTER TABLE {}.{} ADD PRIMARY KEY ({})".format(schema, table, fid)
        conn.execute(sql)
        # make fid auto-increment in case we want to add records
        sql = """
            CREATE SEQUENCE {schema}.{table}_{fid}_seq
            OWNED BY {schema}.{table}.{fid};

            SELECT
              setval('{schema}.{table}_{fid}_seq',
              coalesce(max({fid}), 0) + 1, false)
            FROM {schema}.{table};

            ALTER TABLE {schema}.{table}
            ALTER COLUMN {fid}
            SET DEFAULT nextval('{schema}.{table}_{fid}_seq');
        """.format(
            schema=schema, table=table, fid=fid
        )
        conn.execute(sql)
    # otherwise, create a new serial ogc_fid
    else:
        sql = "ALTER TABLE {}.{} ADD COLUMN ogc_fid SERIAL PRIMARY KEY".format(
            schema, table
        )
        conn.execute(sql)

    if not append:
        conn.execute("ALTER TABLE {}.{} SET LOGGED".format(schema, table))
        log.info("Indexing geometry")
        conn.execute("CREATE INDEX ON {}.{} USING GIST (geom)".format(schema, table))

    log.info(
        "Load of {} to {} in {} complete".format(src, schema + "." + table, db_url)
    )
