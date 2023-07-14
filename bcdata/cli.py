import sys
import json
import logging
import os
import re

import click
from cligj import indent_opt
from cligj import compact_opt
from cligj import verbose_opt, quiet_opt

import bcdata

LOG_FORMAT = "%(asctime)s:%(levelname)s:%(name)s: %(message)s"


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level, format=LOG_FORMAT)


def complete_dataset_names(ctx, param, incomplete):
    return [k for k in bcdata.list_tables() if k.startswith(incomplete)]


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


# tab completion object names not currently supported
# def complete_object_names(ctx, param, incomplete):
#    return [k for k in bcdata.list_tables() if k.startswith(incomplete.upper())]


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

dst_crs_opt = click.option(
    "--dst-crs", "--dst_crs", default="epsg:4326", help="Destination CRS"
)

lowercase_opt = click.option(
    "--lowercase", "-l", is_flag=True, help="Write column/properties names as lowercase"
)


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
    "--count",
    "meta_member",
    flag_value="count",
    help="Print the count of features.",
)
@click.option(
    "--name",
    "meta_member",
    flag_value="name",
    help="Print the table name of the dateset.",
)
@verbose_opt
@quiet_opt
def info(dataset, indent, meta_member, verbose, quiet):
    """Print basic metadata about a DataBC WFS layer as JSON.

    Optionally print a single metadata item as a string.
    """
    verbosity = verbose - quiet
    configure_logging(verbosity)
    dataset = bcdata.validate_name(dataset)
    info = {}
    info["name"] = dataset
    info["count"] = bcdata.get_count(dataset)
    info["description"], info["schema"] = bcdata.get_table_definition(dataset)
    if meta_member:
        click.echo(info[meta_member])
    else:
        click.echo(json.dumps(info, indent=indent))


@cli.command()
@click.option("--out_file", "-o", help="Output file", default="dem25.tif")
@bounds_opt_dem
@click.option(
    "--dst-crs",
    help="CRS of output file",
    default="EPSG:3005",
)
@click.option(
    "--bounds-crs",
    help="CRS of provided bounds",
    default="EPSG:3005",
)
@click.option("--resolution", "-r", type=int, default=25)
@click.option(
    "--align",
    "-a",
    is_flag=True,
    help="Align provided bounds to provincial standard",
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
    help="A valid CQL or ECQL query",
)
@click.option("--out_file", "-o", help="Output file")
@bounds_opt
@click.option(
    "--bounds-crs",
    "--bounds_crs",
    help="CRS of provided bounds",
    default="EPSG:3005",
)
@lowercase_opt
@verbose_opt
@quiet_opt
def dump(dataset, query, out_file, bounds, bounds_crs, lowercase, verbose, quiet):
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
    data = bcdata.get_data(
        table, query=query, bounds=bounds, bounds_crs=bounds_crs, lowercase=lowercase
    )
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
    help="A valid CQL or ECQL query",
)
@bounds_opt
@indent_opt
@compact_opt
@dst_crs_opt
@click.option("--pagesize", "-p", default=10000, help="Maximum request size")
@click.option("--sortby", "-s", help="Name of sort field")
@click.option(
    "--bounds-crs",
    "--bounds_crs",
    help="CRS of provided bounds",
    default="EPSG:3005",
)
@lowercase_opt
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
    lowercase,
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
        lowercase=lowercase,
    ):
        click.echo(json.dumps(feat, **dump_kwds))


@cli.command()
@verbose_opt
@quiet_opt
def clear_cache(verbose, quiet):
    bcdata.clear_cache()


@cli.command()
@click.argument("dataset", type=click.STRING, shell_complete=complete_dataset_names)
@click.option(
    "--db_url",
    "-db",
    help="Target database url, defaults to $DATABASE_URL environment variable if set",
    default=os.environ.get("DATABASE_URL"),
)
@click.option("--table", help="Destination table name")
@click.option("--schema", help="Destination schema name")
@click.option("--geometry_type", help="Spatial type of geometry column")
@click.option(
    "--query",
    help="A valid CQL or ECQL query",
)
@click.option(
    "--count",
    "-c",
    default=None,
    type=int,
    help="Total number of features to load",
)
@click.option("--pagesize", "-p", default=10000, help="Maximum request size")
@click.option("--primary_key", "-k", default=None, help="Primary key of dataset")
@click.option("--sortby", "-s", help="Name of sort field")
@click.option(
    "-e",
    "--schema_only",
    is_flag=True,
    help="Create empty table from catalogue schema",
)
@click.option(
    "--append",
    "-a",
    is_flag=True,
    help="Append to existing table",
)
@click.option(
    "--no_timestamp",
    "-t",
    is_flag=True,
    help="Do not log download to bcdata.log",
)
@verbose_opt
@quiet_opt
def bc2pg(
    dataset,
    db_url,
    table,
    schema,
    geometry_type,
    query,
    count,
    pagesize,
    primary_key,
    sortby,
    no_timestamp,
    schema_only,
    append,
    verbose,
    quiet,
):
    """Download a DataBC WFS layer to postgres

    \b
     $ bcdata bc2pg bc-airports --db_url postgresql://postgres:postgres@localhost:5432/postgis
    """
    # for this command, default to INFO level logging
    verbosity = verbose - quiet
    log_level = max(10, 20 - 10 * verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level, format=LOG_FORMAT)
    log = logging.getLogger(__name__)
    if no_timestamp:
        timestamp = False
    else:
        timestamp = True
    out_table = bcdata.bc2pg(
        dataset,
        db_url,
        table=table,
        schema=schema,
        query=query,
        geometry_type=geometry_type,
        count=count,
        pagesize=pagesize,
        primary_key=primary_key,
        sortby=sortby,
        timestamp=timestamp,
        schema_only=schema_only,
        append=append,
    )
    log.info("Load of {} to {} in {} complete".format(dataset, out_table, db_url))
