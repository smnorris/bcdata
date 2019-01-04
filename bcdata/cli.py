import json
import logging
import math
import os
import subprocess
from subprocess import Popen, PIPE
from urllib.parse import urlencode
from urllib.parse import urlparse

import click
from cligj import indent_opt
from owslib.wfs import WebFeatureService

import pgdata

import bcdata


bcdata.configure_logging()
log = logging.getLogger(__name__)


def parse_db_url(db_url):
    """provided a db url, return a dict with connection properties
    """
    u = urlparse(db_url)
    db = {}
    db["database"] = u.path[1:]
    db["user"] = u.username
    db["password"] = u.password
    db["host"] = u.hostname
    db["port"] = u.port
    return db


def get_objects(ctx, args, incomplete):
    return [k for k in bcdata.list_tables() if incomplete in k]


@click.group()
def cli():
    pass


@cli.command()
@click.option("--refresh", "-r", is_flag=True, help="Refresh the cached list")
def list(refresh):
    """List DataBC layers available via WMS
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
def info(dataset, indent, meta_member):
    """Print basic info about a DataBC WFS layer
    """
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
@click.argument("dataset", type=click.STRING, autocompletion=get_objects)
@click.option(
    "--query",
    help="A valid `CQL` or `ECQL` query (https://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)",
)
@click.option("--out_file", "-o", help="Output file")
@click.option(
    "--crs",
    type=click.Choice(["EPSG:3005", "EPSG:4326"]),
    help="Output coordinate reference system",
)
def dump(dataset, query, out_file, crs):
    """Dump a data layer from DataBC WFS
    """
    table = bcdata.validate_name(dataset)
    data = bcdata.get_data(table, query=query, crs=crs)
    if out_file:
        with open(out_file, "w") as f:
            json.dump(data.json(), f)
    else:
        sink = click.get_text_stream("stdout")
        sink.write(json.dumps(data))


@cli.command()
@click.argument("dataset", type=click.STRING, autocompletion=get_objects)
@click.option("--db_url", "-db", default=os.environ["DATABASE_URL"])
@click.option(
    "--query",
    help="A valid `CQL` or `ECQL` query (https://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)",
)
@click.option(
    "--pagesize", "-p", default=10000, help="Max number of records to request"
)
@click.option("--sortby", "-s", help="Name of sort field")
def bc2pg(dataset, db_url, query, pagesize, sortby):
    """Replicate a DataBC table in a postgres database
    """
    # Just a wrapper around a command like this:
    """
    ogr2ogr \
          -f PostgreSQL \
          PG:"host=localhost user=postgres dbname=postgis password=postgres" \
          -lco SCHEMA=whse_imagery_and_base_maps \
          -lco GEOMETRY_NAME=geom \
          -nln gsr_airports_svw \
          "https://openmaps.gov.bc.ca/geo/pub/WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW/wfs?service=WFS&version=2.0.0&request=GetFeature&typeName=WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW&outputFormat=json&SRSNAME=epsg%3A3005"
    """
    src_table = bcdata.validate_name(dataset)
    schema, table = [i.lower() for i in src_table.split(".")]

    # create schema if it does not exist
    conn = pgdata.connect(db_url)
    if schema not in conn.schemas:
        click.echo("Schema {} does not exist, creating it".format(schema))
        conn.create_schema(schema)

    db = parse_db_url(db_url)
    request = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typename": src_table,
        "outputFormat": "json",
    }
    if query:
        request["CQL_FILTER"] = query

    n = bcdata.get_count(src_table)

    # for tables smaller than the pagesize, just get everything at once
    if n <= pagesize:
        payload = urlencode(request, doseq=True)
        url = bcdata.WFS_URL + "?" + payload
        command = [
            "ogr2ogr",
            "-f PostgreSQL",
            'PG:"host={h} user={u} dbname={db} password={pwd}"'.format(
                h=db["host"], u=db["user"], db=db["database"], pwd=db["password"]
            ),
            "-t_srs EPSG:3005",
            "-lco OVERWRITE=YES",
            "-lco SCHEMA={}".format(schema),
            "-lco GEOMETRY_NAME=geom",
            "-nln {}".format(table),
            '"' + url + '"',
        ]
        click.echo("Loading {} to {}".format(src_table, db_url))
        subprocess.call(" ".join(command), shell=True)

    # for bigger tables, iterate through the chunks
    else:
        # first, how many requests need to be made?
        chunks = math.ceil(n / pagesize)

        # A sort key is needed when using startindex.
        # If we don't know what we want to sort by, just pick the first
        # column in the table in alphabetical order...
        # Ideally we would get the primary key from bcdc api, but it doesn't
        # seem to be available
        if not sortby:
            wfs = WebFeatureService(url=bcdata.OWS_URL, version="2.0.0")
            sortby = sorted(wfs.get_schema("pub:" + src_table)["properties"].keys())[0]

        # run the first insert
        request["sortby"] = sortby
        request["startIndex"] = 0
        request["count"] = pagesize
        payload = urlencode(request, doseq=True)
        url = bcdata.WFS_URL + "?" + payload
        command = [
            "ogr2ogr",
            "-f PostgreSQL",
            'PG:"host={h} user={u} dbname={db} password={pwd}"'.format(
                h=db["host"], u=db["user"], db=db["database"], pwd=db["password"]
            ),
            "-t_srs EPSG:3005",
            "-lco OVERWRITE=YES",
            "-lco SCHEMA={}".format(schema),
            "-nln {}".format(table),
            '"' + url + '"',
        ]
        click.echo("Loading chunk 1 of {}".format(str(chunks)))
        subprocess.call(" ".join(command), shell=True)

        # now append to the newly created table
        # first, build the command strings
        commands = []
        for i in range(1, chunks):
            request["startIndex"] = i * pagesize
            payload = urlencode(request, doseq=True)
            url = bcdata.WFS_URL + "?" + payload
            command = [
                "ogr2ogr",
                "-update",
                "-append",
                "-f PostgreSQL",
                # note that schema must be specified here in connection
                # string when appending to a layer, -lco opts are ignored
                'PG:"host={h} user={u} dbname={db} password={pwd} active_schema={s}"'.format(
                    h=db["host"],
                    u=db["user"],
                    db=db["database"],
                    pwd=db["password"],
                    s=schema,
                ),
                "-t_srs EPSG:3005",
                "-nln {}".format(table),
                '"' + url + '"',
            ]
            commands.append(" ".join(command))

        # now execute in parallel
        click.echo("Loading remaining chunks in parallel")
        procs_list = [Popen(cmd, shell=True) for cmd in commands]
        for proc in procs_list:
            proc.wait()

    # todo - add a check to make sure feature counts add up

    click.echo("Load of {} to {} complete".format(src_table, db_url))
