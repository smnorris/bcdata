import json
import click
import logging

from cligj import indent_opt
from owslib.wfs import WebFeatureService

import bcdata


bcdata.configure_logging()
log = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command()
def list():
    """List DataBC layers available via WMS
    """
    # This works too, but is much slower:
    # ogrinfo WFS:http://openmaps.gov.bc.ca/geo/ows?VERSION=1.1.0
    for table in sorted(bcdata.list_tables()):
        click.echo(table)


@cli.command()
@click.argument("dataset")
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
@click.argument("dataset")
@click.option(
    "--query",
    help="A valid `CQL` or `ECQL` query (https://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)",
)
@click.option("--out_file", "-o", help="Output file")
@click.option("--number", "-n", help="Number of features to dump")
@click.option("--crs", type=click.Choice(['EPSG:3005', 'EPSG:4326']), help="Output coordinate reference system")
def dump(dataset, query, out_file, number, crs):
    """Dump a data layer from DataBC WFS
    """
    table = bcdata.validate_name(dataset)
    data = bcdata.get_data(table, query=query, number=number, crs=crs)
    if out_file:
        with open(out_file, "w") as f:
            json.dump(data.json(), f)
    else:
        sink = click.get_text_stream("stdout")
        sink.write(json.dumps(data))
