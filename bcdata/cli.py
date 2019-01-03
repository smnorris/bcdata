import json
import click

from cligj import indent_opt
from owslib.wfs import WebFeatureService

import bcdata


@click.group()
def cli():
    pass


@cli.command()
@click.argument("dataset")
@click.option(
    "--query",
    help="A valid `CQL` or `ECQL` query (https://docs.geoserver.org/stable/en/user/tutorials/cql/cql_tutorial.html)",
)
@click.option("--out_file", "-o", help="Output file")
@click.option("--number", "-n", help="Number of features to dump")
def dump(dataset, query, out_file, number):
    """Dump a data layer from DataBC WFS
    """
    if dataset in bcdata.list_tables():
        table = dataset
    else:
        table = bcdata.bcdc_package_show(dataset)["object_name"]
    data = bcdata.get_data(table, query=query, number=number)
    if out_file:
        with open(out_file, "w") as f:
            json.dump(data.json(), f)
    else:
        sink = click.get_text_stream("stdout")
        sink.write(json.dumps(data))


@cli.command()
def list():
    """List DataBC layers available to dump
    """
    # This works too, but is much slower:
    # ogrinfo WFS:http://openmaps.gov.bc.ca/geo/ows?VERSION=1.1.0

    # perhaps cache this list for speed?
    # if cached, could use to validate dataset arg for dump and count
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
    if dataset in bcdata.list_tables():
        table = dataset
    else:
        table = bcdata.bcdc_package_show(dataset)["object_name"]
    wfs = WebFeatureService(url=bcdata.SERVICE_URL, version="2.0.0")
    info = {}
    info["name"] = table
    info["count"] = bcdata.get_count(table)
    info["schema"] = wfs.get_schema("pub:" + table)
    if meta_member:
        click.echo(info[meta_member])
    else:
        click.echo(json.dumps(info, indent=indent))
