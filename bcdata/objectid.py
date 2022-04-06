import bcdata
from owslib.wfs import WebFeatureService
import click

tables = bcdata.list_tables()
with click.progressbar(tables) as bar:
    for table in bar:
        wfs = WebFeatureService(url=bcdata.OWS_URL, version="2.0.0")
        columns = list(wfs.get_schema("pub:" + table)["properties"].keys())
        if "OBJECTID" not in columns:
            print("{table} does not include OBJECTID column".format(table=table))
