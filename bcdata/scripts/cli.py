import os
import shutil

import click
import bcdata


def validate_email(ctx, param, value):
    if not value:
        raise click.BadParameter('Provide --email or set $BCDATA_EMAIL')
    else:
        return value


def validate_crs(ctx, param, value):
    if value not in bcdata.CRS.keys():
        raise click.BadParameter('--crs must be one of '+bcdata.CRS.keys())
    return value


def validate_format(ctx, param, value):
    formats = bcdata.FORMATS.keys()
    # add shortcuts to formats
    shortcuts = {"shp": "ESRI Shapefile",
                 "Shapefile": "ESRI Shapefile",
                 "gdb": "FileGDB"}
    valid_keys = bcdata.FORMATS.keys()+shortcuts.keys()
    if value in shortcuts.keys():
        value = shortcuts[value]
    if value not in valid_keys:
        raise click.BadParameter("--format must be one of "+valid_keys)
    return value


#def validate_geomark(ctx, param, value):
#    raise click.BadParameter("--geomark does not exist")


#def validate_geomark(ctx, param, value):
#    raise click.BadParameter("--bounds are invalid or outside of BC")

@click.command('bcdata')
@click.argument('dataset')
@click.option('--email',
              help="Email address. Default: $BCDATA_EMAIL",
              envvar='BCDATA_EMAIL',
              callback=validate_email)
@click.option('--driver', '-d', default="FileGDB",
              help="Output file format. Default: FileGDB",
              callback=validate_format)
@click.option('--output', '-o', help="Output folder/gdb")
#@click.option('--layer', '-l', help="Output layer/shp")
@click.option('--crs', default="BCAlbers",
              callback=validate_crs,
              help="Downloaded CRS. Default: BCAlbers)")
#@click.option('--bounds')
@click.option('--geomark', help="BC Geomark ID. Eg: gm-3D54AEE61F1847BA881E8BF7DE23BA21")
def cli(dataset, email, driver, output, layer, crs, geomark):
    """Download a dataset from BC Data Distribution Service"""
    # create the order
    order_id = bcdata.create_order(dataset,
                                   email,
                                   crs=crs,
                                   driver=driver,
                                   geomark=geomark)
    if not order_id:
        click.abort("Failed to create order")
    # download the order to temp
    dl_path = bcdata.download_order(order_id)
    if not dl_path:
        click.abort("No data downloaded, check email to view issue")
    # if output not given, write to current directory using default folder name
    if not output:
        output = os.path.join(os.getcwd(), os.path.split(dl_path)[1])
    # copy data to specified path
    shutil.copy(dl_path, output)
    click.echo(dataset + " downloaded to " + output)
