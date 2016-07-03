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


def validate_format(ctx, param, value):
    if value not in bcdata.FORMATS.keys():
        raise click.BadParameter("--format must be one of "+bcdata.FORMATS.keys())


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
@click.option('--output', '-o', help="Destination folder to write.")
@click.option('--format', '-f', help="Output file format. Default: FileGDB")
@click.option('--crs', help="Output file CRS. Default: EPSG:3005 (BC Albers)")
#@click.option('--bounds')
@click.option('--geomark', help="BC Geomark ID. Eg: gm-3D54AEE61F1847BA881E8BF7DE23BA21")
def cli(dataset, email, output, crs, format, geomark):
    """Download a dataset from BC Data Distribution Service"""
    order_id = bcdata.create_order(dataset,
                                   email,
                                   crs=crs,
                                   file_format=format,
                                   geomark=geomark)
    # download to temp
    dl_path = bcdata.download_order(order_id)
    # , file_format=driver, crs=crs, geomark=geomark)
    # if output not specified, write data as named by dl service to cwd
    if not output:
        output = os.path.join(os.getcwd(), os.path.basename(dl_path))
    # only write if folder doesn't exist
    # TODO - append to existing folder
    if os.path.exists(output):
        click.echo('Output folder exists. Find download in '+dl_path)
    else:
        shutil.move(dl_path, output)
        click.echo(dataset + " downloaded to " + output)
