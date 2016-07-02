import os
import shutil

import click
import bcdata


def validate_email(ctx, param, value):
    if not value:
        raise click.BadParameter('Provide --email or set $BCDATA_EMAIL')

#@click.option('--driver')
#@click.option('--crs')
#@click.option('--bounds')
@click.command('bcdata')
@click.argument('dataset')
@click.option('--output', '-o')
@click.option('--email', envvar='BCDATA_EMAIL', callback=validate_email)
@click.option('--geomark')
def cli(dataset, output, email, geomark):
    """Download a dataset from BC Data Distribution Service"""
    order_id = bcdata.create_order(dataset, email, geomark=geomark)
    # download to temp
    dl_path = bcdata.download_order(order_id)
    # , file_format=driver, crs=crs, geomark=geomark)
    # if output not specified, write data as named by dl service to cwd
    if not output:
        output = os.path.join(os.getcwd(), os.path.basename(dl_path))
    # only write if folder doesn't exist
    if os.path.exists(output):
        click.echo('Output folder exists. Find download in '+dl_path)
    else:
        shutil.move(dl_path, output)
        click.echo(dataset + " downloaded to " + output)
