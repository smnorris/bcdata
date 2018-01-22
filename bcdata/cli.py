from __future__ import absolute_import
import json
import os
import shutil

import click
import bcdata


def validate_email(ctx, param, value):
    if not value:
        raise click.BadParameter('Provide --email or set $BCDATA_EMAIL')
    else:
        return value


def validate_format(ctx, param, value):
    # add shortcuts to formats
    shortcuts = {"shp": "ESRI Shapefile",
                 "Shapefile": "ESRI Shapefile",
                 "gdb": "FileGDB"}
    valid_keys = list(bcdata.formats.keys())+list(shortcuts.keys())
    if value in list(shortcuts.keys()):
        value = shortcuts[value]
    if value not in valid_keys:
        raise click.BadParameter("--format must be one of "+valid_keys)
    return value


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
@click.option('--info', '-i', is_flag=True, help="Display info about dataset")
def cli(dataset, email, driver, output, info):
    """Download a dataset from BC Data Distribution Service
    """
    if info:
        package_info = bcdata.package_show(dataset)
        object_name = package_info['object_name'].lower()
        r = {'schema': object_name.split('.')[0],
             'table': object_name.split('.')[1]}
        click.echo(json.dumps(r))

    else:
        # download to temp
        dl_path = bcdata.download(dataset,
                                  email,
                                  driver=driver)
        if not dl_path:
            click.abort("No data downloaded, check email to view issue")
        # if output not given, write to current directory using default folder name
        if not output:
            output = os.path.join(os.getcwd(), os.path.split(dl_path)[1])
        # copy data to specified path
        shutil.copytree(dl_path, output)
        click.echo(dataset + " downloaded to " + output)
