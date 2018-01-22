from __future__ import absolute_import
import os
try:
    from urllib.request import urlopen
except ImportError:
    from six.moves.urllib.request import urlopen
import tempfile
import zipfile

import requests
from bs4 import BeautifulSoup
import polling

import bcdata

# Data BC URLs
BCDC_API_URL = 'https://catalogue.data.gov.bc.ca/api/3/action/'
DWDS = "https://apps.gov.bc.ca/pub/dwds"
DWDS_SUBMIT = DWDS+"/viewOrderSubmit.so"
DOWNLOAD_URL = "https://apps.gov.bc.ca/pub/dwds/initiateDownload.do?"


def make_sure_path_exists(path):
    """
    Make directories in path if they do not exist.
    Modified from http://stackoverflow.com/a/5032238/1377021
    """
    try:
        os.makedirs(path)
    except:
        pass


def package_show(package):
    """Return basic info about a DataBC Catalogue dataset
    """
    params = {'id': package}
    r = requests.get(BCDC_API_URL+'package_show', params=params)
    if r.status_code != 200:
        raise ValueError('{d} is not present in DataBC API list'
                         .format(d=package))
    return r.json()['result']


def download(package, email_address, driver="FileGDB", download_path=None,
             timeout=7200):
    """Submit a Data BC Distribution Service order for the specified dataset
    """
    package_info = package_show(package)
    dwds_resources = [resource['url'] for resource in package_info['resources']
                      if 'dwds' in resource['url']]

    # assume that the first resource is the one resource
    if len(dwds_resources) > 0:
        dwds_link = dwds_resources[0]
    else:
        raise ValueError('Specified package is not available via DWDS')

    # open the download link
    r = requests.get(dwds_link)
    if r.status_code != 200:
        raise ValueError('DWDS URL does not exist, something went wrong')

    # build the POST request
    order_id = r.cookies["DWDS_orderId"]
    payload = {"aoiOption": "0",
               "clippingMethodMapsheet": "0",
               "recalc_type": "mapsheet",
               "clippingMethod": "0",
               "clippingMethodShape": "0",
               "prj": "BC Albers",
               "clippingMethodGeomark": "0",
               "recalc_type": "geomark",
               "crs": "0",
               "fileFormat": bcdata.formats[driver],
               "termsCheckbox": "1",
               "clickedSubmit": "true",
               "userEmail": email_address}
    r = requests.post(
        DWDS_SUBMIT,
        params={"orderId": order_id},
        data=payload)

    # is the download ready?
    try:
        polling.poll(lambda: requests.get(
            DOWNLOAD_URL,
            params={'orderId': order_id}).status_code < 400,
            step=20,
            timeout=timeout)
    except:
        raise RuntimeError("Download for order_id "+order_id+" timed out")

    # download the zipfile to tmp
    r = requests.get(DOWNLOAD_URL, params={'orderId': order_id})
    soup = BeautifulSoup(r.text, "html5lib")
    url = soup.select('a.body')[0].get('href')

    # download to /bcdata in temp if no path supplied
    if not download_path:
        download_path = os.path.join(tempfile.gettempdir(), "bcdata")

    make_sure_path_exists(download_path)

    # using a simple urlretrieve works, but complains and fails tests with:
    # >>>  IOError: [Errno ftp error] 200 Type set to I.
    # use urllib2 instead as per
    # https://github.com/OpenBounds/Processing/blob/master/utils.py
    fp = tempfile.NamedTemporaryFile(
        'wb',
        dir=download_path,
        suffix="zip",
        delete=False)
    download = urlopen(url)
    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = download.read(block_sz)
        if not buffer:
            break
        file_size_dl += len(buffer)
        fp.write(buffer)
    fp.close()

    # extract zipfile
    unzip_folder = os.path.join(download_path,
                                os.path.splitext(os.path.basename(url))[0])
    make_sure_path_exists(unzip_folder)
    zip_ref = zipfile.ZipFile(fp.name, 'r')
    zip_ref.extractall(unzip_folder)
    zip_ref.close()
    # delete the temporary zipfile
    os.unlink(fp.name)
    # data is held in the only folder present in extract
    # find folder name: https://stackoverflow.com/questions/973473
    folders = next(os.walk(unzip_folder))[1]
    # make sure some data was actually downloaded
    if folders:
        return os.path.join(unzip_folder, folders[0])
    else:
        return None
