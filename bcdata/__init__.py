import os
import urllib2
import tempfile
import zipfile
import logging

import requests
from bs4 import BeautifulSoup

import polling


# tag version
__version__ = "0.0.5"

# Data BC URLs
CATALOG_URL = 'https://catalogue.data.gov.bc.ca'
DWDS = "https://apps.gov.bc.ca/pub/dwds"
DWDS_SUBMIT = DWDS+"/viewOrderSubmit.so"

DOWNLOAD_URL = "https://apps.gov.bc.ca/pub/dwds/initiateDownload.do?"

# supported dwds file formats (and shortcuts)
FORMATS = {"ESRI Shapefile": "0",
           "CSV": "2",
           "FileGDB": "3"}

# dwds formats not supported by this module
UNSUPPORTED = {"AVCE00": "1",
               "GeoRSS": "4"}


def make_sure_path_exists(path):
    """
    Make directories in path if they do not exist.
    Modified from http://stackoverflow.com/a/5032238/1377021
    """
    try:
        os.makedirs(path)
    except:
        pass


def download(url, email_address, driver="FileGDB"):
    """Submit a Data BC Distribution Service order for the specified dataset
    """
    # if just the key is provided, pre-pend the full url
    if os.path.split(url)[0] == '':
        url = os.path.join(CATALOG_URL, 'dataset', url)
    # request the url
    r = requests.get(url)
    # bail if it doesn't exist
    if r.status_code != 200:
        raise ValueError('DataBC Catalog URL does not exist')

    # find the download link on the catalog page
    soup = BeautifulSoup(r.text, "html5lib")
    dwds_link = soup.select('a[href^='+DWDS+']')[0].get("href")

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
               "fileFormat": FORMATS[driver],
               "termsCheckbox": "1",
               "clickedSubmit": "true",
               "userEmail": email_address}
    r = requests.post(DWDS_SUBMIT,
                      params={"orderId": order_id},
                      data=payload)

    # is the download ready?
    timeout = 3600
    try:
        polling.poll(
            lambda: requests.get(DOWNLOAD_URL,
                                 params = {'orderId': order_id}).status_code < 400,
            step=20,
            timeout=timeout)
    except:
        raise RuntimeError("Download for order_id "+order_id+" timed out")

    # download the zipfile to tmp
    r = requests.get(DOWNLOAD_URL, params = {'orderId': order_id})
    soup = BeautifulSoup(r.text, "html5lib")
    url = soup.select('a.body')[0].get('href')
    download_path = os.path.join(tempfile.gettempdir(), "bcdata")
    make_sure_path_exists(download_path)

    # using a simple urlretrieve works, but complains and fails tests with:
    # >>>  IOError: [Errno ftp error] 200 Type set to I.
    # use urllib2 instead as per
    # https://github.com/OpenBounds/Processing/blob/master/utils.py
    fp = tempfile.NamedTemporaryFile('wb', dir=download_path, suffix="zip",
                                     delete=False)
    download = urllib2.urlopen(url)
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
    # data is held in the only folder present in extract
    # find folder name: https://stackoverflow.com/questions/973473
    folders = next(os.walk(unzip_folder))[1]
    # make sure some data was actually downloaded
    if folders:
        return os.path.join(unzip_folder, folders[0])
    else:
        return None
