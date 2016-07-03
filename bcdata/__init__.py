try:
    from urllib.parse import urlparse
    from urllib.request import urlretrieve
except ImportError:
    from urlparse import urlparse
    from urllib import urlretrieve

import os
import tempfile
import zipfile
import shutil
import logging
import sys

import requests
from selenium import webdriver
from selenium.webdriver.support.ui import Select
import polling

# tag version
__version__ = "0.0.1"

# Data BC URLs
CATALOG_URL = 'https://catalogue.data.gov.bc.ca'
DOWNLOAD_URL = "https://apps.gov.bc.ca/pub/dwds/initiateDownload.do?"

# supported dwds file formats (and shortcuts)
FORMATS = {"ESRI Shapefile": "0",
           "Shapefile": "0",
           "shp": "0",
           "CSV": "2",
           "FileGDB": "3",
           "gdb": "3"}

# dwds formats not supported by this module
UNSUPPORTED = {"AVCE00": "1",
               "GeoRSS": "4"}

# dwds supported projections
CRS = {"EPSG:3005": "0",
       "EPSG:26907": "1",
       "EPSG:26908": "2",
       "EPSG:26909": "3",
       "EPSG:26910": "4",
       "EPSG:26911": "5",
       "EPSG:4269": "6"   # note spherical available as NAD83 rather than WGS84
       }

#logging.basicConfig(stream=sys.stderr, level=logging.INFO)


def create_order(url, email_address, file_format="FileGDB", crs="EPSG:3005",
                 geomark=None):
    """Submit a Data BC Distribution Service order for the specified dataset"""
    # if just the key is provided, pre-pend the full url
    if os.path.split(url)[0] == '':
        url = os.path.join(CATALOG_URL, 'dataset', url)
    # check that url exists
    if requests.get(url).status_code != 200:
        raise ValueError('DataBC Catalog URL does not exist')
    #try:
    driver = webdriver.Firefox()
    driver.get(url)
    # within the catalog page, find the link to the custom download
    for element in driver.find_elements_by_tag_name('a'):
        if element.get_attribute("title").endswith("- Custom Download"):
            custom_download_link = element
    custom_download_link.click()
    # fill out the distribution service form
    crs_selector = Select(driver.find_element_by_name("crs"))
    crs_selector.select_by_value(CRS[crs])
    fileformat_selector = Select(driver.find_element_by_name("fileFormat"))
    fileformat_selector.select_by_value(FORMATS[file_format])
    email = driver.find_element_by_name('userEmail')
    email.send_keys(email_address)
    terms = driver.find_element_by_name('termsCheckbox')
    terms.click()
    # If geomark is applied first the terms element becomes stale
    # rather than figure out how to wait for page load
    # http://www.obeythetestinggoat.com/how-to-get-selenium-to-wait-for-page-load-after-a-click.html
    # just be sure to specify the geomark last
    if geomark:
        aoi = Select(driver.find_element_by_name("aoiOption"))
        aoi.select_by_value("4")
        geomark_form = driver.find_element_by_name("geomark")
        geomark_form.send_keys(geomark)
        geomark_recalc = driver.find_element_by_name("geomark_recalc")
        geomark_recalc.click()
    # submit order
    submit = driver.find_element_by_id('submitImg')
    submit.click()
    # get order id
    order_id = urlparse(driver.current_url).query.split('=')[1]
    driver.close()
    return order_id
    #except:
    #    raise RuntimeError("Error during order processing")


def download_order(order_id, outpath=None, timeout=1800):
    """
    Download and extract an order
    """
    # has the download url been provided?
    try:
        polling.poll(
            lambda: requests.get(DOWNLOAD_URL,
                                 {'orderId': order_id}).status_code < 400,
            step=20,
            timeout=timeout)
    except:
        raise RuntimeError("Download for order_id "+order_id+" timed out")
    r = requests.get(DOWNLOAD_URL, {'orderId': order_id})
    url = r.text.split('<iframe height="0" width="0" src="')[1]
    url = url.split('"></iframe>')[0]
    # download to temp
    download_file = os.path.join(tempfile.gettempdir(), os.path.basename(url))
    unzip_folder = os.path.join(tempfile.gettempdir(),
                                os.path.splitext(os.path.basename(url))[0])
    if not os.path.exists(unzip_folder):
        os.makedirs(unzip_folder)
    urlretrieve(url, download_file)
    # extract file
    zip_ref = zipfile.ZipFile(download_file, 'r')
    zip_ref.extractall(unzip_folder)
    zip_ref.close()
    # data is held in the only folder present in extract
    # find folder name: https://stackoverflow.com/questions/973473
    folders = next(os.walk(unzip_folder))[1]
    # make sure some data was actually downloaded
    if folders:
        folder = folders[0]
        datapath = os.path.join(unzip_folder, folder)
        if outpath:
            shutil.copytree(datapath, os.path.join(outpath, folder))
            datapath = os.path.join(outpath, folder)
        return datapath
    else:
        return None
