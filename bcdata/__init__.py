try:
    from urllib.parse import urlparse
    from urllib.parse import urljoin
    from urllib.request import urlretrieve

except ImportError:
    from urlparse import urlparse
    from urlparse import urljoin
    from urllib import urlretrieve

import os
import tempfile
import zipfile

import requests
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import polling


# tag version
__version__ = "0.0.4"

# Data BC URLs
CATALOG_URL = 'https://catalogue.data.gov.bc.ca'
DWDS = "https://apps.gov.bc.ca/pub/dwds"
DOWNLOAD_URL = "https://apps.gov.bc.ca/pub/dwds/initiateDownload.do?"

# supported dwds file formats (and shortcuts)
FORMATS = {"ESRI Shapefile": "0",
           "CSV": "2",
           "FileGDB": "3"}

# dwds formats not supported by this module
UNSUPPORTED = {"AVCE00": "1",
               "GeoRSS": "4"}

# dwds supported projections
# note that BC Albers may not be EPSG:3005 and EPSG:4326 is unavailable
CRS = {"BCAlbers": "0",
       "UTMZ07": "1",
       "UTMZ08": "2",
       "UTMZ09": "3",
       "UTMZ10": "4",
       "UTMZ11": "5",
       "NAD83": "6"}

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) " +
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36")


def create_order(url, email_address, driver="FileGDB", crs="BCAlbers",
                 geomark=None):
    """
    Submit a Data BC Distribution Service order for the specified dataset
    """
    # if just the key is provided, pre-pend the full url
    if os.path.split(url)[0] == '':
        url = os.path.join(CATALOG_URL, 'dataset', url)
    # check that url exists
    if requests.get(url).status_code != 200:
        raise ValueError('DataBC Catalog URL does not exist')

    dcap = dict(DesiredCapabilities.PHANTOMJS)
    dcap["phantomjs.page.settings.userAgent"] = USER_AGENT
    browser = webdriver.PhantomJS(desired_capabilities=dcap)
    browser.set_window_size(2560, 1440)
    browser.get(url)

    # within the catalog page, find the link to the custom download
    download_link = browser.find_element_by_css_selector("a[href*='"+DWDS+"']")
    download_link.click()
    # once loaded, fill out the distribution service form
    try:
        crs_element = WebDriverWait(browser, 60).until(
            EC.presence_of_element_located((By.NAME, "crs"))
        )
        crs_selector = Select(crs_element)
        crs_selector.select_by_value(CRS[crs])
        fileformat_selector = Select(
                                browser.find_element_by_name("fileFormat"))
        fileformat_selector.select_by_value(FORMATS[driver])
        email = browser.find_element_by_name('userEmail')
        email.send_keys(email_address)
        terms = browser.find_element_by_name('termsCheckbox')
        terms.click()
        # If geomark is applied first the terms element becomes stale
        # rather than figure out how to wait for page load
        # http://www.obeythetestinggoat.com/how-to-get-selenium-to-wait-for-page-load-after-a-click.html
        # just be sure to specify the geomark last
        if geomark:
            aoi_element = polling.poll(
                lambda: browser.find_element_by_name('aoiOption'),
                step=0.25,
                timeout=5)
            aoi = Select(aoi_element)
            aoi.select_by_value("4")
            geomark_form = browser.find_element_by_name("geomark")
            geomark_form.send_keys(geomark)
            geomark_recalc = browser.find_element_by_name("geomark_recalc")
            geomark_recalc.click()
        # submit order
        submit = polling.poll(
            lambda: browser.find_element_by_id('submitImg'),
            step=.25,
            timeout=5)
        submit.click()
        # get order id
        order_id = urlparse(browser.current_url).query.split('=')[1]
        browser.close()
        return order_id
    except:
        browser.quit()
        raise RuntimeError("Request timed out")


def download_order(order_id, timeout=1800):
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
        return os.path.join(unzip_folder, folders[0])
    else:
        return None
