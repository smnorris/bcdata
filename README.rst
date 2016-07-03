bcdata
======

British Columbia's data distribution service, automated.

There is a `wealth of British Columbia geographic information available as open
data <https://catalogue.data.gov.bc.ca/dataset?download_audience=Public>`__,
but access to much of it is available only via a 'Custom Download' through the
`Data Distribution Service application <https://apps.gov.bc.ca/pub/dwds>`__ -
direct download urls are not available. Data analysis tasks requiring many
inputs can be tedious to set up or replicate.

This Python module uses the web browser automation tool
`Selenium <http://www.seleniumhq.org>`__ to enable relatively quick, scriptable
downloads of BC geographic data.


**Note**

- this tool is for my convenience, it is in no way endorsed by the Province of Britsh Columbia or DataBC
- use with care, please don't overload the service
- data are generally licensed as `OGL-BC <http://www2.gov.bc.ca/gov/content/governments/about-the-bc-government/databc/open-data/open-government-license-bc>`__, but it is up to the user to check the licensing for any data downloaded


Installation
-------------------------
bcdata has been tested only on macOS but a general installation should work fine
on other OS. Install with pip:

.. code-block:: console

    pip install https://github.com/smnorris/bcdata/zipball/master

Usage
-------------------------
The most basic usage requires:

- a valid email address (a required Distribution Service form input, the address is not otherwise used)
- url (or identifier portion of url path) for any `DataBC Catalog <https://catalogue.data.gov.bc.ca>`__ record that includes a 'Custom Download' button.

For example, to order and download `airport <https://catalogue.data.gov.bc.ca/dataset/bc-airports>`__ data, use either
:code:`https://catalogue.data.gov.bc.ca/dataset/bc-airports` or :code:`bc-airports`

**Python module**

.. code-block::

    >>> import bcdata
    >>> order_id = bcdata.create_order('bc-airports', 'pilot@scenicflights.ca')
    >>> out_data = bcdata.download_order(order_id)
    >>> out_data
    /temp/airports.gdb

**CLI**

The CLI usage should hopefully be familiar to users of
`fio <https://github.com/Toblerity/Fiona/blob/master/docs/cli.rst>`__,
`rio <https://github.com/mapbox/rasterio/blob/master/docs/cli.rst>`__, and
`ogr2ogr <http://www.gdal.org/ogr2ogr.html>`__.
The CLI uses the $BCDATA_EMAIL environment variable if available, otherwise
an email must be provided as an option.

.. code-block:: console

    $ bcdata --help
    Usage: bcdata [OPTIONS] DATASET

      Download a dataset from BC Data Distribution Service

    Options:
      --email TEXT       Email address. Default: $BCDATA_EMAIL
      -o, --output TEXT  Destination folder to write.
      -f, --format TEXT  Output file format. Default: FileGDB
      --crs TEXT         Output file CRS. Default: EPSG:3005 (BC Albers)
      --geomark TEXT     BC Geomark ID. Eg: gm-3D54AEE61F1847BA881E8BF7DE23BA21
      --help             Show this message and exit.

Common uses might look something like this:

.. code-block:: bash

    $ bcdata --email pilot@scenicflights.ca bc-airports  # basic usage
    $ export BCDATA_EMAIL=pilot@scenicflights.ca         # set a default email
    $ bcdata bc-airports                                 # use default email
    $ bcdata -o my_spots.gdb bc-airports                 # download to specified output location
    $ bcdata bc-airports \                               # get airports within geomark as NAD83 shapefile
        -f shp \
        --crs EPSG:4269 \
        -o crd_airports \
        --geomark gm-3D54AEE61F1847BA881E8BF7DE23BA21

Note that data are downloaded to specified folder.  For above example, a
crd_airports folder would be created in the current working directory and the
individual shp, prj etc files would be found within.

Download times will vary based on server load and size of dataset. Expect about
a minute for the smallest requests.

Development and testing
-------------------------
Note that tests require `Fiona <https://github.com/Toblerity/Fiona>`__ (and thus
`GDAL <http://www.gdal.org>`__) to verify downloads. Using a virtualenv is
probably a good idea.

**macOS/Linux/etc**

.. code-block:: console

    $ mkdir bcdata_env
    $ virtualenv bcdata_env
    $ source bcdata_env/bin/activate
    (bcdata_env)$ git clone git@github.com:smnorris/bcdata.git
    (bcdata_env)$ cd bcdata
    (bcdata_env)$ pip install -e .[test]
    (bcdata_env)$ export BCDATA_EMAIL=mytestemail@testing.ca
    (bcdata_env)$ py.test

**Windows**

Development setup on Windows should be quite similar but installing Fiona on
Windows can be `more challenging <https://github.com/Toblerity/Fiona#windows>`__.

Credits
-------------------------
- `Selenium <http://www.seleniumhq.org>`__
- `pyskel <https://github.com/mapbox/pyskel>`__