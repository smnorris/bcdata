.. image:: logo.gif

British Columbia's data distribution service, automated.

There is a `wealth of British Columbia geographic information available as open
data <https://catalogue.data.gov.bc.ca/dataset?download_audience=Public>`__,
but access to much of it is available only via a 'Custom Download' through the
`Data Distribution Service application <https://apps.gov.bc.ca/pub/dwds>`__ -
direct download urls are not available. Data analysis tasks requiring many
inputs can be tedious to set up or replicate.

This Python module and CLI script enables relatively quick, scriptable downloads of BC geographic data.


**Note**

- this tool is for my convenience, it is in no way endorsed by the Province of Britsh Columbia or DataBC
- use with care, please don't overload the service
- the download service seems to be ok with many download requests but failures may be unpredictable
- data are generally licensed as `OGL-BC <http://www2.gov.bc.ca/gov/content/governments/about-the-bc-government/databc/open-data/open-government-license-bc>`__, but it is up to the user to check the licensing for any data downloaded


Installation
-------------------------
bcdata has been tested only on macOS but a should work fine on other OS.

.. code-block:: console

    $ pip install https://github.com/smnorris/bcdata/zipball/master

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
    >>> dl = bcdata.download('bc-airports', 'pilot@scenicflights.ca')
    >>> dl
    /tmp/bcdata/unzipped_download_folder/GSR_AIRPORTS_SVW.gdb

Download times will vary based mainly on the size of your requested data. Expect
about a minute for the smallest requests to complete.


**CLI**

The CLI uses the $BCDATA_EMAIL environment variable if available, otherwise
an email address must be provided as an option.

.. code-block:: console

    $ bcdata --help
    Usage: bcdata [OPTIONS] DATASET

      Download a dataset from BC Data Distribution Service

    Options:
      --email TEXT       Email address. Default: $BCDATA_EMAIL
      -o, --output TEXT  Destination folder to write.
      -f, --format TEXT  Output file format. Default: FileGDB
      --help             Show this message and exit.

Common uses might look something like this:

.. code-block:: bash

    $ bcdata --email pilot@scenicflights.ca bc-airports  # basic usage
    $ export BCDATA_EMAIL=pilot@scenicflights.ca         # set a default email
    $ bcdata bc-airports                                 # use default email
    $ bcdata -o my_spots.gdb bc-airports                 # download to specified output location
    $ bcdata bc-airports \                               # get airports as shapefile
        -f shp \
        -o bc_airports

Note that data are downloaded to specified folder.  For above example, a
bc_airports folder would be created in the current working directory and the
individual shp, prj etc files would be found within.

Projections / CRS
-------------------------
Several projections are available on request from the Download Service, but this
tool does not support this option, all data are downloaded as the default
BC Albers (which should generally be EPSG:3005).

Use some other tool to reproject your downloads.


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
- `pyskel <https://github.com/mapbox/pyskel>`__
- @ateucher for the correct POST url and syntax