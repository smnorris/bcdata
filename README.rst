bcdata
======

British Columbia's data distribution service, automated.

There is a `wealth of British Columbia geographic information available as open
data <https://catalogue.data.gov.bc.ca/dataset?download_audience=Public>`__,
but access to much of it is available only via a 'Custom Download' through the
`Data Distribution Service application <https://apps.gov.bc.ca/pub/dwds>`__ -
direct download urls are not available. Data analysis tasks requiring many
inputs can be tedious to set up or replicate.

This module uses the web browser automation tool
`Selenium <http://www.seleniumhq.org>`__ to enable quick, scriptable downloads
of BC geographic data.


**Note**

- this project is for convenience only, it is in no way endorsed by the
  Province of Britsh Columbia or DataBC
- use with care, please don't overload the service


Installation
-------------------------
bcdata has been tested only on macOS but a general installation should work fine
on linux and Windows:

.. code-block:: console

    pip install https://github.com/smnorris/bcdata/zipball/master

Usage
-------------------------
The most basic usage requires:

- a valid email address (a required Distribution Service form input, the address is not otherwise used)
- the final item from the url path for any `DataBC Catalog <https://catalogue.data.gov.bc.ca>`__ record that includes a 'Custom Download' button.

For example, to order and download airport data
https://catalogue.data.gov.bc.ca/dataset/bc-airports, use :code:`bc-airports`:

**Python module**

.. code-block::

    >>> import bcdata
    >>> order_id = bcdata.create_order('bc-airports', 'pilot@scenicflights.ca')
    >>> out_data = bcdata.download_order(order_id)
    >>> out_data
    /temp/airports.gdb

**CLI**

The syntax is a mash of
`fio <https://github.com/Toblerity/Fiona/blob/master/docs/cli.rst>`__ and
`rio <https://github.com/mapbox/rasterio/blob/master/docs/cli.rst>`__, with a
sprinkling of `ogr2ogr <http://www.gdal.org/ogr2ogr.html>`__.
The cli will use the $DATABC_EMAIL environment variable if it is set, otherwise
the user will be promted for an email address.

.. code-block:: console

    $ BCDATA_EMAIL=pilot@scenicflights.ca
    $ bcdata bc-airports landings.gdb

Several options are available, see documentation for a full list.

.. code-block:: console

    $ bcdata --driver "Shapefile" --crs EPSG:4326 bc-climate-stations climate_stns
    $ bcdata --crs EPSG:26910 --bounds <> bc-airports airports_z10

Download times will vary based on server load and size of dataset. Expect about
a minute for the smallest requests.

Development and testing
-------------------------
Tests require `Fiona <https://github.com/Toblerity/Fiona>`__.


Credits
-------------------------
- `Selenium <http://www.seleniumhq.org>`__
- `pyskel <https://github.com/mapbox/pyskel>`__