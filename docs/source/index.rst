A python library for reading data from Statistics Canada
========================================================

This library implements most of the functions defined by the Statistics Canada
`Web Data Service <https://www.statcan.gc.ca/eng/developers/wds>`_.
It also has a number of helper functions that make it easy to read Statistics
Canada tables or vectors into pandas dataframes.

Installation
============
The easiest way to install the package is with conda:
::
    conda install -c ian.e.preston stats_can

The code is also available on
`github <https://github.com/ianepreston/stats_can>`_,
I haven't got it on pypi or anything yet.


Key functions
=============

To load a table into a pandas dataframe, use table_to_df. Given a valid
Statistics Canada table name (it will do some basic parsing to handle dashes)
the function will return a dataframe, either from an already downloaded file on
disk, or after downloading the necessary table. By default it will load an hdf5
file and look for tables there. You can set the h5file parameter to None to load
directly from zipped csv files, but the h5 version is significantly faster.

.. autofunction:: stats_can.sc.table_to_df


There are two ways to load a list of vector numbers into a datetime indexed
dataframe, vectors_to_df and vectors_to_df_local. The former uses the web data
service to load each data point directly from Statistics Canada, while the
latter loads them from locally saved tables (again, downloading any required
tables that haven't already been downloaded). They both return basically the
same data but there are likely performance differences, depending on how many
tables your vectors are sourced from, your internet connection speed, etc.

.. autofunction:: stats_can.sc.vectors_to_df

.. autofunction:: stats_can.sc.vectors_to_df_local

To update locally stored tables with the latest data, use update_tables.
Again, there is an option to update zipped csv files, but hdf5 is the default
This will be slower to run initially as it converts the tables into dataframes
before storing them in hdf5, but subsequent loading of the tables is faster.

.. autofunction:: stats_can.sc.update_tables


For managing already downloaded tables there is list_downloaded_tables and
delete_tables. Neither are particularly necessary if you're only working with
zipped CSV files, since it's pretty easy to just look in a folder and see 
what's there and delete it if necessary, but it makes managing an hdf5 file
easier, which is the preferred way of storing tables.

.. autofunction:: stats_can.sc.list_downloaded_tables

.. autofunction:: stats_can.sc.delete_tables


Indices and tables
==================

* :ref:`modindex`
* :ref:`search`
