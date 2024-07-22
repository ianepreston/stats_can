A python library for reading data from Statistics Canada
========================================================

This library implements most of the functions defined by the Statistics Canada
`Web Data Service <https://www.statcan.gc.ca/eng/developers/wds>`_.
It also has a number of helper functions that make it easy to read Statistics
Canada tables or vectors into pandas dataframes.

Installation
============
The package can either be installed with pip or conda:
::

    conda install -c conda-forge stats_can

Or:
::

    pip install stats-can

The code is also available on
`github <https://github.com/ianepreston/stats_can>`_,

Upcoming Change Warning
=======================

In the upcoming v3 release of this library I will be removing the StatsCan
object, along with all functionality that relates to working with tables in hdfs.

When I first developed this library I wanted it to be a one stop shop for managing
StatsCan data. As time has gone on I've realized that's expanded the scope of this
library beyond its core function. Reliance on HDFS has led to compatibility issues
for some users, and unnecessarily increased the size of the library.

Pandas has built in functionality to export dataframes to a number of persistent formats,
so tightly coupling this library to HDFS is quite limiting. In the future I'd like
to add (optional) capabilities to read tables into other common DataFrame structures
like polars or spark, and having to make all those interoperate with HDFS would be
limiting and confusing.

If you rely on the HDFS functionality to persist StatsCan data please do not
update to version 3, at least not before refactoring your code to remove that
requirement. I won't release version 3 before 2025 to give people time
to update to this current release and get the warnings I'm putting
in the library for deprecated functions.

If you want to check out the new version in advance you can install from the
v3 branch in the project repo.

Quickstart
==========
After installing the stats_can package, all of the core functionality
is available by instantiating a StatsCan object:

::

    from stats_can import StatsCan
    sc = StatsCan()

Without any arguments the StatsCan object will look for a file in the current
working directory named "stats_can.h5". If it doesn't exist it will create one
when it is first asked to load a table. You can also pass in a path in order
to specify the location of the file. This is useful if you or a team want persistent
access to certain tables.

For example:
::

    sc = StatsCan(data_folder="~/stats_can_data")

The most common use case for stats_can is simply to read in a table
from Statistics Canada to a Pandas DataFrame. For example, table 271-000-22-01
is "Personnel engaged in research and development by performing sector and occupational category"
to read in that table (downloading it first if it's the first time accessing it) run:

::

    df = sc.table_to_df("271-000-22-01")

If there are only a couple specific series of interest you can also
read them into a dataframe (whether they're in different source tables or not) as follows:

::

    df = sc.vectors_to_df(["v74804", "v41692457"])

The above command takes an optional start_date argument which will return
a dataframe beginning with a reference date no earlier than the provided start date.
By default it will return all available history for the V#s provided.

You can check which tables you have stored locally by running

::

    sc.downloaded_tables

Which will return a list of table numbers.

If a table is locally stored, it will not automatically update if
Statistics Canada releases an update. To update locally stored tables
run:

::

    sc.update_tables()

You can optionally pass in a list of tables if you only want a subset of the
locally stored tables to be updated.

Finally, if you want to delete any tables you've loaded you can run:

::

    sc.delete_tables("271-000-22-01")

StatsCan class documentation
============================
Core functions outlined in the Quickstart along with some extra
functionality are described here:

.. autoclass:: stats_can.api_class.StatsCan
    :members:

Contributing
============

Contributions to this project are welcome. Fork the repository from
`github <https://github.com/ianepreston/stats_can>`_,

You'll need a python environment with poetry and nox installed. A good guide for setting
up an environment and project (that I used for this library) is `hypermodern python <https://cjolowicz.github.io/posts/hypermodern-python-01-setup/>`_.

After making any changes you can run nox to make sure testing and linting went ok, and then you should be good to submit a PR.

I'd also welcome contributions to the docs, or anything else that would make this tool better for you or others.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
