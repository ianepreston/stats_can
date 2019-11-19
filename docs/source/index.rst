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

Contributions are welcome. I'll outline the method I use to set up a dev environment below,
but a great enhancement for this project would be to extend that to other possible approaches.
Using conda, a dev environment can bet set up as follows:
::
        git clone <your fork of stats_can>
        cd stats_can
        conda env create -f sc_dev.yml
        conda activate sc_dev
        python setup.py develop

From there you should be able to make any changes to the source code, run tests and build
docs. I use travis CI for building and testing. I'm actually not sure if it will work for
people who aren't me the way I've got it set up, since the last step is to push a passing
build to Anaconda cloud.

This is pretty meta but I would love some contributions around making this project easier
to contribute to. If you have ideas for that please feel free to create an issue or submit a PR.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
