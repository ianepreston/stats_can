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

Refer to the module index below for a list of the modules and the methods they contain.

* :ref:`modindex`

Generally if you want to call any of the endpoints of the service you can find a corresponding function
in the scwds module, and higher level functions to read tables or vectors into a DataFrame are in the sc module

Contributing
============

Contributions to this project are welcome. Fork the repository from
`github <https://github.com/ianepreston/stats_can>`_,

You'll need a python environment with poetry installed. A good guide for setting
up an environment and project (that I used for this library) is `hypermodern python <https://cjolowicz.github.io/posts/hypermodern-python-01-setup/>`_.

I've configured the project to use nix for environment creation. If you use nix then the makefile in the root of the project will let you create
development environments and run tests. However you like to configure a poetry project should work though.

I'd also welcome contributions to the docs, or anything else that would make this tool better for you or others.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
