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
