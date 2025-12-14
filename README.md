# Python api for Statistics Canada New Data Model (NDM)

[![Tests](https://github.com/ianepreston/stats_can/workflows/Tests/badge.svg)](https://github.com/ianepreston/stats_can/actions?workflow=Tests)
[![Documentation Status](https://readthedocs.org/projects/stats-can/badge/?version=latest)](https://stats-can.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/stats-can.svg)](https://badge.fury.io/py/stats-can)
[![Anaconda-Server Badge](https://anaconda.org/conda-forge/stats_can/badges/version.svg)](https://anaconda.org/conda-forge/stats_can)

API documentation for StatsCan can be found on the [web data service docs](https://www.statcan.gc.ca/eng/developers/wds)

If you're looking for Table/Vector IDs to use in the app you can find them through [the StatCan data page](https://www150.statcan.gc.ca/n1/en/type/data)

[Anaconda package](https://anaconda.org/conda-forge/stats_can)

[Read the docs](https://stats-can.readthedocs.io/en/latest/)

# Introduction

This library implements most of the functions defined by the Statistics Canada
[Web Data Service](https://www.statcan.gc.ca/eng/developers/wds).
It also has a number of helper functions that make it easy to read Statistics
Canada tables or vectors into pandas dataframes.

# Installation

The package can either be installed with pip or conda:

```bash
conda install -c conda-forge stats_can
```

Or:

```bash
pip install stats-can
```

The code is also available on

[github](https://github.com/ianepreston/stats_can).

# Contributing

Contributions to this project are welcome. Fork the repository from
[github](https://github.com/ianepreston/stats_can).

You'll need a python environment with poetry installed. A good guide for setting
up an environment and project (that I used for this library) is [hypermodern python](https://cjolowicz.github.io/posts/hypermodern-python-01-setup/).

I've configured the project to use nix for environment creation. If you use nix then the makefile in the root of the project will let you create
development environments and run tests. However you like to configure a uv project should work though.

I'd also welcome contributions to the docs, or anything else that would make this tool better for you or others.
