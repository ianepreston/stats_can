from os import path
from setuptools import setup, find_packages
from version import find_version

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "requirements.txt")) as f:
    requirements = f.read().splitlines()

setup(
  name='stats_can',
  author='Ian Preston',
  description='Access StatsCan data with python',
  license='GPL',
  version=find_version('stats_can', '__init__.py'),
  packages=find_packages(),
  install_requires=requirements
)
