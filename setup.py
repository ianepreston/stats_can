from setuptools import setup, find_packages
from version import find_version

setup(
  name='stats_can',
  author='Ian Preston',
  description='Access StatsCan data with python',
  license='GPL',
  version=find_version('stats_can', '__init__.py'),
  packages=find_packages()
)