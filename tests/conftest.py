import sys, os
import pytest

# Make sure that the application source directory (this directory's parent) is
# on sys.path.
# Also change to this directory to load test files

parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent)

here = os.path.dirname(os.path.abspath(__file__))
os.chdir(here)
