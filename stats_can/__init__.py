'''Read StatsCan Data into python, mostly pandas dataframes

TODO
----
More comprehensive tests

Logging

French support
'''
__version__ = '0.2'

from .scwds import get_changed_series_list
from .scwds import get_changed_cube_list
from .scwds import get_cube_metadata
from .scwds import get_series_info_from_vector
from .sc import get_tables_for_vectors
from .sc import table_subsets_from_vectors
from .sc import vectors_to_df
from .sc import download_tables
from .sc import zip_update_tables
from .sc import zip_table_to_dataframe
from .sc import get_classic_vector_format_df
