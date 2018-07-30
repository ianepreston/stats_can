'''Read StatsCan Data into python, mostly pandas dataframes'''
__version__ = '0.1'

from .stats_can import get_changed_series_list
from .stats_can import get_changed_cube_list
from .stats_can import get_cube_metadata
from .stats_can import get_series_info_from_vector
from .stats_can import get_bulk_vector_data_by_range
from .stats_can import get_tables_for_vectors
from .stats_can import table_subsets_from_vectors
from .stats_can import vectors_to_df
from .stats_can import download_tables
from .stats_can import update_tables
from .stats_can import table_to_dataframe
from .stats_can import get_classic_vector_format_df