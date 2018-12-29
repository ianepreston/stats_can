'''Read StatsCan Data into python, mostly pandas dataframes

TODO
----
Logging

French support
'''
__version__ = '1.2.0'

from stats_can.scwds import get_changed_series_list
from stats_can.scwds import get_changed_cube_list
from stats_can.scwds import get_cube_metadata
from stats_can.scwds import get_series_info_from_vector
from stats_can.sc import get_tables_for_vectors
from stats_can.sc import table_subsets_from_vectors
from stats_can.sc import vectors_to_df
from stats_can.sc import download_tables
from stats_can.sc import zip_update_tables
from stats_can.sc import zip_table_to_dataframe
from stats_can.sc import get_classic_vector_format_df
from stats_can.sc import tables_to_h5
from stats_can.sc import table_from_h5
from stats_can.sc import metadata_from_h5
from stats_can.sc import h5_update_tables
from stats_can.sc import h5_included_keys
