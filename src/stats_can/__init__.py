"""Read StatsCan Data into python, mostly pandas dataframes.

Todo
----
Logging

French support
"""

try:
    from importlib.metadata import version, PackageNotFoundError  # type: ignore
except ImportError:  # pragma: no cover
    from importlib_metadata import version, PackageNotFoundError  # type: ignore


try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
__all__ = [
    "sc",
    "StatsCan",
    "code_sets_to_df_dict",
    "delete_tables",
    "list_downloaded_tables",
    "table_to_df",
    "update_tables",
    "vectors_to_df",
    "vectors_to_df_local",
    "scwds",
    "get_changed_cube_list",
    "get_changed_series_list",
    "get_cube_metadata",
    "get_series_info_from_vector",
]
from stats_can import sc
from stats_can.api_class import StatsCan
from stats_can.sc import code_sets_to_df_dict
from stats_can.sc import delete_tables
from stats_can.sc import list_downloaded_tables
from stats_can.sc import table_to_df
from stats_can.sc import update_tables
from stats_can.sc import vectors_to_df
from stats_can.sc import vectors_to_df_local
from stats_can import scwds
from stats_can.scwds import get_changed_cube_list
from stats_can.scwds import get_changed_series_list
from stats_can.scwds import get_cube_metadata
from stats_can.scwds import get_series_info_from_vector
