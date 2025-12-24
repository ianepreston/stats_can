"""Read StatsCan Data into python, mostly pandas dataframes.

Todo
----
Logging

French support
"""

try:
    from importlib.metadata import PackageNotFoundError  # type: ignore
    from importlib.metadata import version
except ImportError:  # pragma: no cover
    from importlib_metadata import PackageNotFoundError  # type: ignore
    from importlib_metadata import version


try:
    __version__ = version(__name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
__all__ = [
    "sc",
    "schemas",
    "code_sets_to_df_dict",
    "zip_table_to_dataframe",
    "vectors_to_df",
    "scwds",
    "get_changed_cube_list",
    "get_changed_series_list",
    "get_cube_metadata",
    "get_series_info_from_vector",
]
from stats_can import sc, scwds, schemas
from stats_can.sc import code_sets_to_df_dict, vectors_to_df, zip_table_to_dataframe
from stats_can.scwds import (
    get_changed_cube_list,
    get_changed_series_list,
    get_cube_metadata,
    get_series_info_from_vector,
)
