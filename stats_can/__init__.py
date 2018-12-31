'''Read StatsCan Data into python, mostly pandas dataframes

TODO
----
Logging

French support
'''
__version__ = '1.4.0'
import warnings
from stats_can import sc
from stats_can.scwds import get_changed_series_list
from stats_can.scwds import get_changed_cube_list
from stats_can.scwds import get_cube_metadata
from stats_can.scwds import get_series_info_from_vector
from stats_can.sc import table_to_df
from stats_can.sc import update_tables
from stats_can.sc import list_downloaded_tables
from stats_can.sc import delete_tables
from stats_can.sc import vectors_to_df
from stats_can.sc import vectors_to_df_local

def get_tables_for_vectors(vectors):
    msg = (
        'This will be removed from the top level in v2.0, import it from the'
        ' sc module'
    )
    warnings.warn(msg)
    return sc.get_tables_for_vectors(vectors)


def table_subsets_from_vectors(vectors):
    msg = (
        'This will be removed from the top level in v2.0, import it from the'
        ' sc module'
    )
    warnings.warn(msg)
    return sc.table_subsets_from_vectors(vectors)


def download_tables(tables, path=None, csv=True):
    msg = (
        "Shouldn't need to call this directly now so in 2.0 it will be removed "
        "from the top level, you can still import it from the sc module. "
        "Calling table_to_df will automatically download the table if necessary"
    )
    warnings.warn(msg)
    return sc.download_tables(tables=tables, path=path, csv=csv)


def zip_update_tables(path=None, csv=True):
    msg = (
        "This will be removed from the top level in 2.0. Call update_tables "
        "instead (handles zips and h5 updates) or import it from the sc module"
    )    
    warnings.warn(msg)
    return sc.zip_update_tables(path=path, csv=True)


def zip_table_to_dataframe(table, path=None):
    msg = (
        "This will be removed from the top level in 2.0. Call table_to_df "
        "instead (handles zips and h5 updates) or import it from the sc module"
    )
    warnings.warn(msg)
    return sc.zip_table_to_dataframe(table=table, path=path)


def get_classic_vector_format_df(vectors, path=None, start_date=None, h5file='stats_can.h5'):
    msg = (
        "This will be renamed vectors_to_df_local in 2.0"
    )
    warnings.warn(msg)
    return sc.vectors_to_df_local(
        vectors=vectors, path=path, start_date=start_date, h5file=h5file
        )


def tables_to_h5(tables, h5file='stats_can.h5', path=None):
    msg = (
        "Shouldn't need to call this directly now so in 2.0 it will be removed "
        "from the top level, you can still import it from the sc module. "
        "Calling table_to_df will automatically download the table if necessary"
    )
    warnings.warn(msg)
    return sc.tables_to_h5(tables=tables, path=path, h5file=h5file)


def table_from_h5(table, h5file='stats_can.h5', path=None):
    msg = (
        "This will be removed from the top level in 2.0. Call table_to_df "
        "instead (handles zips and h5 updates) or import it from the sc module"
    )
    warnings.warn(msg)
    return sc.table_from_h5(table=table, h5file=h5file, path=path)


def metadata_from_h5(tables, h5file='stats_can.h5', path=None):
    msg = (
        "This will be removed from the top level in 2.0. Call "
        "list_downloaded_tables instead (handles zips and h5 updates) or "
        "import it from the sc module"
    )
    warnings.warn(msg)
    return sc.metadata_from_h5(tables=tables, h5file=h5file, path=path)


def h5_update_tables(h5file='stats_can.h5', path=None, tables=None):
    msg = (
        "This will be removed from the top level in 2.0. Call update_tables "
        "instead (handles zips and h5 updates) or import it from the sc module"
    )    
    warnings.warn(msg)
    return sc.h5_update_tables(h5file=h5file, path=path, tables=tables)


def h5_included_keys(h5file='stats_can.h5', path=None):
    msg = (
        "This will be removed from the top level in 2.0. Call "
        "list_downloaded_tables instead (handles zips and h5 updates) or "
        "import it from the sc module"
    )
    warnings.warn(msg)
    return sc.h5_included_keys(h5file=h5file, path=path)
