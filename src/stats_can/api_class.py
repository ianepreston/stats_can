"""Define a class wrapper for Stats Can functions."""
from pathlib import Path

from stats_can import sc
from stats_can import scwds


class StatsCan:
    """Load Statistics Canada data and metadata into python.

    Parameters
    ----------
    data_folder: Path/str, default None
    The location to save/search for locally stored Statistics Canada
    data tables. Defaults to the current working directory
    """

    def __init__(self, data_folder=None):
        if data_folder is None:
            self.data_folder = Path.cwd()
        else:
            self.data_folder = Path(data_folder)  # Get a path object even if passed str

    @property
    def downloaded_tables(self):
        """Check which tables you've downloaded.

        Checks the file "stats_can.h5" in the instantiated data folder
        and lists all tables stored there.

        Returns
        -------
        [table_ids]
        """
        stat_h5 = self.data_folder / "stats_can.h5"
        if stat_h5.exists():
            full_meta = sc.list_h5_tables(path=self.data_folder, h5file="stats_can.h5")
            tables = [item["productId"] for item in full_meta]
        else:
            tables = []
        return tables

    def table_to_df(self, table):
        """Read a table to a dataframe.

        Parameters
        ----------
        table: str
            The ID of the table of interest, e.g "271-000-22"

        Returns
        -------
        pandas.DataFrame
            Dataframe of the requested table

        If the table has been previously loaded to the file in self.data_folder
        it will retrieve that locally stored dataframe. If it's unavailable it will
        download it and then return the table. To update a locally stored table,
        call StatsCan.update_tables(), optionally passing just the table number of interest
        """
        return sc.table_to_df(table=table, path=self.data_folder, h5file="stats_can.h5")

    @staticmethod
    def vectors_to_df_remote(
        vectors, periods=1, start_release_date=None, end_release_date=None
    ):
        """Retrieve V# data directly from Statistics Canada.

        Parameters
        ----------
        vectors: str or [str]
            V#(s) to retrieve data for
        periods: int, default 1
            Number of periods to retrieve data.
            Note that this will be ignored if start_release_date and end_release date
            are set
        start_release_date: datetime.date, default None
            earliest release date to retrieve data
        end_release_date: datetime.date, default None
            latest release date to retrieve data

        Returns
        -------
        pandas.DataFrame
            Dataframe indexed on reference (not release) date, with columns for each V#
            input

        Note that start and end release date refer to the dates the data was released,
        not the reference period they cover. For example. October labour force survey
        data is released on the first or second Friday of November.
        """
        return sc.vectors_to_df(vectors, periods, start_release_date, end_release_date)

    def vectors_to_df(self, vectors, start_date=None):
        """Get a dataframe of V#s.

        Parameters
        ----------
        vectors: str or [str]
            the V#s to retrieve
        start_date: datetime.date, optional
            earliest reference period to return, defaults to all available history

        Returns
        -------
        pandas.DataFrame
            Dataframe indexed on reference date, with columns for each V# input

        Note that any V#s in tables that are not currently locally stored will
        have their tables downloaded prior to returning the dataframe
        """
        return sc.vectors_to_df_local(
            vectors=vectors,
            path=self.data_folder,
            start_date=start_date,
            h5file="stats_can.h5",
        )

    def update_tables(self, tables=None):
        """Update locally stored tables.

        Compares latest available reference period in locally stored tables to the
        latest available on Statistics Canada and updates any tables necessary

        Parameters
        ----------
        tables: str or [str], default None
            Optional subset of tables to check for updates, defaults to update all
            downloaded tables

        Returns
        -------
        [str] list of tables that were updated, empty list if no updates made
        """
        return sc.update_tables(
            path=self.data_folder, h5file="stats_can.h5", tables=tables, csv=True
        )

    def delete_tables(self, tables):
        """Remove locally stored tables.

        Parameters
        ----------
        tables: str or [str]
            tables to delete

        Returns
        -------
        [deleted tables]
        """
        return sc.delete_tables(
            tables=tables, path=self.data_folder, h5file="stats_can.h5", csv=True
        )

    @staticmethod
    def get_code_sets():
        """Get code sets.

        Code sets provide additional metadata to describe variables and are grouped into
        scales, frequencies, symbols etc.

        Returns
        -------
        code_sets: [dict]
            one dictionary for each group of information
        """
        return scwds.get_code_sets()

    @staticmethod
    def vectors_updated_today():
        """Get a list of all V#s that were updated today.

        Returns
        -------
        changed_series: [dict]
            one dictionary for each vector with its update date
        """
        return scwds.get_changed_series_list()

    @staticmethod
    def tables_updated_today():
        """Get a list of tables that were updated today.

        Returns
        -------
        changed_tables: [dict]
            one dictionary for each table with its update date
        """
        return scwds.get_changed_cube_list()

    @staticmethod
    def tables_updated_on_date(date):
        """Get a list of tables that were updated on a given date.

        Parameters
        ----------
        date: str or datetime.date
            The date to check tables

        Returns
        -------
        changed_tables: [dict]
            one dictionary for each table with its update date
        """
        return scwds.get_changed_cube_list(date)

    @staticmethod
    def vector_metadata(vectors):
        """Get metadata on vectors.

        Parameters
        ----------
        vectors: str or [str]
            V#(s) to retrieve metadata

        Returns
        -------
        vector_metadata: [dict]
            list of dictionaries with one dict for each vector
        """
        return scwds.get_series_info_from_vector(vectors)

    @staticmethod
    def get_tables_for_vectors(vectors):
        """Find which table(s) a V# or list of V#s is from.

        Parameters
        ----------
        vectors: str or [str]
            V#(s) to look up tables for

        Returns
        -------
        dictionary of  vector:table pairs plus an
        "all_tables" key with a list of all tables
        containing the input V#s


        >>> StatsCan.get_tables_for_vectors("v39050")
        {39050: '10100139', 'all_tables': ['10100139']}
        >>> StatsCan..get_tables_for_vectors(["v39050", "v1074250274"])
        {39050: '10100139', 1074250274: '16100011', 'all_tables': ['10100139', '16100011']}
        """
        return sc.get_tables_for_vectors(vectors)
