"""Helper functions that shouldn't need to be directly called by an end user"""
import re


def check_status(results):
    """Make sure list of results succeeded

    Parameters
    ----------
    results : list of dicts, or dict
        JSON from an API call parsed as a dictionary
    """
    results.raise_for_status()
    results = results.json()

    def check_one_status(result):
        """Do the check on an individual result"""
        if result["status"] != "SUCCESS":
            raise RuntimeError(str(result["object"]))

    if isinstance(results, list):
        for result in results:
            check_one_status(result)
    else:
        check_one_status(results)
    return results


def parse_tables(tables):
    """ Basic cleanup of table or tables to numeric


    Strip out hyphens or other non-numeric characters from a list of tables
    or a single table
    Table names in StatsCan often have a trailing -01 which isn't necessary
    So also take just the first 8 characters.
    This function by no means guarantees you have a clean list of valid tables,
    but it's a good start.

    Parameters
    ----------
    tables : list of str or str
        A string or list of strings of table names to be parsed

    Returns
    -------
    list of str
        tables with unnecessary characters removed
    """

    def parse_table(table):
        """Clean up one table string"""
        return re.sub(r"\D", "", table)[:8]

    if isinstance(tables, str):
        return [parse_table(tables)]
    return [parse_table(t) for t in tables]


def parse_vectors(vectors):
    """ Basic cleanup of vector or vectors

    Strip out V from V#s. Similar to parse tables, this by no means guarantees
    a valid entry, just helps with some standard input formats

    Parameters
    ----------
    vectors : list of str or str
        A string or list of strings of vector names to be parsed

    Returns
    -------
    list of str
        vectors with unnecessary characters removed
    """

    def parse_vector(vector):
        """Strip string to numeric elements only"""
        if isinstance(vector, int):  # Already parsed earlier
            return vector
        return int(re.sub(r"\D", "", vector))

    if isinstance(vectors, str):
        return [parse_vector(vectors)]
    return [parse_vector(v) for v in vectors]


def chunk_vectors(vectors):
    """api calls max out at 300 vectors so break list into chunks

    Parameters
    ----------
    vectors : list of str or str
        A string or list of strings of vector names to be parsed

    Returns
    -------
    list of lists of str
        lists of vectors in chunks
    """
    MAX_CHUNK = 250
    vectors = parse_vectors(vectors)
    return [vectors[i : i + MAX_CHUNK] for i in range(0, len(vectors), MAX_CHUNK)]
