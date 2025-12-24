"""Helper functions that shouldn't need to be directly called by an end user."""

import re


def _parse_table(table: str | int) -> str:
    """Clean up one table string.

    Parameters
    ----------
    table: str or int
        A single table, possibly with hyphens or other formatting

    Returns
    -------
    str
        A single table stripped of all formatting
    """
    parsed_table: str = re.sub(r"\D", "", str(table))[:8]
    return parsed_table


def parse_tables(tables: str | list[str]) -> list[str]:
    """Parse string of table or tables to numeric.

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
    if isinstance(tables, str):
        return [_parse_table(tables)]
    elif isinstance(tables, int):
        return [str(tables)]
    else:
        return [_parse_table(t) for t in tables]


def _parse_vector(vector: int | str) -> int:
    """Strip string to numeric elements only.

    Parameters
    ----------
    vector: str or int
        vector to be formatted

    Returns
    -------
    vector: int
        the parsed vector
    """
    if not isinstance(vector, int):  # Already parsed earlier
        vector = int(re.sub(r"\D", "", vector))
    return vector


def parse_vectors(vectors: list[str] | str) -> list[int]:
    """Parse string of vector or vectors to numeric.

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
    if isinstance(vectors, str):
        return [_parse_vector(vectors)]
    return [_parse_vector(v) for v in vectors]


def chunk_vectors(vectors: list[str] | str) -> list[list[int]]:
    """Break vectors into chunks small enough for the API (300 limit).

    Parameters
    ----------
    vectors : list of str or str
        A string or list of strings of vector names to be parsed

    Returns
    -------
    chunks: list of lists of str
        lists of vectors in chunks
    """
    MAX_CHUNK = 250
    parsed_vectors = parse_vectors(vectors)
    chunks = [
        parsed_vectors[i : i + MAX_CHUNK]
        for i in range(0, len(parsed_vectors), MAX_CHUNK)
    ]
    return chunks
