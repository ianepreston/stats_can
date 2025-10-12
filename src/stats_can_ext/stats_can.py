"""

This interface provides a public API and some utility functions to interact with [Statistics Canada](https://www150.statcan.gc.ca/n1/en/type/data)

API features:
* a searchable index of table names, with look up by keyword
* a mapping from semantic table name to table ID
* table name to description map
* table look up by description search

"""

import pandas as pd
import requests
import json
from typing import Literal, Optional

class StatsCan:
    def __init__(self):
        self.table_metadata = pd.DataFrame(self._get_all_tables())
        self._normalize()
        
    def _normalize(self):
        self.table_metadata["title_normed"] = self.table_metadata["cubeTitleEn"].str.lower()
        self.table_metadata["title"] = self.table_metadata["cubeTitleEn"]
        
    def _get_all_tables(self):
        url = "https://www150.statcan.gc.ca/t1/wds/rest/getAllCubesList"
        response = requests.get(url)
        return json.loads(response.content)

    def search(self, keywords: list[str], normalize : Optional[bool] = True) -> pd.DataFrame:
        if normalize:
            keywords = [keyword.lower() for keyword in keywords] 
            return self.table_metadata[
                self.table_metadata["title_normed"].str.contains("|".join(keywords))
            ][["title", "cansimId"]]
        else:
            return self.table_metadata[
                self.table_metadata["title"].str.contains("|".join(keywords))
            ][["title", "cansimId"]]
        
