from . import operations
import pandas as pd
from typing import List


def analyze_profitability(database: str,
                          table: str,
                          actors: bool = False,
                          dataframe: bool = False,
                          fit_in_memory: bool = False,
                          index_col: str = 'index') -> pd.DataFrame or List:
    """
    Returns either list or pandas DataFrame of top 10 genres or actors by profit, descending.
    If returning a DataFrame larger than 1/10 of available memory, set fit_in_memory to False.
    """
    if not dataframe:
        return operations.sql_query(table=table, database=database, actors=actors)

    elif not fit_in_memory:
        return operations.dask_query(table=table, database=database, actors=actors, index_col=index_col)

    else:
        return operations.pandas_query(table=table, database=database, actors=actors)
