from sqlalchemy import create_engine
from typing import List
import dask.dataframe as ddf
import pandas as pd


def sql_query(table: str, database: str, actors: bool = False) -> List:
    """
    Generates and returns list of top 10 genres or actors by profit, descending
    by performing a SQL query on a sqlite database.
    """
    engine = create_engine(database)
    conn = engine.connect()

    group_by = 'genres'

    if actors:
        group_by = 'actor_1_name'

    query = (f"SELECT {group_by}, budget, gross, (gross - budget) as profit "
             f"FROM {table} "
             f"GROUP BY {group_by} "
             "ORDER BY profit DESC "
             "LIMIT 10")

    result = conn.execute(query)

    results_list = [(group_by, 'budget', 'gross', 'profit'), [row for row in result]]

    conn.close()

    return results_list


def dask_query(table: str, database: str, actors: bool = False, index_col: str = 'index',
               n_partitions: int = 12) -> pd.DataFrame:
    """
    Generates Dask DataFrame from a sqlite database, and returns pandas DataFrame of top 10 genres
    by or actors, profit, descending. Can handle larger than memory data sets, but slower
    than pandas for in-memory group by operations.
    """
    group_by = 'genres'

    if actors:
        group_by = 'actor_1_name'

    df = ddf.read_sql_table(table, database, index_col=index_col, npartitions=n_partitions)

    df = df[[group_by, 'budget', 'gross']]

    df['profit'] = df['gross'] - df['budget']

    group_by = df.groupby(group_by).mean().compute()

    return group_by.sort_values(by='profit', ascending=False)[0:10]


def pandas_query(table: str, database: str, actors: bool = False) -> pd.DataFrame:
    """
    Generates pandas DataFrame from sqlite database, and returns pandas DataFrame of top 10 genres
    or actors, by profit, descending. Can be used for tables < 1/10 of available memory.
    """
    group_by = 'genres'

    if actors:
        group_by = 'actor_1_name'

    engine = create_engine(database)

    query = (f"SELECT {group_by}, budget, gross "
             f"FROM {table} ")

    engine.dispose()

    df = pd.read_sql_query(query, engine)

    df['profit'] = df['gross'] - df['budget']

    return (df.groupby(group_by)
              .mean()
              .sort_values(by='profit', ascending=False)[0:10])

