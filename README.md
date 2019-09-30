### Approach

The idea was to create a single flexible function to provide answers to our queries. 

There are 3 different possibilites depending on the size of the data. 

1. In memory processing with pandas
2. Larger than memory processing with Dask
3. Processing with SQL 

Because this is done with sqlite there isn't much practical difference in this exact implementation. 

### Process

1. Undertand the data using pandas. If this were a larger dataset, use a subsample of data or use Dask
2. Create script to create database
3. Create database
4. Test pandas and sql code against database
5. Turn notebook code into module
6. Clean up in pycharm
7. Write tests
8. Clean up problems revealed by tests

### Tests

Each unit test is designed to test the expected outputs from modifying an input for the `analyze_profitability` function. 

There is also one test for the logic in analyze_profitability between the pandas and dask functions. Both the pandas and dask functions return pandas dataframes. The pandas function will only break on a larger than memory dataset, which was impractical to set up a test for. In order to address this the test modifies the `index_col` paramter, which will be ignored by a pandas dataframe but cause a `KeyError` for Dask. 

### Libraries

There is a requirements.txt folder. The core of the functionality is built around numpy, Dask, pandas, and SQLAlchemy

### Instructions

You can use either the provided database or create a new database from the provided csv file by using `create_sqlite_db.py` (it will create a sqlite database called `movies.db` with the default arguments). Once you have the database, you can run `analyze_movie.profitability`. By default it will return a list of the top 10 movie genres by average profit, the first element being the column names and the second element containing the data. 

Change the `actors` argument to `True` to return the top 10 actors by average profit.

Change the `dataframe` argument to `True` to return a pandas dataframe generated by Dask (slower for in-memory). The default `index_col` is already set.

Change the `dataframe` argument to `True` and the `fit_in_memory` argument to `True` to return a pandas dataframe generated by pandas.
