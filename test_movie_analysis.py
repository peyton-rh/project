from analyze_movie import movie_analysis
import pytest


class TestMovieAnalysis:

    def test_sql_actor_heading(self):
        expected_heading = 'actor_1_name'
        actual_heading = movie_analysis.analyze_profitability("sqlite:///movies.db",
                                                              'movie',
                                                              actors=True,
                                                              dataframe=False)[0][0]
        assert expected_heading == actual_heading

    def test_sql_genre_heading(self):
        expected_heading = 'genres'
        actual_heading = movie_analysis.analyze_profitability("sqlite:///movies.db",
                                                              'movie',
                                                              dataframe=False)[0][0]
        assert expected_heading == actual_heading

    def test_sql_actor_results(self):
        expected_result = ('CCH Pounder', 237000000.0, 760505847.0, 523505847.0)
        actual_result = movie_analysis.analyze_profitability("sqlite:///movies.db",
                                                             'movie',
                                                             actors=True,
                                                             dataframe=False)[1][0]
        assert expected_result == actual_result

    def test_sql_genre_results(self):
        expected_result = ('Action|Adventure|Fantasy|Sci-Fi', 237000000.0, 760505847.0, 523505847.0)
        actual_result = movie_analysis.analyze_profitability("sqlite:///movies.db",
                                                             'movie',
                                                             dataframe=False)[1][0]
        assert expected_result == actual_result

    def test_pandas_actor_results(self):
        expected_name = 'Wayne Knight'
        expected_profit = 293784000.0
        df = movie_analysis.analyze_profitability("sqlite:///movies.db",
                                                             'movie',
                                                             actors=True,
                                                             dataframe=True,
                                                             fit_in_memory=True)
        actual_name = df.index[0]
        actual_profit = df.iloc[0,2]
        assert expected_name == actual_name
        assert expected_profit == actual_profit

    def test_pandas_genre_results(self):
        expected_genre = 'Family|Sci-Fi'
        expected_profit = 424449459.0
        df = movie_analysis.analyze_profitability("sqlite:///movies.db",
                                                             'movie',
                                                             dataframe=True,
                                                             fit_in_memory=True)
        actual_genre = df.index[0]
        actual_profit = df.iloc[0,2]
        assert expected_genre == actual_genre
        assert expected_profit == actual_profit

    def test_dask_actor_results(self):
        expected_name = 'Wayne Knight'
        expected_profit = 293784000.0
        df = movie_analysis.analyze_profitability("sqlite:///movies.db",
                                                             'movie',
                                                             actors=True,
                                                             dataframe=True,
                                                             fit_in_memory=False)
        actual_name = df.index[0]
        actual_profit = df.iloc[0,2]
        assert expected_name == actual_name
        assert expected_profit == actual_profit

    def test_dask_genre_results(self):
        expected_genre = 'Family|Sci-Fi'
        expected_profit = 424449459.0
        df = movie_analysis.analyze_profitability("sqlite:///movies.db",
                                                  'movie',
                                                  dataframe=True,
                                                  fit_in_memory=False)
        actual_genre = df.index[0]
        actual_profit = df.iloc[0,2]
        assert expected_genre == actual_genre
        assert expected_profit == actual_profit

    def test_fit_in_memory_logic(self):
        with pytest.raises(KeyError):
            df = movie_analysis.analyze_profitability("sqlite:///movies.db",
                                                      'movie',
                                                      dataframe=True,
                                                      fit_in_memory=False,
                                                      index_col='idx')
