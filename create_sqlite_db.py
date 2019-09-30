from sqlalchemy import create_engine
import pandas as pd


def generate_database_from_csv(csv: str='movie_metadata.csv', db_name: str='movies', table_name: str='movie') -> None:

    def generate_db_name(db_name: str) -> str:
        if db_name[-3:] == '.db':
            return f"sqlite:///{db_name}"

        else:
            return f"sqlite:///{db_name}.db"

    engine = create_engine(generate_db_name(db_name))
    df = pd.read_csv(csv)
    df.to_sql(table_name, engine, if_exists='replace')
    engine.dispose()


if __name__ == "__main__":

    generate_database_from_csv()
