import os
import time
import requests

import dotenv
import pandas as pd
from sqlalchemy import create_engine, Engine

dotenv.load_dotenv()

SOURCE_URL = os.getenv("SOURCE_URL")
FILE_NAME = os.getenv("FILE_NAME")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_TABLE = os.getenv("PG_TABLE")
PG_DSN = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
DDL_FILE_NAME = os.getenv("DDL_FILE_NAME")


def log_time(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"{func.__name__} took {round(end - start, 3)} seconds")
        return result

    return wrapper


@log_time
def get_data_from_web(url: str, file_path: str) -> None:
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "wb") as file:
            file.write(response.content)
    return None


@log_time
def read_data_from_parquet(file_name: str) -> pd.DataFrame:
    df = pd.read_parquet(file_name)
    return df


@log_time
def init_pg_engine(dsn: str) -> Engine:
    pg_engine = create_engine(dsn).execution_options(autocommit=True)
    return pg_engine


@log_time
def create_postgres_table(engine: Engine, ddl_file_name: str) -> None:
    connection = engine.raw_connection()
    cursor = connection.cursor()
    with open(ddl_file_name, "r") as ddl_file:
        cursor.execute(ddl_file.read())
    connection.commit()
    return None


@log_time
def load_data_to_postgres(engine: Engine, df: pd.DataFrame, table: str) -> None:
    df.to_sql(
        con=engine,
        name=table,
        schema="public",
        if_exists="append",
    )
    return None


@log_time
def get_data_from_postgres(engine: Engine, table: str) -> pd.DataFrame:
    df = pd.read_sql_table(table, engine)
    return df


get_data_from_web(SOURCE_URL, FILE_NAME)
data = read_data_from_parquet(FILE_NAME)

pg_engine = init_pg_engine(PG_DSN)
create_postgres_table(pg_engine, DDL_FILE_NAME)

load_data_to_postgres(pg_engine, data, PG_TABLE)
get_data_from_postgres(pg_engine, PG_TABLE)
