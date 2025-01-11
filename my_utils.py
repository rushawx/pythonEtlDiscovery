import os
import time
import uuid
import requests
from functools import wraps

import dotenv
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

RUN_ID = uuid.uuid4()


class TestCase:
    def __init__(self, df, file_name: str=None, engine: Engine=None, table: str=None) -> None:
        self.df = df
        self.file_name = file_name
        self.engine = engine
        self.table = table


def log_time(func, file_path="data.csv"):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        record = ",".join([func.__doc__, func.__name__, str(round(end - start, 3))])
        with open(file_path, "a") as file:
            file.write(record + "\n")
        print(record)
        return result
    return wrapper


@log_time
def get_data_from_web(url: str, file_path: str) -> None:
    """requests"""
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "wb") as file:
            file.write(response.content)
    return None


@log_time
def init_pg_engine(dsn: str) -> Engine:
    """sqlalchemy"""
    pg_engine = create_engine(dsn).execution_options(autocommit=True)
    return pg_engine


@log_time
def create_postgres_table(engine: Engine, ddl_file_name: str) -> None:
    """sqlalchemy"""
    connection = engine.raw_connection()
    cursor = connection.cursor()
    with open(ddl_file_name, "r") as ddl_file:
        cursor.execute(ddl_file.read())
    connection.commit()
    return None
