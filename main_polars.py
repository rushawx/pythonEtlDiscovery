import os

import polars as pl

import my_utils


@my_utils.log_time
def read_data_from_parquet(tc: my_utils.TestCase=None) -> my_utils.TestCase:
    """polars"""
    tc.df = pl.read_parquet(tc.file_name)
    return tc


@my_utils.log_time
def load_data_to_postgres(tc: my_utils.TestCase=None) -> my_utils.TestCase:
    """polars"""
    tc.df.write_database(tc.table, tc.engine, if_table_exists="append")
    return tc


@my_utils.log_time
def get_data_from_postgres(tc: my_utils.TestCase=None) -> my_utils.TestCase:
    """polars"""
    tc.df = pl.read_database(f"select * from {tc.table}", tc.engine)
    return tc


if __name__ == "__main__":

    if not os.path.exists(my_utils.FILE_NAME):
        my_utils.get_data_from_web(my_utils.SOURCE_URL, my_utils.FILE_NAME)
    data = read_data_from_parquet(my_utils.FILE_NAME)

    pg_engine = my_utils.init_pg_engine(my_utils.PG_DSN)
    my_utils.create_postgres_table(pg_engine, my_utils.DDL_FILE_NAME)

    load_data_to_postgres(pg_engine, data, my_utils.PG_TABLE)
    get_data_from_postgres(pg_engine, my_utils.PG_TABLE)
