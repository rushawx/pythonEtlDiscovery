import os
import my_utils
import main_pandas as mpd
import main_polars as mpl


def pd_main():
    if not os.path.exists(my_utils.FILE_NAME):
        my_utils.get_data_from_web(my_utils.SOURCE_URL, my_utils.FILE_NAME)
    pg_engine = my_utils.init_pg_engine(my_utils.PG_DSN)
    my_utils.create_postgres_table(pg_engine, my_utils.DDL_FILE_NAME)

    pd_flow = [mpd.read_data_from_parquet, mpd.load_data_to_postgres, mpd.get_data_from_postgres]

    pd_tc = my_utils.TestCase(
        df=None,
        file_name=my_utils.FILE_NAME,
        engine=pg_engine,
        table=my_utils.PG_TABLE
    )

    for func in pd_flow:
        func(pd_tc)


def pl_main():
    if not os.path.exists(my_utils.FILE_NAME):
        my_utils.get_data_from_web(my_utils.SOURCE_URL, my_utils.FILE_NAME)
    pg_engine = my_utils.init_pg_engine(my_utils.PG_DSN)
    my_utils.create_postgres_table(pg_engine, my_utils.DDL_FILE_NAME)

    pl_flow = [mpl.read_data_from_parquet, mpl.load_data_to_postgres, mpl.get_data_from_postgres]

    pl_tc = my_utils.TestCase(
        df=None,
        file_name=my_utils.FILE_NAME,
        engine=pg_engine,
        table=my_utils.PG_TABLE
    )

    for func in pl_flow:
        func(pl_tc)


if __name__ == "__main__":
    n = 1
    for i in range(1, n+1):
        print(f"_____{i}/{n}_____", end="\n")
        pd_main()
        pl_main()
