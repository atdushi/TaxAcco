from pyspark.sql import functions as F
import vertica_python

conn_info = {'host': '127.0.0.1',
             'port': 5433,
             'user': 'dbadmin',
             'password': '',
             'database': ''}


def read_table(table_name):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table_name}")
        rows = cur.fetchall()
        print(rows)


# read_table('Rate')


def get_latest_rates(df_rates):
    return (
        df_rates.select(F.col("Currency"), F.struct(F.col("RateDate"), F.col("Rate")).alias("vs"))
        .groupBy(F.col("Currency"))
        .agg(F.max(F.col("vs")).alias("vs"))
        .select("Currency", "vs.RateDate", "vs.Rate")
    )
