from pyspark.sql import functions as F

def get_latest_rates(df_rates):
    return (
        df_rates.select(F.col("Currency"), F.struct(F.col("RateDate"), F.col("Rate")).alias("vs"))
        .groupBy(F.col("Currency"))
        .agg(F.max(F.col("vs")).alias("vs"))
        .select("Currency", "vs.RateDate", "vs.Rate")
    )