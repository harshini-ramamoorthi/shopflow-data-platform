from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    sequence,
    to_date,
    year,
    month,
    dayofmonth,
    quarter,
    dayofweek,
    date_format
)
from pyspark.sql.types import IntegerType


spark = (
    SparkSession.builder
    .appName("ShopFlow Date Dimension")
    .master("local[*]")
    .getOrCreate()
)


start_date = "2024-01-01"
end_date = "2026-12-31"


df = spark.sql(
    f"""
    SELECT explode(
        sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 day
        )
    ) AS full_date
    """
)


df = df.withColumn(
    "date_key",
    date_format(col("full_date"), "yyyyMMdd").cast(IntegerType())
)

df = df.withColumn(
    "day",
    dayofmonth(col("full_date"))
)

df = df.withColumn(
    "month",
    month(col("full_date"))
)

df = df.withColumn(
    "month_name",
    date_format(col("full_date"), "MMMM")
)

df = df.withColumn(
    "quarter",
    quarter(col("full_date"))
)

df = df.withColumn(
    "year",
    year(col("full_date"))
)

df = df.withColumn(
    "day_of_week",
    dayofweek(col("full_date"))
)

df = df.withColumn(
    "day_name",
    date_format(col("full_date"), "EEEE")
)


df = df.select(
    "date_key",
    "full_date",
    "day",
    "month",
    "month_name",
    "quarter",
    "year",
    "day_of_week",
    "day_name"
)


df.printSchema()

df.show(10, truncate=False)

print("Date dimension rows:", df.count())


output_path = "data/warehouse/dim_date"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)


print("Date dimension created successfully.")


spark.stop()