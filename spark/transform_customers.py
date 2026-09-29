from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    TimestampType
)

spark = (
    SparkSession.builder
    .appName("ShopFlow Customer Transformation")
    .master("local[*]")
    .getOrCreate()
)

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("signup_date", DateType(), True),
    StructField("updated_at", TimestampType(), True)
])

input_path = "data/raw/customers.csv"

df = (
    spark.read
    .option("header", True)
    .schema(customer_schema)
    .csv(input_path)
)

df = (
    df
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("city", trim(col("city")))
    .withColumn("state", trim(col("state")))
    .withColumn("country", trim(col("country")))
)

df = df.dropDuplicates(["customer_id"])

df.printSchema()

df.show(10, truncate=False)

print("Customer rows:", df.count())

output_path = "data/staging/customers"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Customer staging transformation completed.")

spark.stop()