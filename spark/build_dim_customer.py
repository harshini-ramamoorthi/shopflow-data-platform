from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id


spark = (
    SparkSession.builder
    .appName("ShopFlow Customer Dimension")
    .master("local[*]")
    .getOrCreate()
)


input_path = "data/staging/customers"

df = spark.read.parquet(input_path)


df = df.select(
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "state",
    "country",
    "signup_date"
)


df = df.withColumn(
    "customer_key",
    monotonically_increasing_id()
)


df = df.select(
    "customer_key",
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "city",
    "state",
    "country",
    "signup_date"
)


df.printSchema()

df.show(10, truncate=False)

print("Customer dimension rows:", df.count())


output_path = "data/warehouse/dim_customer"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)


print("Customer dimension created successfully.")


spark.stop()
