"""Lab: Bronze to silver with Spark DataFrames (module 07).

Builds the same silver layer as the dbt lab, with the DataFrame API and Spark SQL,
then writes it to S3 as Parquet and as Iceberg tables.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

# Spark 4.1.1 / Scala 2.13 in this image -> iceberg-spark-runtime-4.1_2.13
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("session:", spark)

# ----------------------------------------------------------------------
# Bronze layer
# ----------------------------------------------------------------------

users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv",
    header=True, inferSchema=True, samplingRatio=0.01, multiLine=True,
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv",
    header=True, inferSchema=True, samplingRatio=0.01, multiLine=True,
)

print("== bronze users schema ==")
users_bronze.printSchema()
users_bronze.show(3, truncate=False)

print("== address column ==")
users_bronze.select("address").show(3, truncate=False)

# ----------------------------------------------------------------------
# Silver: users, DataFrame API
# ----------------------------------------------------------------------

users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)

# regexp_extract() returns "" and not NULL on no match, unlike SQL's nullif().
# Left as "", city would count as a group in a later groupBy() instead of being excluded.
stg_users = stg_users.withColumn(
    "city", F.when(F.col("city") == "", None).otherwise(F.col("city"))
)

print("== stg_users ==")
stg_users.show(5, truncate=False)

# ----------------------------------------------------------------------
# Silver: orders, DataFrame API with window deduplication
# ----------------------------------------------------------------------

orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)

print("Original:", orders_typed.count())

# Intentional duplication scenario: ingestion replays do produce duplicates.
duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)

print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()

# partitionBy() is a wide transformation: rows are shuffled so that every row of a
# given order_id lands on the same partition before row_number() can be computed.
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())

stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

# ----------------------------------------------------------------------
# The same transformation with Spark SQL
# ----------------------------------------------------------------------

orders_typed.createOrReplaceTempView("orders_typed")

stg_orders_sql = spark.sql('''
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
''')

stg_orders_sql.show(5)

assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("DataFrame API and Spark SQL agree")

stg_users.createOrReplaceTempView("stg_users")
print("== users per state ==")
spark.sql('''
    SELECT state,
           count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users
    GROUP BY state
    ORDER BY users DESC
''').show(10)

# ----------------------------------------------------------------------
# Write the silver layer as Parquet
# ----------------------------------------------------------------------

stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("parquet written")

users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("parquet round-trip ok")

# ----------------------------------------------------------------------
# Write the silver layer as Iceberg tables
# ----------------------------------------------------------------------

spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")

stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
print("iceberg tables written")

print("== snapshots ==")
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)

print("== files ==")
spark.sql(
    "SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files"
).show(truncate=False)

users_iceberg = spark.table("lab.silver.users")
orders_iceberg = spark.table("lab.silver.orders")
assert users_iceberg.count() == users_bronze.count()
assert orders_iceberg.count() == stg_orders.count()
print("iceberg round-trip ok")

spark.stop()
print("DONE")
