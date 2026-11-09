"""Lab: Explore Iceberg tables with Spark (module 08).

Snapshots, time travel and schema evolution on lab.silver.orders.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-iceberg")
    .master("local[*]")
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

print("== namespaces ==")
spark.sql("SHOW NAMESPACES IN lab").show()

print("== table totals ==")
spark.sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders").show()

print("== snapshots (before append) ==")
spark.sql("SELECT * FROM lab.silver.orders.snapshots").show(truncate=False)

print("== files ==")
spark.sql(
    "SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files"
).show(truncate=False)

# ----------------------------------------------------------------------
# Create a second snapshot
# ----------------------------------------------------------------------

stg_orders = spark.table("lab.silver.orders")
stg_orders.show(5)

# New order IDs so the table keeps one row per order
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo("lab.silver.orders").append()
print("appended 3 orders")

print("== snapshots (after append) ==")
spark.sql(
    "SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at"
).show(truncate=False)

print("== count after append ==")
spark.sql("SELECT count(*) FROM lab.silver.orders").show()

# ----------------------------------------------------------------------
# Time travel
# ----------------------------------------------------------------------

snapshots = spark.sql(
    "SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at"
).collect()
print("snapshot ids:", [r["snapshot_id"] for r in snapshots])

first_snapshot = snapshots[0]["snapshot_id"]
print(f"== count at first snapshot {first_snapshot} ==")
spark.sql(f"SELECT count(*) FROM lab.silver.orders VERSION AS OF {first_snapshot}").show()

print("== count now ==")
spark.sql("SELECT count(*) FROM lab.silver.orders").show()

# ----------------------------------------------------------------------
# Schema evolution
# ----------------------------------------------------------------------

spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
print("== schema after ADD COLUMN ==")
spark.sql("DESCRIBE lab.silver.orders").show()

spark.sql('''
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders
    LIMIT 1
''')
print("== rows carrying the new column ==")
spark.sql("SELECT source, count(*) FROM lab.silver.orders GROUP BY source").show()

print("== final snapshots ==")
spark.sql(
    "SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at"
).show(truncate=False)

spark.stop()
print("DONE")
