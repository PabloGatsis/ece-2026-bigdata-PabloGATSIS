"""Lab: register the Silver Iceberg table in the Polaris catalog (module 09, lab 2).

Connects Spark to Polaris through the Iceberg REST catalog, using the `dev`
principal, and registers the existing Hadoop-catalog table.
"""

import os

from pyspark.sql import SparkSession

BUCKET = os.environ["KUBERNETES_NAMESPACE"]
CATALOG = BUCKET
POLARIS_URI = "http://polaris:8181/api/catalog"
CLIENT_ID = os.environ["DEV_ID"]
CLIENT_SECRET = os.environ["DEV_SECRET"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-polaris-register")
    .master("local[*]")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Polaris, through the Iceberg REST catalog
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", POLARIS_URI)
    .config("spark.sql.catalog.polaris.warehouse", CATALOG)
    .config("spark.sql.catalog.polaris.credential", f"{CLIENT_ID}:{CLIENT_SECRET}")
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.header.Polaris-Realm", "POLARIS")
    .config("spark.sql.catalog.polaris.token-refresh-enabled", "false")
    # The Hadoop catalog holding the table produced by the Spark lab
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    # S3
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print("== namespaces in polaris ==")
spark.sql("SHOW NAMESPACES IN polaris").show()

spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
spark.sql("SHOW NAMESPACES IN polaris").show()

# Location of the table written by the Spark lab, registered as-is in Polaris
metadata_location = (
    spark.sql("SELECT file FROM lab.silver.orders.metadata_log_entries ORDER BY timestamp DESC LIMIT 1")
    .collect()[0]["file"]
)
print("metadata:", metadata_location)

spark.sql(f"CALL polaris.system.register_table(table => 'silver.orders', metadata_file => '{metadata_location}')")
print("registered")

print("== tables in polaris.silver ==")
spark.sql("SHOW TABLES IN polaris.silver").show()

print("== read through polaris ==")
spark.sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM polaris.silver.orders").show()

spark.stop()
print("DONE")
