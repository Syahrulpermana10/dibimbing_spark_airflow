import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Nama host Spark Master
spark_master_host = "dibimbing-dataeng-spark-master"

# URL Spark Master
spark_host = f"spark://{spark_master_host}:7077"

# Path JAR PostgreSQL
postgresql_jar_path = "/opt/bitnami/spark/jars/postgresql-42.2.18.jar"

# Konfigurasi Spark
spark_conf = (
    pyspark
    .SparkConf()
    .setAppName('Dibimbing')
    .setMaster(spark_host)
    .set("spark.jars", postgresql_jar_path)
)

# Inisialisasi SparkContext
sparkcontext = pyspark.SparkContext.getOrCreate(conf=spark_conf)
sparkcontext.setLogLevel("WARN")

# Inisialisasi SparkSession
spark = SparkSession(sparkcontext.getOrCreate())

# Database connection parameters
db_properties = {
    "user": "your_postgres_user",
    "password": "your_postgres_password",
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://dataeng-postgres:5433/your_postgres_db",
}

# Read data from PostgreSQL table
table_name = "retail_data"
df = spark.read.format("jdbc").option("url", db_properties["url"]).option("dbtable", table_name).option("user", db_properties["user"]).option("password", db_properties["password"]).option("driver", db_properties["driver"]).load()

# Filter data based on the specified date range
df = df.withColumn("InvoiceDate", to_date("InvoiceDate", "MM/dd/yyyy"))
date_range_start = "2011-01-10"
date_range_end = "2011-09-09"
df_filtered = df.filter((col("InvoiceDate") >= date_range_start) & (col("InvoiceDate") <= date_range_end))

# Churn Analysis
churn_analysis = df_filtered.groupBy("CustomerID").agg(
    max("InvoiceDate").alias("LastPurchaseDate")
)
churned_customers = churn_analysis.filter(col("LastPurchaseDate") <= date_range_start)

# Output the churned customers to CSV
output_path = "/output"
churned_customers.write.csv(output_path + "/churned_customers", header=True)

# Retention Analysis
retention_analysis = df_filtered.groupBy("CustomerID").agg(
    min("InvoiceDate").alias("FirstPurchaseDate"),
    max("InvoiceDate").alias("LastPurchaseDate")
)

retained_customers = retention_analysis.filter((col("FirstPurchaseDate") < date_range_start) & (col("LastPurchaseDate") >= date_range_end))

# Output the retained customers to CSV
retained_customers.write.csv(output_path + "/retained_customers", header=True)

# Stop Spark session
spark.stop()