from pathlib import Path
from pyspark.sql import SparkSession
from clickhouse_driver import Client
import pandas as pd

FINAL_DIR = Path("/opt/airflow/data/final")


# =====================================================
# START SPARK
# =====================================================

spark = SparkSession.builder \
    .appName("LoadToClickHouse") \
    .getOrCreate()

# =====================================================
# CONNECT CLICKHOUSE
# =====================================================

client = Client(
    host="clickhouse-server",
    user="admin",
    password="rahasia"
)

client.execute(
    "CREATE DATABASE IF NOT EXISTS analytics"
)

# =====================================================
# LOAD CLEANED MASTER
# =====================================================

print("Loading cleaned_master...")

cleaned_master_df = spark.read.parquet(
    str(FINAL_DIR / "master_cleaned")
)

master_pd = cleaned_master_df.toPandas()

# =====================================================
# CREATE MASTER TABLE
# =====================================================

client.execute("""
    CREATE TABLE IF NOT EXISTS analytics.cleaned_master (
        order_id String,
        review_score Int32,
        review_comment_message String,
        sentiment String,
        product_category_name_english String,
        delivery_delay_days Float64,
        delivery_status String,
        customer_state String,
        seller_id String,
        seller_state String,
        same_state_shipping UInt8
    )
    ENGINE = MergeTree()
    ORDER BY order_id
""")


print("Inserting cleaned_master...")
client.execute("TRUNCATE TABLE analytics.cleaned_master")
master_tuples = [tuple(x) for x in master_pd.to_numpy()]

if master_tuples:
    client.execute("INSERT INTO analytics.cleaned_master VALUES", master_tuples)
print(f"{len(master_tuples)} rows inserted into cleaned_master")


# =====================================================
# LOAD TFIDF CSV
# =====================================================

print("Loading tfidf_keywords...")
tfidf_pd = pd.read_csv(
    str(FINAL_DIR / "tfidf_keywords.csv")
)

tfidf_pd = tfidf_pd.fillna("").astype(str)


# =====================================================
# CREATE TFIDF TABLE
# =====================================================

client.execute("""
    CREATE TABLE IF NOT EXISTS analytics.tfidf_keywords (
        category String,
        translated_keywords String
    )
    ENGINE = MergeTree()
    ORDER BY category
""")
print("Inserting tfidf_keywords...")

client.execute("TRUNCATE TABLE analytics.tfidf_keywords")
tfidf_tuples = [tuple(x) for x in tfidf_pd.to_numpy()]

if tfidf_tuples:
    client.execute("INSERT INTO analytics.tfidf_keywords VALUES", tfidf_tuples)
print(f"{len(tfidf_tuples)} rows inserted into tfidf_keywords")

# =====================================================
# STOP SPARK
# =====================================================

spark.stop()

print("Pipeline selesai")