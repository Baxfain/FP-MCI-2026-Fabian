from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
import os
from pathlib import Path

# =====================================================
# BASE DIRECTORY
# =====================================================

from pathlib import Path

RAW_DIR = Path("/opt/airflow/data/raw")

CLEANED_DIR = Path("/opt/airflow/data/cleaned")
CLEANED_DIR.mkdir(parents=True,exist_ok=True)

# =====================================================
# SPARK SESSION
# =====================================================

spark = SparkSession.builder \
    .appName("CustomerReviewEDA") \
    .getOrCreate()

# =====================================================
# DATA LOADING
# =====================================================

reviews_df = spark.read.csv(
    str(RAW_DIR / "order_reviews.csv"),
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"',
    quote='"',
    mode="DROPMALFORMED"
)

orders_df = spark.read.csv(
    str(RAW_DIR / "orders.csv"),
    header=True,
    inferSchema=True
)

items_df = spark.read.csv(
    str(RAW_DIR / "order_items.csv"),
    header=True,
    inferSchema=True
)

products_df = spark.read.csv(
    str(RAW_DIR / "products.csv"),
    header=True,
    inferSchema=True
)

customers_df = spark.read.csv(
    str(RAW_DIR / "customers.csv"),
    header=True,
    inferSchema=True
)

sellers_df = spark.read.csv(
    str(RAW_DIR / "sellers.csv"),
    header=True,
    inferSchema=True
)

translation_df = spark.read.csv(
    str(RAW_DIR / "category_translation.csv"),
    header=True,
    inferSchema=True
)

# =====================================================
# DATA SELECTION
# =====================================================

reviews_df = reviews_df.select(
    "review_id",
    "order_id",
    "review_score",
    "review_comment_message"
)

reviews_df = reviews_df.withColumn(
    "sentiment",
    when(col("review_score") <= 2, "negative")
    .when(col("review_score") == 3, "neutral")
    .otherwise("positive")
)

orders_df = orders_df.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
)

orders_df = orders_df.filter(
    (col("order_status") == "delivered")
)

orders_df = orders_df.withColumn(
    "delivery_delay_days",
    datediff(
        col("order_delivered_customer_date"),
        col("order_estimated_delivery_date")
    )
)

orders_df = orders_df.withColumn(
    "delivery_status",
    when(
        col("delivery_delay_days") > 0,
        "late"
    ).when(
        col("delivery_delay_days") < 0,
        "early"
    ).otherwise("on_time")
)

items_df = items_df.select(
    "order_id",
    "product_id",
    "seller_id",
    "price",
    "freight_value"
)

products_df = products_df.select(
    "product_id",
    "product_category_name"
)

customers_selected = customers_df.select(
    "customer_id",
    "customer_city",
    "customer_state"
)


sellers_selected = sellers_df.select(
    "seller_id",
    "seller_city",
    "seller_state"
)


translation_df = translation_df.select(
    "product_category_name",
    "product_category_name_english"
)

# =====================================================
# CLEAN ORDERS DATASET
# =====================================================

cleaned_orders_df = orders_df.filter(
    col("order_delivered_customer_date").isNotNull()
&
    col("order_estimated_delivery_date").isNotNull()
)

cleaned_orders_df = cleaned_orders_df.dropDuplicates(
    ["order_id"]
)

# =====================================================
# CLEAN PRODUCTS DATASET
# =====================================================

cleaned_products_df = products_df.filter(
    col("product_category_name").isNotNull()
)
cleaned_products_df = cleaned_products_df.dropDuplicates(
    ["product_id"]
)
cleaned_products_df = cleaned_products_df.filter(
    col("product_category_name").isNotNull()
)
cleaned_products_df = cleaned_products_df.dropDuplicates(
    ["product_id"]
)

# =====================================================
# CLEAN REVIEWS DATASET
# =====================================================

cleaned_reviews_df = reviews_df.dropDuplicates(
    ["review_id"]
)

cleaned_reviews_df = cleaned_reviews_df.withColumn(
    "review_comment_message",
    trim(col("review_comment_message"))
)

cleaned_items_df = items_df

# =====================================================
# CLEAN CUSTOMERS DATASET
# =====================================================

cleaned_customers_df = customers_selected.dropDuplicates(
    ["customer_id"]
)

cleaned_sellers_df = sellers_df

# =====================================================
# SAVING PARQUET FILES
# =====================================================

timestamp_columns = [
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for col_name in timestamp_columns:
    if col_name in cleaned_orders_df.columns:
        cleaned_orders_df = cleaned_orders_df.withColumn(
            col_name,
            col(col_name).cast("timestamp")
        )

cleaned_orders_df.coalesce(1).write.mode("overwrite").parquet(
    str(CLEANED_DIR / "orders_cleaned")
)
print("orders_cleaned saved")

cleaned_reviews_df.coalesce(1).write.mode("overwrite").parquet(
    str(CLEANED_DIR / "reviews_cleaned")
)
print("reviews_cleaned saved")


cleaned_products_df.coalesce(1).write.mode("overwrite").parquet(
    str(CLEANED_DIR / "products_cleaned")
)
print("products_cleaned saved")


cleaned_items_df.coalesce(1).write.mode("overwrite").parquet(
    str(CLEANED_DIR / "items_cleaned")
)
print("items_cleaned saved")


cleaned_customers_df.coalesce(1).write.mode("overwrite").parquet(
    str(CLEANED_DIR / "customers_cleaned")
)
print("customers_cleaned saved")


cleaned_sellers_df.coalesce(1).write.mode("overwrite").parquet(
    str(CLEANED_DIR / "sellers_cleaned")
)
print("sellers_cleaned saved")

translation_df.coalesce(1).write.mode("overwrite").parquet(
    str(CLEANED_DIR / "translations_cleaned")
)
print("translations_cleaned saved")