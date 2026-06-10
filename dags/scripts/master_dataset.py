from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from pathlib import Path
import os

# =====================================================
# BASE DIRECTORY
# =====================================================
RAW_DIR = Path("/opt/airflow/data/raw")

CLEANED_DIR = Path("/opt/airflow/data/cleaned")
CLEANED_DIR.mkdir(parents=True,exist_ok=True)

FINAL_DIR = Path("/opt/airflow/data/final")
FINAL_DIR.mkdir(parents=True,exist_ok=True)

# =====================================================
# SPARK SESSION
# =====================================================

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("MasterCustomerExperience") \
    .getOrCreate()

# =====================================================
# LOAD CLEANED TABLES
# =====================================================

cleaned_orders_df = spark.read.parquet(
    str(CLEANED_DIR / "orders_cleaned")
)

cleaned_reviews_df = spark.read.parquet(
    str(CLEANED_DIR / "reviews_cleaned")
)

cleaned_products_df = spark.read.parquet(
    str(CLEANED_DIR / "products_cleaned")
)

cleaned_items_df = spark.read.parquet(
    str(CLEANED_DIR / "items_cleaned")
)

cleaned_customers_df = spark.read.parquet(
    str(CLEANED_DIR / "customers_cleaned")
)

cleaned_sellers_df = spark.read.parquet(
    str(CLEANED_DIR / "sellers_cleaned")
)

cleaned_translation_df = spark.read.parquet(
    str(CLEANED_DIR / "translations_cleaned")
)

# =====================================================
# JOIN REVIEWS + ORDERS
# =====================================================

master_df = cleaned_reviews_df.join(
    cleaned_orders_df,
    on="order_id",
    how="inner"
)


# =====================================================
# JOIN ORDER ITEMS
# =====================================================

master_df = master_df.join(
    cleaned_items_df,
    on="order_id",
    how="inner"
)


# =====================================================
# JOIN PRODUCTS
# =====================================================

master_df = master_df.join(
    cleaned_products_df,
    on="product_id",
    how="left"
)


# =====================================================
# JOIN CATEGORY TRANSLATION
# =====================================================

master_df = master_df.join(
    cleaned_translation_df,
    on="product_category_name",
    how="left"
)

# =====================================================
# JOIN CUSTOMERS
# =====================================================

master_df = master_df.join(
    cleaned_customers_df,
    on="customer_id",
    how="left"
)

# =====================================================
# JOIN SELLERS
# =====================================================

master_df = master_df.join(
    cleaned_sellers_df,
    on="seller_id",
    how="left"
)

# =====================================================
# DATA SELECTION
# =====================================================

master_df = master_df.select(

    # REVIEW

    "order_id",
    "review_score",
    "review_comment_message",
    "sentiment",

    # PRODUCT

    "product_category_name_english",

    # ORDER

    "delivery_delay_days",
    "delivery_status",

    # CUSTOMER
    "customer_state",

    # SELLER

    "seller_id",
    "seller_state"
)

# =====================================================
# DATA CLEANING
# =====================================================

cleaned_master_df = master_df.filter(
    col("product_category_name_english").isNotNull()
)
cleaned_master_df = cleaned_master_df.filter(
    col("delivery_status").isNotNull()
)
cleaned_master_df = cleaned_master_df.fillna({
    "review_comment_message": "no_review"
})
cleaned_master_df = cleaned_master_df.dropDuplicates()

# =====================================================
# ADD FEATURES
# =====================================================

print("\n===== SAME STATE SHIPPING FEATURE =====")

cleaned_master_df = cleaned_master_df.withColumn(
    "same_state_shipping",
    when(
        col("customer_state") == col("seller_state"), 1
    ).otherwise(0)
)

# =====================================================
# SAVING PARQUET FILE
# =====================================================

cleaned_master_df.coalesce(1).write.mode("overwrite").parquet(
    str(FINAL_DIR / "master_cleaned")
)

print("cleaned_master saved")

spark.stop()