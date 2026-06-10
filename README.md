# FP-MCI-2026-Fabian

| Name           | NRP        |
| --- | --- |
| Severinus Fabian Tanuwidjaja            | 5025241110        |

#Introduction
Dataset that used in this project is DustiniaDelixia Groceria Dataset. The dataset was taken from https://its.id/m/Dataset_FP_MCI. The main purpose that I personally choose is to analyse the Customer Experience using mainly order_reviews table as the main data and the rest a support. Later on, I will analyse the reviews itself, then combine and compare it with product's category, delivery delay, and the geolocation of the seller and the customer.

#Project Files Structure
```
FINAL_PROJECT
│
├── dags
│   │
│   ├── marketplace_pipeline.py
│   │
│   ├── __pycache__
│   │
│   └── scripts
│       │
│       ├── __pycache__
│       │
│       ├── data_preparation.py
│       ├── master_dataset.py
│       ├── tfidf.py
│       └── process_spark.py
│
├── data
│   │
│   ├── raw
│   │   │
│   │   ├── category_translation.csv
│   │   ├── closed_deals.csv
│   │   ├── customers.csv
│   │   ├── geolocation.csv
│   │   ├── mql.csv
│   │   ├── order_items.csv
│   │   ├── order_payments.csv
│   │   ├── order_reviews.csv
│   │   ├── orders.csv
│   │   ├── products.csv
│   │   └── sellers.csv
│   │
│   ├── cleaned
│   │   │
│   │   ├── customers_cleaned
│   │   │   ├── _SUCCESS
│   │   │   ├── ._SUCCESS.crc
│   │   │   ├── part-00000-....
│   │   │   └── .part-00000-....crc
│   │   │
│   │   ├── items_cleaned
│   │   │   ├── _SUCCESS
│   │   │   ├── ._SUCCESS.crc
│   │   │   ├── part-00000-....
│   │   │   └── .part-00000-....crc
│   │   │
│   │   ├── orders_cleaned
│   │   │   ├── _SUCCESS
│   │   │   ├── ._SUCCESS.crc
│   │   │   ├── part-00000-....
│   │   │   └── .part-00000-....crc
│   │   │
│   │   ├── products_cleaned
│   │   │   ├── _SUCCESS
│   │   │   ├── ._SUCCESS.crc
│   │   │   ├── part-00000-....
│   │   │   └── .part-00000-....crc
│   │   │
│   │   ├── reviews_cleaned
│   │   │   ├── _SUCCESS
│   │   │   ├── ._SUCCESS.crc
│   │   │   ├── part-00000-....
│   │   │   └── .part-00000-....crc
│   │   │
│   │   ├── sellers_cleaned
│   │   │   ├── _SUCCESS
│   │   │   ├── ._SUCCESS.crc
│   │   │   ├── part-00000-....
│   │   │   └── .part-00000-....crc
│   │   │
│   │   └── translations_cleaned
│   │       ├── _SUCCESS
│   │       ├── ._SUCCESS.crc
│   │       ├── part-00000-....
│   │       └── .part-00000-....crc
│   │
│   └── final
│       │
│       ├── master_cleaned
│       │   ├── _SUCCESS
│       │   ├── ._SUCCESS.crc
│       │   ├── part-00000-....
│       │   └── .part-00000-....crc
│       │
│       └── tfidf_keywords.csv
│
├── notebook
│   │
│   ├── data_preparation.ipynb
│   ├── master_dataset.ipynb
│   └── tfidf.ipynb
│
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── sql-clickhouse.sql
└── sql-metabase.sql
```

#Timeline & Workflow
```
RAW CSV
   ↓
data_preparation.py
   ↓
cleaned parquet
   ↓
master_dataset.py
   ↓
master_cleaned parquet
   ↓
tfidf.py
   ↓
tfidf_keywords.csv
   ↓
process_spark.py
   ↓
ClickHouse
   ↓
Metabase Dashboard
```

#Code Explanation


#DAG Result


#ClickHouse


#Metabase Dashboard


