# FP-MCI-2026-Fabian

| Name           | NRP        |
| --- | --- |
| Severinus Fabian Tanuwidjaja            | 5025241110        |

# Introduction
Dataset that used in this project is DustiniaDelixia Groceria Dataset. The dataset was taken from https://its.id/m/Dataset_FP_MCI. The main purpose that I personally choose is to analyse the Customer Experience using mainly order_reviews table as the main data and the rest a support. Later on, I will analyse the reviews itself, then combine and compare it with product's category, delivery delay, and the geolocation of the seller and the customer.

# Project Files Structure
Here are the folders and files that i used. I combined the knowledge from the first MCI task until the last one. There are also some .ipynb file that I used to do EDA and data cleaning (just in case you also want to see the process of it).
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
The structure are mainly splitted into 4 part. 
1. The foundation support of the system which are the dockerfile etc.
2. The data that I use. It also spliited into 3, which are the raw data, the cleaned data that already been through data cleaning, and the final which are the result of everything, basically the tfidf file and the master_cleaned data that filled with one master table.
3. The scripts that each of it handle raw to cleaned data, cleaned to master data, tfidf, and the spark process for the final data.
4. The pipeline file that handle the flow of every file in the project.

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

# Code Explanation
There are 5 main codes in this projects which are all of the five .py files. Each of them handle each of the process of from the first data loading to the database connection.
## data_preparation.py
This script is responsible for the initial data preparation stage of the analytics pipeline. It loads multiple raw CSV datasets related to marketplace transactions, such as orders, reviews, products, customers, sellers, and product category translations using Apache Spark. After loading the data, the script performs several preprocessing operations including column selection, null filtering, duplicate removal, timestamp conversion, sentiment labeling, and delivery performance calculation. For example, customer reviews are classified into positive, neutral, or negative sentiment categories based on their review score, while delivery status is derived from the difference between estimated and actual delivery dates. <br>

After cleaning and transforming the datasets, the script stores each processed table as a Parquet file inside the data/cleaned/ directory. The use of Parquet format improves storage efficiency and enables faster processing in subsequent Spark jobs. This script acts as the foundation of the entire pipeline because all later stages depend on these cleaned datasets rather than the original raw CSV files.

## master_dataset.py
This script builds the main analytical dataset by combining all cleaned tables into a single master dataset. Using Spark joins, it integrates information from reviews, orders, order items, products, customers, sellers, and category translation tables. The resulting dataset contains both transactional and contextual information, allowing more advanced analytics to be performed later in the pipeline. <br>

In addition to joining tables, the script also performs feature engineering. It creates new analytical features such as delivery delay, delivery status, customer and seller state relationships, and same-state shipping indicators. The final master dataset is then cleaned again to remove invalid records and stored as a Parquet file inside data/final/master_cleaned.

## tfidf.py
This script performs Natural Language Processing (NLP) analysis on negative customer reviews using the TF-IDF (Term Frequency–Inverse Document Frequency) method. It begins by loading the master dataset and filtering only low-rated reviews. The review text is then normalized through lowercasing, special character removal, whitespace cleanup, and tokenization to prepare the text for vectorization.<br>

After preprocessing, Spark MLlib is used to generate TF-IDF vectors from the review text. The script calculates the average TF-IDF score for each product category and extracts the most representative negative keywords associated with that category. These keywords are then translated from Portuguese into English using Google Translator. Finally, the results are saved as tfidf_keywords.csv in the data/final/ directory. This output is later used for business insights and dashboard visualization in Metabase.

## process_spark.py
This script acts as the data warehouse loading stage of the pipeline. It loads the processed master dataset and TF-IDF results, then inserts them into ClickHouse, which functions as the analytical database for the project. The script first establishes a connection to ClickHouse, creates the required database and tables if they do not already exist, and truncates old data to ensure that only the latest pipeline output is stored.<br>

The script converts Spark DataFrames into Pandas DataFrames before transforming them into tuples for bulk insertion into ClickHouse. Two main analytical tables are populated: cleaned_master, which stores the integrated marketplace transaction data, and tfidf_keywords, which stores extracted negative review keywords by product category. This script enables fast querying and real-time dashboard visualization through Metabase.

## marketplace_pipeline.py
This script defines the orchestration workflow using Apache Airflow. It organizes the entire analytics process into a Directed Acyclic Graph (DAG), where each task represents one stage of the pipeline. The workflow starts from data_preparation.py, continues to master_dataset.py, then executes tfidf.py, and finally runs process_spark.py to load the final results into ClickHouse. <br>

The pipeline is configured to run automatically on a daily schedule and uses BashOperators to execute each Python script inside the Airflow container environment. Task dependencies ensure that every stage only runs after the previous stage completes successfully. This orchestration script is essential for automating the end-to-end ETL and analytics workflow, transforming raw marketplace data into structured business intelligence dashboards.

# DAG Result
<img width="1892" height="899" alt="image" src="https://github.com/user-attachments/assets/ede39d97-766a-4199-abf9-e47b355a2095" /> <br>
<img width="1078" height="222" alt="image" src="https://github.com/user-attachments/assets/1164a078-e124-42fd-8ed6-5ce77cdf766a" />



#ClickHouse


#Metabase Dashboard


