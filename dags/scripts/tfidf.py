from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.ml.feature import (
    RegexTokenizer,
    CountVectorizer,
    IDF
)
from pyspark.ml.stat import Summarizer

import pandas as pd
import numpy as np
from pathlib import Path
from deep_translator import GoogleTranslator

# =====================================================
# BASE DIRECTORY
# =====================================================

CLEANED_DIR = Path("/opt/airflow/data/cleaned")

FINAL_DIR = Path("/opt/airflow/data/final")
FINAL_DIR.mkdir(parents=True,exist_ok=True)

# =====================================================
# SPARK SESSION
# =====================================================

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TFIDFAnalysis") \
    .getOrCreate()

# =====================================================
# DATA LOADING
# =====================================================

cleaned_master_df = spark.read.parquet(
    str(FINAL_DIR / "master_cleaned")
)

# =====================================================
# DATA SELECTION
# =====================================================

negative_reviews_df = cleaned_master_df.filter(
    col("review_score") <= 2
)

# =====================================================
# DATA CLEANING
# =====================================================

print(f"Old data total: {negative_reviews_df.count()}")
negative_reviews_df = negative_reviews_df.filter(
    col("review_comment_message") != "no_review"
)

negative_reviews_df = negative_reviews_df.filter(
    trim(col("review_comment_message")) != ""
)

print(f"Cleaned data total: {negative_reviews_df.count()}")

# =====================================================
# DATA NORMALIZATION
# =====================================================

negative_reviews_df = negative_reviews_df.withColumn(
    "review_comment_message",
    trim(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    lower(
                        col("review_comment_message")
                    ),
                    r"\n|\t", " "
                ),
                r"[^a-zA-ZÀ-ÿ\s]",""
            ),
            r"\s+", " "
        )
    )
)

# =====================================================
# TEXT PREPROCESSING
# =====================================================

tokenizer = RegexTokenizer(
    inputCol="review_comment_message",
    outputCol="words",
    pattern="\\s+"
)

df_token = tokenizer.transform(negative_reviews_df)

# =====================================================
# TF-IDF ANALYSIS
# =====================================================

cv = CountVectorizer(
    inputCol="words",
    outputCol="tf",
    vocabSize=20000
)
cv_model = cv.fit(df_token)
df_tf = cv_model.transform(df_token)


idf = IDF(inputCol="tf", outputCol="tfidf")
idf_model = idf.fit(df_tf)
df_tfidf = idf_model.transform(df_tf)


idf_values = idf_model.idf.toArray()
vocab = cv_model.vocabulary
threshold = 4.0


important_words = [
    vocab[i] for i, val in enumerate(idf_values)
    if val > threshold
]

print("Important Words Total:", len(important_words))
print("Example:", important_words[:20])

# =====================================================
# SIGNATURE KEYWORD EXTRACTION
# =====================================================

negative_words = [
    "defeito",
    "quebrado",
    "quebrada",
    "atraso",
    "atrasada",
    "faltando",
    "erro",
    "ruim",
    "péssimo",
    "demora",
    "danificado",
    "problema",
    "cancelado",
    "fraco",
    "sujo",
    "enganado",
    "defeituoso",
    "incompleto",
    "rasgado",
    "amassado",
    "desligado",
    "vazando",
    "errado",
    "trincado",
    "reclamação"
]

df_category_grouped = (

    df_tfidf.groupBy(
        "product_category_name_english"
    )
    .agg(
        Summarizer.mean(
            col("tfidf")
        ).alias("avg_tfidf")
    )
)

category_rows = (
    df_category_grouped.collect()
)

# =====================================================
# FINAL
# =====================================================

results = []

for row in category_rows:
    category = row[
        "product_category_name_english"
    ]
    tfidf_vector = row[
        "avg_tfidf"
    ]
    arr = tfidf_vector.toArray()
    nonzero_indices = np.where(
        arr > 0
    )[0]
    valid_candidates = [
        i for i in nonzero_indices
        if (
            idf_values[i] > threshold
            and vocab[i] in negative_words
        )
    ]

    sorted_keywords = sorted(
        valid_candidates,
        key=lambda i: arr[i],
        reverse=True
    )

    top_keywords = [
        vocab[i]
        for i in sorted_keywords[:5]
    ]

    results.append({
        "category": category,
        "negative_keywords": top_keywords
    })

result_pd = pd.DataFrame(
    results
)

# =====================================================
# TRANSLATION
# =====================================================

def translate_keywords(keyword_list):

    translated = []

    for word in keyword_list:

        try:

            translated_word = GoogleTranslator(
                source="pt",
                target="en"
            ).translate(word)

            translated.append(translated_word)

        except:

            translated.append(word)

    return translated

translated_result_pd = result_pd.copy()

translated_result_pd["translated_keywords"] = (
    translated_result_pd["negative_keywords"]
    .apply(translate_keywords)
)

translated_result_pd["translated_keywords"] = (
    translated_result_pd["translated_keywords"]
    .apply(lambda x: ", ".join(x))
)

translated_result_pd = translated_result_pd[
    ["category", "translated_keywords"]
]

translated_result_pd = translated_result_pd.fillna("")

translated_result_pd["category"] = (
    translated_result_pd["category"]
    .astype(str)
)

translated_result_pd["translated_keywords"] = (
    translated_result_pd["translated_keywords"]
    .astype(str)
)

translated_result_pd.to_csv(
    str(FINAL_DIR / "tfidf_keywords.csv"),
    index=False
)

print("tfidf_keywords.csv saved")

spark.stop()