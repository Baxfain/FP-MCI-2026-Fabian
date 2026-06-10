-- =====================================================
-- DATABASE CHECK
-- =====================================================

SHOW DATABASES;

USE analytics;

SHOW TABLES;

DESCRIBE analytics.cleaned_master;

DESCRIBE analytics.tfidf_keywords;


-- =====================================================
-- BASIC DATA OVERVIEW
-- =====================================================

-- Total rows in master dataset
SELECT COUNT(*)
FROM analytics.cleaned_master;

-- Total product categories analyzed
SELECT COUNT(DISTINCT product_category_name_english)
FROM analytics.cleaned_master;

-- Total sellers
SELECT COUNT(DISTINCT seller_id)
FROM analytics.cleaned_master;

-- Total customer states
SELECT COUNT(DISTINCT customer_state)
FROM analytics.cleaned_master;


-- =====================================================
-- SAMPLE DATA
-- =====================================================

-- Show sample records
SELECT *
FROM analytics.cleaned_master
LIMIT 10;

-- Show TF-IDF keyword samples
SELECT *
FROM analytics.tfidf_keywords
LIMIT 10;


-- =====================================================
-- REVIEW ANALYTICS
-- =====================================================

-- Average review score
SELECT AVG(review_score)
FROM analytics.cleaned_master;

-- Review distribution
SELECT
    review_score,
    COUNT(*)
FROM analytics.cleaned_master
GROUP BY review_score
ORDER BY review_score;

-- Sentiment distribution
SELECT
    sentiment,
    COUNT(*)
FROM analytics.cleaned_master
GROUP BY sentiment
ORDER BY COUNT(*) DESC;

-- Negative reviews only
SELECT review_comment_message
FROM analytics.cleaned_master
WHERE sentiment = 'negative'
LIMIT 20;


-- =====================================================
-- DELIVERY ANALYTICS
-- =====================================================

-- Delivery status distribution
SELECT
    delivery_status,
    COUNT(*)
FROM analytics.cleaned_master
GROUP BY delivery_status
ORDER BY COUNT(*) DESC;

-- Average delivery delay
SELECT AVG(delivery_delay_days)
FROM analytics.cleaned_master;

-- Most delayed product categories
SELECT
    product_category_name_english,
    AVG(delivery_delay_days) AS avg_delay
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY avg_delay DESC
LIMIT 10;

-- Fastest delivered categories
SELECT
    product_category_name_english,
    AVG(delivery_delay_days) AS avg_delay
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY avg_delay ASC
LIMIT 10;


-- =====================================================
-- STATE ANALYTICS
-- =====================================================

-- Customer state distribution
SELECT
    customer_state,
    COUNT(*)
FROM analytics.cleaned_master
GROUP BY customer_state
ORDER BY COUNT(*) DESC;

-- Seller state distribution
SELECT
    seller_state,
    COUNT(*)
FROM analytics.cleaned_master
GROUP BY seller_state
ORDER BY COUNT(*) DESC;

-- Same-state shipping ratio
SELECT
    same_state_shipping,
    COUNT(*)
FROM analytics.cleaned_master
GROUP BY same_state_shipping;

-- Same-state shipping percentage
SELECT
    ROUND(
        AVG(same_state_shipping) * 100,
        2
    ) AS same_state_shipping_percentage
FROM analytics.cleaned_master;


-- =====================================================
-- CATEGORY ANALYTICS
-- =====================================================

-- Most reviewed categories
SELECT
    product_category_name_english,
    COUNT(*) AS total_reviews
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY total_reviews DESC
LIMIT 10;

-- Categories with worst average review
SELECT
    product_category_name_english,
    AVG(review_score) AS avg_review
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY avg_review ASC
LIMIT 10;

-- Categories with best average review
SELECT
    product_category_name_english,
    AVG(review_score) AS avg_review
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY avg_review DESC
LIMIT 10;


-- =====================================================
-- NEGATIVE REVIEW ANALYTICS
-- =====================================================

-- Categories with most negative reviews
SELECT
    product_category_name_english,
    COUNT(*) AS negative_reviews
FROM analytics.cleaned_master
WHERE sentiment = 'negative'
GROUP BY product_category_name_english
ORDER BY negative_reviews DESC
LIMIT 10;

-- Average delay for each sentiment
SELECT
    sentiment,
    AVG(delivery_delay_days)
FROM analytics.cleaned_master
GROUP BY sentiment;

-- Late deliveries causing negative sentiment
SELECT
    delivery_status,
    sentiment,
    COUNT(*)
FROM analytics.cleaned_master
GROUP BY delivery_status, sentiment
ORDER BY COUNT(*) DESC;


-- =====================================================
-- TF-IDF KEYWORD ANALYTICS
-- =====================================================

-- View extracted negative keywords
SELECT *
FROM analytics.tfidf_keywords
LIMIT 20;

-- Categories containing damaged-related complaints
SELECT *
FROM analytics.tfidf_keywords
WHERE translated_keywords LIKE '%damaged%';

-- Categories containing delay complaints
SELECT *
FROM analytics.tfidf_keywords
WHERE translated_keywords LIKE '%delay%';

-- Categories containing broken product complaints
SELECT *
FROM analytics.tfidf_keywords
WHERE translated_keywords LIKE '%broken%';


-- =====================================================
-- KPI QUERIES
-- =====================================================

-- KPI: Overall average review score
SELECT
    ROUND(AVG(review_score), 2) AS avg_review_score
FROM analytics.cleaned_master;

-- KPI: Total negative reviews
SELECT
    COUNT(*) AS total_negative_reviews
FROM analytics.cleaned_master
WHERE sentiment = 'negative';

-- KPI: Late delivery percentage
SELECT
    ROUND(
        SUM(
            CASE
                WHEN delivery_status = 'late' THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*),
        2
    ) AS late_delivery_percentage
FROM analytics.cleaned_master;

-- KPI: Average delivery delay
SELECT
    ROUND(AVG(delivery_delay_days), 2)
FROM analytics.cleaned_master;