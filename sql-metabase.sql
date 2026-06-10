-- =========================================================
-- NUMBER CARD
-- =========================================================

-- Total review records
SELECT COUNT(*)
FROM analytics.cleaned_master;

-- Average review score
SELECT ROUND(AVG(review_score), 2)
FROM analytics.cleaned_master;

-- Total negative reviews
SELECT COUNT(*)
FROM analytics.cleaned_master
WHERE sentiment = 'negative';

-- Late delivery percentage
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

-- Same-state shipping percentage
SELECT
    ROUND(
        AVG(same_state_shipping) * 100,
        2
    ) AS same_state_shipping_percentage
FROM analytics.cleaned_master;


-- =========================================================
-- BAR CHART (Vertical / Horizontal)
-- =========================================================

-- Top product categories by total reviews
SELECT
    product_category_name_english,
    COUNT(*) AS total_reviews
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY total_reviews DESC
LIMIT 10;

-- Product categories with worst average review
SELECT
    product_category_name_english,
    ROUND(AVG(review_score), 2) AS avg_review_score
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY avg_review_score ASC
LIMIT 10;

-- Product categories with highest delivery delay
SELECT
    product_category_name_english,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delay
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY avg_delay DESC
LIMIT 10;

-- Seller states with most orders
SELECT
    seller_state,
    COUNT(*) AS total_orders
FROM analytics.cleaned_master
GROUP BY seller_state
ORDER BY total_orders DESC;

-- Customer states with most orders
SELECT
    customer_state,
    COUNT(*) AS total_orders
FROM analytics.cleaned_master
GROUP BY customer_state
ORDER BY total_orders DESC;

-- Same-state vs different-state shipping review comparison
SELECT
    CASE
        WHEN same_state_shipping = 1
            THEN 'Same State'
        ELSE 'Different State'
    END AS shipping_type,

    ROUND(AVG(review_score), 2) AS avg_review_score,

    COUNT(*) AS total_orders

FROM analytics.cleaned_master

GROUP BY shipping_type

ORDER BY avg_review_score DESC;


-- Top 10 negative words by TF-IDF score
SELECT
    keyword,
    COUNT(*) AS total_mentions
FROM
(
    SELECT
        arrayJoin(
            splitByString(', ', translated_keywords)
        ) AS keyword
    FROM analytics.tfidf_keywords
)
GROUP BY keyword
ORDER BY total_mentions DESC
limit 10;

-- =========================================================
-- PIE CHART
-- =========================================================

-- Sentiment composition
SELECT
    sentiment,
    COUNT(*) AS total_reviews
FROM analytics.cleaned_master
GROUP BY sentiment
ORDER BY total_reviews DESC;

-- Delivery status composition
SELECT
    delivery_status,
    COUNT(*) AS total_orders
FROM analytics.cleaned_master
GROUP BY delivery_status
ORDER BY total_orders DESC;

-- Shipping type composition
SELECT
    CASE
        WHEN same_state_shipping = 1
            THEN 'Same State'
        ELSE 'Different State'
    END AS shipping_type,
    COUNT(*) AS total_orders
FROM analytics.cleaned_master
GROUP BY shipping_type
ORDER BY total_orders DESC;

-- Review score composition
SELECT
    review_score,
    COUNT(*) AS total_reviews
FROM analytics.cleaned_master
GROUP BY review_score
ORDER BY review_score;


-- =========================================================
-- SCATTER PLOT
-- =========================================================

-- Delivery delay vs review score
SELECT
    delivery_delay_days,
    review_score
FROM analytics.cleaned_master
WHERE delivery_delay_days IS NOT NULL;

-- Average review vs average delay per category
SELECT
    product_category_name_english,
    ROUND(AVG(review_score), 2) AS avg_review,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delay
FROM analytics.cleaned_master
GROUP BY product_category_name_english;

-- Seller performance
SELECT
    seller_id,
    ROUND(AVG(review_score), 2) AS avg_review,
    COUNT(*) AS total_orders
FROM analytics.cleaned_master
GROUP BY seller_id;


-- =========================================================
-- TABLE CARD
-- =========================================================

-- Top 10 worst reviewed categories
SELECT
    product_category_name_english,
    ROUND(AVG(review_score), 2) AS avg_review,
    COUNT(*) AS total_reviews
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY avg_review ASC
LIMIT 10;

-- Top delayed categories
SELECT
    product_category_name_english,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delay,
    COUNT(*) AS total_orders
FROM analytics.cleaned_master
GROUP BY product_category_name_english
ORDER BY avg_delay DESC
LIMIT 10;

-- Top seller states with negative reviews
SELECT
    seller_state,
    COUNT(*) AS negative_reviews
FROM analytics.cleaned_master
WHERE sentiment = 'negative'
GROUP BY seller_state
ORDER BY negative_reviews DESC
LIMIT 10;

-- TF-IDF negative keywords table
SELECT *
FROM analytics.tfidf_keywords
LIMIT 20;


-- =========================================================
-- TF-IDF ANALYTICS
-- =========================================================

-- Categories containing damaged complaints
SELECT *
FROM analytics.tfidf_keywords
WHERE translated_keywords LIKE '%damaged%';

-- Categories containing delay complaints
SELECT *
FROM analytics.tfidf_keywords
WHERE translated_keywords LIKE '%delay%';

-- Categories containing broken complaints
SELECT *
FROM analytics.tfidf_keywords
WHERE translated_keywords LIKE '%broken%';

-- Categories containing defective complaints
SELECT *
FROM analytics.tfidf_keywords
WHERE translated_keywords LIKE '%defect%';