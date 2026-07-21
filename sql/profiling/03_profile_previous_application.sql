-- ============================================================
-- SCRIPT: 03_profile_previous_application.sql
-- PURPOSE: Profile staging.previous_application
-- LAYER: Staging (read-only, no data changed)
-- ============================================================

-- STEP 1: Row count and unique applicants
-- expect ~1,670,214 rows and ~338,857 unique applicants
-- from earlier light profiling check
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT sk_id_curr) AS unique_applicants,
    ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT sk_id_curr), 1)
        AS avg_applications_per_applicant
FROM staging.previous_application;

-- STEP 2: Key categorical columns
-- NAME_CONTRACT_STATUS tells outcome of each previous application
-- This is one of the most important columns in this table
-- for credit risk — a history of rejections is a strong signal
SELECT
    name_contract_status,
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM staging.previous_application
GROUP BY name_contract_status
ORDER BY record_count DESC;

-- STEP 3: Full null analysis across all columns
-- Check IS NULL and empty string '' because this table
-- was ingested as TEXT — both patterns mean "missing"
SELECT
    COUNT(*) FILTER (WHERE sk_id_prev IS NULL OR sk_id_prev = '') AS null_sk_id_prev,
    COUNT(*) FILTER (WHERE amt_annuity IS NULL OR amt_annuity = '') AS null_annuity,
    COUNT(*) FILTER (WHERE amt_application IS NULL OR amt_application = '') AS null_amt_application,
    COUNT(*) FILTER (WHERE amt_credit IS NULL OR amt_credit = '') AS null_amt_credit,
    COUNT(*) FILTER (WHERE amt_down_payment IS NULL OR amt_down_payment = '') AS null_down_payment,
    COUNT(*) FILTER (WHERE amt_goods_price IS NULL OR amt_goods_price = '') AS null_goods_price,
    COUNT(*) FILTER (WHERE name_type_suite IS NULL OR name_type_suite = '') AS null_name_type_suite,
    COUNT(*) FILTER (WHERE name_client_type IS NULL OR name_client_type = '') AS null_client_type,
    COUNT(*) FILTER (WHERE name_goods_category IS NULL OR name_goods_category = '') AS null_goods_category,
    COUNT(*) FILTER (WHERE name_portfolio IS NULL OR name_portfolio = '') AS null_portfolio,
    COUNT(*) FILTER (WHERE name_product_type IS NULL OR name_product_type = '') AS null_product_type,
    COUNT(*) FILTER (WHERE channel_type IS NULL OR channel_type = '') AS null_channel_type,
    COUNT(*) FILTER (WHERE sellerplace_area IS NULL OR sellerplace_area = '') AS null_sellerplace_area,
    COUNT(*) FILTER (WHERE name_seller_industry IS NULL OR name_seller_industry = '') AS null_seller_industry,
    COUNT(*) FILTER (WHERE cnt_payment IS NULL OR cnt_payment = '') AS null_cnt_payment,
    COUNT(*) FILTER (WHERE name_yield_group IS NULL OR name_yield_group = '') AS null_yield_group,
    COUNT(*) FILTER (WHERE product_combination IS NULL OR product_combination = '') AS null_product_combination,
    COUNT(*) FILTER (WHERE days_first_drawing IS NULL OR days_first_drawing = '') AS null_days_first_drawing,
    COUNT(*) FILTER (WHERE days_first_due IS NULL OR days_first_due = '') AS null_days_first_due,
    COUNT(*) FILTER (WHERE days_last_due_1st_version IS NULL OR days_last_due_1st_version = '') AS null_days_last_due_1st,
    COUNT(*) FILTER (WHERE days_last_due IS NULL OR days_last_due = '') AS null_days_last_due,
    COUNT(*) FILTER (WHERE days_termination IS NULL OR days_termination = '') AS null_days_termination,
    COUNT(*) FILTER (WHERE nflag_insured_on_approval IS NULL OR nflag_insured_on_approval = '') AS null_nflag_insured
FROM staging.previous_application;