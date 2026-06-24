-- ============================================================
-- SCRIPT: 02_profile_bureau.sql
-- PURPOSE: Profile staging.bureau before designing clean layer
-- LAYER: Staging (read-only, no data changed)
-- ============================================================

-- STEP 1: Row count and applicant count (already partially known,
-- confirming again for this dedicated profiling script)
SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT sk_id_curr) AS unique_applicants
FROM staging.bureau;

-- STEP 2: Check CREDIT_ACTIVE categories and their frequency
-- This tells us what loan statuses actually exist in the data
-- before we decide if any need special handling
SELECT 
    credit_active,
    COUNT(*) AS record_count
FROM staging.bureau
GROUP BY credit_active
ORDER BY record_count DESC;

-- STEP 3: Check DAYS_CREDIT for impossible values
-- Since bureau is TEXT, we cast inline to check this safely
-- Should always be negative or zero (credit opened before or on
-- application date) — a positive value would be impossible
SELECT
    MIN(days_credit::NUMERIC) AS min_days_credit,
    MAX(days_credit::NUMERIC) AS max_days_credit,
    COUNT(*) FILTER (WHERE days_credit::NUMERIC > 0) AS impossible_future_credit
FROM staging.bureau
WHERE days_credit IS NOT NULL 
AND days_credit != '';

-- ============================================================
-- STEP 4 (corrected): Null analysis across ALL bureau columns
-- ============================================================

SELECT
    COUNT(*) FILTER (WHERE sk_id_bureau IS NULL OR sk_id_bureau = '') AS null_sk_id_bureau,
    COUNT(*) FILTER (WHERE credit_active IS NULL OR credit_active = '') AS null_credit_active,
    COUNT(*) FILTER (WHERE credit_currency IS NULL OR credit_currency = '') AS null_credit_currency,
    COUNT(*) FILTER (WHERE days_credit IS NULL OR days_credit = '') AS null_days_credit,
    COUNT(*) FILTER (WHERE credit_day_overdue IS NULL OR credit_day_overdue = '') AS null_credit_day_overdue,
    COUNT(*) FILTER (WHERE days_credit_enddate IS NULL OR days_credit_enddate = '') AS null_enddate,
    COUNT(*) FILTER (WHERE days_enddate_fact IS NULL OR days_enddate_fact = '') AS null_enddate_fact,
    COUNT(*) FILTER (WHERE amt_credit_max_overdue IS NULL OR amt_credit_max_overdue = '') AS null_max_overdue,
    COUNT(*) FILTER (WHERE cnt_credit_prolong IS NULL OR cnt_credit_prolong = '') AS null_cnt_prolong,
    COUNT(*) FILTER (WHERE amt_credit_sum IS NULL OR amt_credit_sum = '') AS null_credit_sum,
    COUNT(*) FILTER (WHERE amt_credit_sum_debt IS NULL OR amt_credit_sum_debt = '') AS null_credit_sum_debt,
    COUNT(*) FILTER (WHERE amt_credit_sum_limit IS NULL OR amt_credit_sum_limit = '') AS null_credit_sum_limit,
    COUNT(*) FILTER (WHERE amt_credit_sum_overdue IS NULL OR amt_credit_sum_overdue = '') AS null_credit_sum_overdue,
    COUNT(*) FILTER (WHERE credit_type IS NULL OR credit_type = '') AS null_credit_type,
    COUNT(*) FILTER (WHERE days_credit_update IS NULL OR days_credit_update = '') AS null_credit_update,
    COUNT(*) FILTER (WHERE amt_annuity IS NULL OR amt_annuity = '') AS null_annuity
FROM staging.bureau;