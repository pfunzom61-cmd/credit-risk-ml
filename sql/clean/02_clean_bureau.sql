-- ============================================================
-- SCRIPT: 02_clean_bureau.sql
-- PURPOSE: Build clean.bureau from staging.bureau
-- LAYER: Clean (type alignment + business logic decisions)
-- ============================================================
-- DECISIONS MADE IN THIS SCRIPT (documented for audit trail):
--
-- 1. TYPE ALIGNMENT
--    All columns ingested as TEXT. Cast to correct types:
--    - sk_id_curr, sk_id_bureau, days_credit, credit_day_overdue,
--      cnt_credit_prolong, days_credit_update → BIGINT
--    - days_credit_enddate, days_enddate_fact → NUMERIC(14,3)
--    - amt_* money columns → NUMERIC(14,3)
--    - credit_active, credit_currency, credit_type → VARCHAR
--
-- 2. amt_annuity → DROPPED ENTIRELY
--    71.5% null, no clean signal confirmed via diagnostic join.
--
-- 3. amt_credit_max_overdue → FILL NULL WITH 0
--    65.5% null. "No overdue recorded" treated as zero overdue.
--
-- 4. amt_credit_sum → MEDIAN IMPUTATION (only 13 nulls)
--
-- 5. amt_credit_sum_debt → MEDIAN IMPUTATION (15% null)
--
-- 6. amt_credit_sum_limit → MEDIAN IMPUTATION (34.5% null)
--
-- 7. days_credit_enddate → LEFT AS NULL (6.1% null)
--
-- 8. days_enddate_fact → LEFT AS NULL (confirmed structural:
--    Active loans 99.7% null because they haven't ended yet)
--
-- 9. Orphaned records excluded via INNER JOIN at feature layer,
--    not removed here. Staging data is never modified.
--
-- PERFORMANCE NOTE:
--    bureau has 1,716,428 rows. Using inline subqueries for
--    median imputation (as in application_train) caused the
--    query to time out. Fix: pre-calculate all medians once
--    into a temporary table first, then reference fixed scalar
--    values in the CTAS. This runs the median calculation once
--    across the full column instead of once per row.
-- ============================================================

-- ── STEP 1: Pre-calculate medians once upfront ─────────────
-- We store results in a temp table so the CTAS below can
-- reference fixed scalar values instead of recalculating
-- PERCENTILE_CONT for every single row (1.7M times = crash)
-- Temp tables exist only for this session and auto-drop after

DROP TABLE IF EXISTS temp_bureau_medians;

CREATE TEMP TABLE temp_bureau_medians AS
SELECT
    -- Calculate each median once across the full column
    -- Only non-null, non-empty values are included
    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY amt_credit_sum::NUMERIC
    ) AS median_credit_sum,

    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY amt_credit_sum_debt::NUMERIC
    ) AS median_credit_sum_debt,

    PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY amt_credit_sum_limit::NUMERIC
    ) AS median_credit_sum_limit

FROM staging.bureau
WHERE amt_credit_sum IS NOT NULL AND amt_credit_sum != ''
  AND amt_credit_sum_debt IS NOT NULL AND amt_credit_sum_debt != ''
  AND amt_credit_sum_limit IS NOT NULL AND amt_credit_sum_limit != '';

-- Quick sanity check — confirm medians calculated correctly
-- You should see three non-null numeric values
SELECT * FROM temp_bureau_medians;

-- ── STEP 2: Build clean.bureau using pre-calculated medians ─
DROP TABLE IF EXISTS clean.bureau;

CREATE TABLE clean.bureau AS
SELECT
    -- ── JOIN KEYS ──────────────────────────────────────────
    -- Cast TEXT to BIGINT — confirmed safe (no 0.0 formatting)
    sk_id_curr::BIGINT AS sk_id_curr,
    sk_id_bureau::BIGINT AS sk_id_bureau,

    -- ── CATEGORICAL COLUMNS ────────────────────────────────
    credit_active::VARCHAR(50) AS credit_active,
    credit_currency::VARCHAR(20) AS credit_currency,
    credit_type::VARCHAR(100) AS credit_type,

    -- ── DAY OFFSET COLUMNS ─────────────────────────────────
    -- Whole number day offsets, always negative or zero
    -- Confirmed no nulls in profiling
    days_credit::BIGINT AS days_credit,
    credit_day_overdue::BIGINT AS credit_day_overdue,
    days_credit_update::BIGINT AS days_credit_update,

    -- days_credit_enddate: scheduled end date of credit
    -- 6.1% null — left as NULL deliberately
    -- NULLIF converts empty strings '' to true SQL NULL
    -- before casting, preventing cast errors on empty text
    NULLIF(TRIM(days_credit_enddate), '')::NUMERIC(14,3)
        AS days_credit_enddate,

    -- days_enddate_fact: actual end date once credit closed
    -- Confirmed structural: Active loans are 99.7% null here
    -- because they haven't ended yet — NULL means "still open"
    NULLIF(TRIM(days_enddate_fact), '')::NUMERIC(14,3)
        AS days_enddate_fact,

    -- ── COUNT COLUMNS ──────────────────────────────────────
    cnt_credit_prolong::BIGINT AS cnt_credit_prolong,

    -- ── MONETARY COLUMNS ───────────────────────────────────
    -- amt_credit_sum: total credit amount, 13 nulls only
    -- Cross join to temp table brings median into every row
    -- as a single fixed scalar — no repeated subquery execution
    COALESCE(
        NULLIF(TRIM(b.amt_credit_sum), '')::NUMERIC(14,3),
        m.median_credit_sum::NUMERIC(14,3)
    ) AS amt_credit_sum,

    -- amt_credit_sum_debt: remaining debt, 15% null
    COALESCE(
        NULLIF(TRIM(b.amt_credit_sum_debt), '')::NUMERIC(14,3),
        m.median_credit_sum_debt::NUMERIC(14,3)
    ) AS amt_credit_sum_debt,

    -- amt_credit_sum_limit: credit limit, 34.5% null
    COALESCE(
        NULLIF(TRIM(b.amt_credit_sum_limit), '')::NUMERIC(14,3),
        m.median_credit_sum_limit::NUMERIC(14,3)
    ) AS amt_credit_sum_limit,

    -- amt_credit_sum_overdue: current overdue amount, no nulls
    NULLIF(TRIM(b.amt_credit_sum_overdue), '')::NUMERIC(14,3)
        AS amt_credit_sum_overdue,

    -- amt_credit_max_overdue: highest overdue ever recorded
    -- 65.5% null — fill with 0 (no overdue recorded = zero)
    COALESCE(
        NULLIF(TRIM(b.amt_credit_max_overdue), '')::NUMERIC(14,3),
        0
    ) AS amt_credit_max_overdue

    -- amt_annuity intentionally omitted — dropped per decision 2

FROM staging.bureau b
-- CROSS JOIN brings the single median row into every bureau row
-- This is safe because temp_bureau_medians has exactly 1 row
CROSS JOIN temp_bureau_medians m;

-- ============================================================
-- VERIFICATION QUERIES — run after CTAS completes
-- ============================================================

-- 1. Row count must match staging exactly (1,716,428)
SELECT COUNT(*) AS clean_row_count FROM clean.bureau;

-- 2. Confirm days_enddate_fact structural null pattern holds
SELECT
    credit_active,
    COUNT(*) AS total_records,
    COUNT(*) FILTER (WHERE days_enddate_fact IS NULL) AS null_enddate_fact
FROM clean.bureau
GROUP BY credit_active
ORDER BY total_records DESC;

-- 3. Confirm null imputation worked — all three should return 0
SELECT
    COUNT(*) FILTER (WHERE amt_credit_max_overdue IS NULL) AS null_max_overdue,
    COUNT(*) FILTER (WHERE amt_credit_sum_debt IS NULL) AS null_sum_debt,
    COUNT(*) FILTER (WHERE amt_credit_sum IS NULL) AS null_credit_sum
FROM clean.bureau;

-- 4. Confirm type casting worked — arithmetic on these columns
--    would fail if they were still stored as TEXT
SELECT
    MIN(days_credit) AS min_days_credit,
    MAX(days_credit) AS max_days_credit,
    ROUND(AVG(amt_credit_sum)::NUMERIC, 2) AS avg_credit_sum
FROM clean.bureau;