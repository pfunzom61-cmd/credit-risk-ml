
Data profiling: The below SQL queries give show an understanding of the application_train table, which is the main table containing applicant information and the target variable (default or not). This profiling will help identify data quality issues, understand variable distributions, and validate business logic before you start modeling. Each step is explained in detail to guide you through the process.

-- STEP 1: Total row count
-- We need to know the population size 
SELECT COUNT(*) AS total_rows
FROM staging.application_train;

-- STEP 2: Primary key uniqueness check
-- sk_id_curr is the unique applicant ID
-- Every other table joins to this — if it has duplicates, every downstream join will multiply rows and corrupt your model
-- If duplicate_keys = 0, then sk_id_curr is a perfect primary key
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT sk_id_curr) AS unique_applicants,
       COUNT(*) - COUNT(DISTINCT sk_id_curr) AS duplicate_keys
FROM staging.application_train;


-- STEP 4: Target variable distribution
-- TARGET = 0 means repaid, TARGET = 1 means defaulted
-- In credit risk, defaults are always rare (called class imbalance)
-- If 90% of rows are 0, your model can cheat by always predicting 0


SELECT
    target,
    COUNT(*) AS applicant_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM staging.application_train
GROUP BY target
ORDER BY target;

-- STEP 5: Business logic validation
-- days_birth should ALWAYS be negative (days before application), a psoitive vale is impossible and indicates a data error

SELECT
    -- Check for impossible birth dates (should be zero)
    SUM(CASE WHEN days_birth > 0 THEN 1 ELSE 0 END) AS impossible_birth_dates,
    
    
    -- No one under 18 gets a loan, no one over 100 is realistic
    MIN(ROUND(days_birth / -365.0, 1)) AS youngest_applicant_years,
    MAX(ROUND(days_birth / -365.0, 1)) AS oldest_applicant_years,
    
    -- days_employed = 365243 is a known bad sentinel value
    -- It means "not employed" was coded as a huge number instead of NULL
    SUM(CASE WHEN days_employed = 365243 THEN 1 ELSE 0 END) AS sentinel_employed_count,
    
    -- Check for impossible employment (positive days_employed = future employment)
    SUM(CASE WHEN days_employed > 0 
             AND days_employed != 365243 THEN 1 ELSE 0 END) AS impossible_employed
FROM staging.application_train;


-- STEP 6: Distribution check on external credit scores
-- ext_source_1/2/3 are external bureau scores — likely the strongest predictors of default in this dataset
-- understand their range and whether the values are sensible. A credit score should be bounded — not negative, not above 1.0 in this dataset
-- We also check the average by target group to see if these scores already separate defaulters from non-defaulters before any modeling

SELECT
    -- Basic range check for each source
    ROUND(MIN(ext_source_1)::NUMERIC, 3) AS ext1_min,
    ROUND(MAX(ext_source_1)::NUMERIC, 3) AS ext1_max,
    ROUND(AVG(ext_source_1)::NUMERIC, 3) AS ext1_avg,

    ROUND(MIN(ext_source_2)::NUMERIC, 3) AS ext2_min,
    ROUND(MAX(ext_source_2)::NUMERIC, 3) AS ext2_max,
    ROUND(AVG(ext_source_2)::NUMERIC, 3) AS ext2_avg,

    ROUND(MIN(ext_source_3)::NUMERIC, 3) AS ext3_min,
    ROUND(MAX(ext_source_3)::NUMERIC, 3) AS ext3_max,
    ROUND(AVG(ext_source_3)::NUMERIC, 3) AS ext3_avg

FROM staging.application_train;


-- STEP 7: Do credit scores already separate defaulters from non-defaulters?
-- first real EDA hypothesis test
-- HYPOTHESIS: Applicants who defaulted will have lower external credit scores
-- If the averages differ meaningfully between target=0 and target=1, these columns will be strong model features
-- This is exactly the analysis a credit risk analyst presents to a committee

SELECT
    target,
    COUNT(*) AS applicant_count,
    ROUND(AVG(ext_source_1)::NUMERIC, 3) AS avg_ext_source_1,
    ROUND(AVG(ext_source_2)::NUMERIC, 3) AS avg_ext_source_2,
    ROUND(AVG(ext_source_3)::NUMERIC, 3) AS avg_ext_source_3
FROM staging.application_train
GROUP BY target
ORDER BY target;


-- STEP 8: Satellite table row counts and join key validation
-- These tables all link to application_train via sk_id_curr
-- I need to confirm:
-- 1. How many rows each table has
-- 2. How many unique applicants appear (one applicant can have many rows)
-- 3. Whether all sk_id_curr values in satellite tables exist in application_train
--    (orphaned records = join keys that don't match the main table (application_train) = data integrity failure)

-- Bureau table: external credit history per applicant
SELECT
    'bureau' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT sk_id_curr) AS unique_applicants,
    -- How many loans on average per applicant in the bureau
    ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT sk_id_curr), 1) AS avg_records_per_applicant
FROM staging.bureau

UNION ALL

-- Previous applications at Home Credit
SELECT
    'previous_application',
    COUNT(*),
    COUNT(DISTINCT sk_id_curr),
    ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT sk_id_curr), 1)
FROM staging.previous_application

UNION ALL

-- Monthly credit card balance snapshots
SELECT
    'credit_card_balance',
    COUNT(*),
    COUNT(DISTINCT sk_id_curr),
    ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT sk_id_curr), 1)
FROM staging.credit_card_balance

UNION ALL

-- Installment payment history
SELECT
    'installments_payments',
    COUNT(*),
    COUNT(DISTINCT sk_id_curr),
    ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT sk_id_curr), 1)
FROM staging.installments_payments

UNION ALL

-- POS and cash loan monthly balance
SELECT
    'pos_cash_balance',
    COUNT(*),
    COUNT(DISTINCT sk_id_curr),
    ROUND(COUNT(*)::NUMERIC / COUNT(DISTINCT sk_id_curr), 1)
FROM staging.pos_cash_balance;


-- STEP 9: Orphan record check
-- We cast sk_id_curr to BIGINT in satellite tables because I ingested as TEXT 


SELECT
    'bureau orphans' AS check_name,
    COUNT(*) AS orphaned_records
FROM staging.bureau b
WHERE NOT EXISTS (
    SELECT 1 FROM staging.application_train a
    WHERE a.sk_id_curr = b.sk_id_curr::BIGINT
)

UNION ALL

SELECT
    'previous_application orphans',
    COUNT(*)
FROM staging.previous_application p
WHERE NOT EXISTS (
    SELECT 1 FROM staging.application_train a
    WHERE a.sk_id_curr = p.sk_id_curr::BIGINT
)

UNION ALL

SELECT
    'installments_payments orphans',
    COUNT(*)
FROM staging.installments_payments i
WHERE NOT EXISTS (
    SELECT 1 FROM staging.application_train a
    WHERE a.sk_id_curr = i.sk_id_curr::BIGINT
);