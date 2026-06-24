-- ============================================================
-- SCRIPT: 01_clean_application_train.sql
-- PURPOSE: Build clean.application_train from staging.application_train
-- LAYER: Clean (business logic applied, decisions documented)
-- ============================================================
-- DECISIONS MADE IN THIS SCRIPT (documented for audit trail):
--
-- 1. days_employed sentinel value (365243) is replaced with NULL
--    Reason: 365243 is a fake placeholder meaning "not employed" , an error/business decision from data ollection and capture
--    disguised as a number. Verified affecting 55,374 rows.
--
-- 2. 43 property-related columns collapsed into 14 FLAG COLUMNS,
--    one per property "family" (e.g. flag_has_commonarea_data).
--    Reason: initially assumed ONE flag would work for all 43 columns, but verification (NUM_NONNULLS test across families)
--    proved this false — applicants showed a wide scattered spread from 0 to 43 populated columns, not a clean 0-or-all split.
--    Re-tested WITHIN a single family (commonarea_avg/mode/medi)
--    and confirmed those 3 always move together (everyone landed
--    at exactly 0 or exactly 3, never 1 or 2). This means each
--    property family is independently present/absent per applicant
--    (e.g. a house may have elevator data but no basement data),
--    so one flag per family preserves real signal that a single
--    universal flag would have destroyed. Each flag checks the
--    family's _avg column as the representative proxy for that
--    family's _avg/_mode/_medi trio.
--
-- 3. amt_req_credit_bureau_* (6 columns, all 13.50% null together)
--    filled with 0, not NULL. Reason: missing here means "no
--    bureau inquiry was made" — a real, knowable fact, not unknown.
--
-- 4. occupation_type (31.35% null) imputed with 'Unknown' category.
--    Reason: categorical column, too much signal to drop, but
--    no numeric mean/median is possible for text categories.
--
-- 5. ext_source_1 and ext_source_3 left as NULL (56.38% / 19.83%).
--    Reason: these are proven strong predictors of default. Tree
--    based models (Random Forest, XGBoost) handle NULL natively.
--    Forcing a mean value here would weaken a feature we already
--    confirmed is valuable. Decision revisited at modeling stage.
--
-- 6. Small-scale nulls (<1%) imputed with median: amt_annuity,
--    amt_goods_price, cnt_fam_members, days_last_phone_change,
--    def_30_cnt_social_circle, def_60_cnt_social_circle,
--    obs_30_cnt_social_circle, obs_60_cnt_social_circle,
--    ext_source_2. Reason: at this scale, imputation carries no
--    meaningful integrity risk.
--
-- 7. name_type_suite (0.42% null, categorical) imputed with the
--    most frequent category, since median doesn't apply to text.
--
-- 8. totalarea_mode has no _avg/_mode/_medi trio (it is standalone,
--    only a _mode variant exists). Treated as its own single-column
--    family: flag_has_totalarea_data.
--
-- 9. Categorical property columns (housetype_mode, wallsmaterial_mode,
--    emergencystate_mode, fondkapremont_mode) are dropped. Their
--    presence/absence is already captured by the numeric family
--    flags above (e.g. housetype_mode is null exactly when the
--    apartments family is null), so they add no new signal once
--    the flags exist.
-- ============================================================

-- I use CREATE TABLE AS SELECT (CTAS). This builds a brand new
-- physical table in the clean schema in a single pass, reading
-- from staging and never modifying the original staging data.
-- This is the safest professional pattern: if this script has a
-- bug, staging.application_train is completely untouched and we
-- can simply fix the script and re-run it.

DROP TABLE IF EXISTS clean.application_train;

CREATE TABLE clean.application_train AS
SELECT
    -- ── IDENTIFIERS AND TARGET (untouched, already correct) ──
    sk_id_curr,
    target,

    -- ── CATEGORICAL APPLICANT ATTRIBUTES (untouched) ──
    name_contract_type,
    code_gender,
    flag_own_car,
    flag_own_realty,
    cnt_children,

    -- name_type_suite: categorical, 0.42% null
    -- MODE() WITHIN GROUP finds the single most frequently occurring value
    -- This is the categorical equivalent of "median" for text columns
    COALESCE(
        name_type_suite,
        (SELECT MODE() WITHIN GROUP (ORDER BY name_type_suite)
         FROM staging.application_train)
    ) AS name_type_suite,

    name_income_type,
    name_education_type,
    name_family_status,
    name_housing_type,

    -- ── FINANCIAL COLUMNS ──
    amt_income_total,
    amt_credit,

    -- amt_annuity: <1% null, safe for median imputation
    -- COALESCE returns the first non-null value in the list
    -- so if amt_annuity is NULL, it substitutes the table-wide median
    COALESCE(
        amt_annuity,
        (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amt_annuity)
         FROM staging.application_train)
    ) AS amt_annuity,

    -- amt_goods_price: <1% null, same median imputation logic
    COALESCE(
        amt_goods_price,
        (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amt_goods_price)
         FROM staging.application_train)
    ) AS amt_goods_price,

    -- ── DEMOGRAPHIC / TIME COLUMNS (untouched, already clean) ──
    region_population_relative,
    days_birth,

    -- days_employed: replace the 365243 sentinel value with NULL
    -- CASE WHEN checks the condition row by row; if it matches,
    -- we substitute NULL instead of the fake placeholder number
    CASE 
        WHEN days_employed = 365243 THEN NULL 
        ELSE days_employed 
    END AS days_employed,

    days_registration,
    days_id_publish,

    -- ── FLAG COLUMNS (untouched) ──
    flag_mobil,
    flag_emp_phone,
    flag_work_phone,
    flag_cont_mobile,
    flag_phone,
    flag_email,

    -- occupation_type: categorical, 31.35% null, impute 'Unknown'
    -- rather than dropping a column with this much signal potential
    COALESCE(occupation_type, 'Unknown') AS occupation_type,

    -- cnt_fam_members: <1% null, median imputation
    COALESCE(
        cnt_fam_members,
        (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cnt_fam_members)
         FROM staging.application_train)
    ) AS cnt_fam_members,

    -- ── REGION / ORGANIZATION COLUMNS (untouched) ──
    region_rating_client,
    region_rating_client_w_city,
    weekday_appr_process_start,
    hour_appr_process_start,
    reg_region_not_live_region,
    reg_region_not_work_region,
    live_region_not_work_region,
    reg_city_not_live_city,
    reg_city_not_work_city,
    live_city_not_work_city,
    organization_type,

    -- ── EXTERNAL CREDIT SCORES ──
    -- ext_source_1: left as NULL deliberately. 56.38% missing.
    -- Proven predictor of default — imputing would weaken signal.
    -- Tree-based models handle NULL natively at modeling stage.
    ext_source_1,

    -- ext_source_2: only 0.21% null, safe for median imputation
    COALESCE(
        ext_source_2,
        (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ext_source_2)
         FROM staging.application_train)
    ) AS ext_source_2,

    -- ext_source_3: left as NULL deliberately, same reasoning as ext_source_1
    ext_source_3,

    -- ════════════════════════════════════════════════════════
    -- PROPERTY FLAGS — 14 columns, one per property family
    -- Each replaces that family's _avg/_mode/_medi trio (verified
    -- to move together within-family). Checking the _avg column
    -- of each family is a reliable proxy for that family's trio.
    -- ════════════════════════════════════════════════════════

    CASE WHEN apartments_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_apartments_data,

    CASE WHEN basementarea_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_basementarea_data,

    CASE WHEN years_beginexpluatation_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_years_beginexpluatation_data,

    CASE WHEN years_build_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_years_build_data,

    CASE WHEN commonarea_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_commonarea_data,

    CASE WHEN elevators_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_elevators_data,

    CASE WHEN entrances_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_entrances_data,

    CASE WHEN floorsmax_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_floorsmax_data,

    CASE WHEN floorsmin_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_floorsmin_data,

    CASE WHEN landarea_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_landarea_data,

    CASE WHEN livingapartments_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_livingapartments_data,

    CASE WHEN livingarea_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_livingarea_data,

    CASE WHEN nonlivingapartments_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_nonlivingapartments_data,

    CASE WHEN nonlivingarea_avg IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_nonlivingarea_data,

    -- totalarea_mode has no _avg/_medi siblings, it stands alone
    CASE WHEN totalarea_mode IS NOT NULL THEN 1 ELSE 0 END
        AS flag_has_totalarea_data,

    -- ── SOCIAL CIRCLE COLUMNS: <1% null, median imputation ──
    COALESCE(
        obs_30_cnt_social_circle,
        (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY obs_30_cnt_social_circle)
         FROM staging.application_train)
    ) AS obs_30_cnt_social_circle,

    COALESCE(
        def_30_cnt_social_circle,
        (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY def_30_cnt_social_circle)
         FROM staging.application_train)
    ) AS def_30_cnt_social_circle,

    COALESCE(
        obs_60_cnt_social_circle,
        (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY obs_60_cnt_social_circle)
         FROM staging.application_train)
    ) AS obs_60_cnt_social_circle,

    COALESCE(
        def_60_cnt_social_circle,
        (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY def_60_cnt_social_circle)
         FROM staging.application_train)
    ) AS def_60_cnt_social_circle,

    -- days_last_phone_change: <1% null, median imputation
    COALESCE(
        days_last_phone_change,
        (SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_last_phone_change)
         FROM staging.application_train)
    ) AS days_last_phone_change,

    -- ── DOCUMENT FLAGS (untouched, no nulls) ──
    flag_document_2, flag_document_3, flag_document_4, flag_document_5,
    flag_document_6, flag_document_7, flag_document_8, flag_document_9,
    flag_document_10, flag_document_11, flag_document_12, flag_document_13,
    flag_document_14, flag_document_15, flag_document_16, flag_document_17,
    flag_document_18, flag_document_19, flag_document_20, flag_document_21,

    -- ── BUREAU INQUIRY COLUMNS ──
    -- All 6 columns fill with 0, not NULL. Missing here means
    -- "no inquiry was made" — a real fact, not an unknown value.
    COALESCE(amt_req_credit_bureau_hour, 0) AS amt_req_credit_bureau_hour,
    COALESCE(amt_req_credit_bureau_day, 0) AS amt_req_credit_bureau_day,
    COALESCE(amt_req_credit_bureau_week, 0) AS amt_req_credit_bureau_week,
    COALESCE(amt_req_credit_bureau_mon, 0) AS amt_req_credit_bureau_mon,
    COALESCE(amt_req_credit_bureau_qrt, 0) AS amt_req_credit_bureau_qrt,
    COALESCE(amt_req_credit_bureau_year, 0) AS amt_req_credit_bureau_year

FROM staging.application_train;

-- ============================================================
-- VERIFICATION QUERIES — run these after the CTAS completes
-- ============================================================

-- 1. Confirm row count matches staging exactly (CTAS should never
--    add or remove rows — only columns and values change)
SELECT COUNT(*) AS clean_row_count FROM clean.application_train;

-- 2. Confirm each property flag matches its known staging count.
--    commonarea should show 0 -> 214,865 and 1 -> 92,646 (verified
--    earlier). Use this same pattern to spot-check the others.
SELECT
    flag_has_commonarea_data,
    COUNT(*) AS applicant_count
FROM clean.application_train
GROUP BY flag_has_commonarea_data;

-- 3. Confirm the sentinel value is gone — this should return 0
SELECT COUNT(*) AS remaining_sentinel_values
FROM clean.application_train
WHERE days_employed = 365243;

-- 4. Confirm no nulls remain in columns we explicitly imputed
SELECT 
    COUNT(*) FILTER (WHERE amt_annuity IS NULL) AS null_amt_annuity,
    COUNT(*) FILTER (WHERE occupation_type IS NULL) AS null_occupation,
    COUNT(*) FILTER (WHERE amt_req_credit_bureau_hour IS NULL) AS null_bureau_hour
FROM clean.application_train;

-- 5. Confirm the 14 flag columns exist and contain only 0/1 values
SELECT
    MIN(flag_has_apartments_data) AS min_val,
    MAX(flag_has_apartments_data) AS max_val
FROM clean.application_train;