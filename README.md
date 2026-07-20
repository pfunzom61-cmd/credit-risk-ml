# Credit Risk ML Pipeline

Every year, banks lose billions of rands to loan defaults — applicants who borrow money and never repay it. The challenge is identifying these applicants *before* approving their loan, using only the information available at the time of application.

This project builds an end-to-end credit risk prediction system that estimates the probability a loan applicant will default, using structured financial history from multiple relational data sources. It is designed to reflect the actual workflows used by credit risk analytics teams at financial institutions — not a tutorial project, but a production-oriented pipeline built with the same layered architecture, SQL-first thinking, and documented decision-making that real data teams use.

---

## The Problem This Solves

A bank has two types of mistakes it can make:

- **False positive** — rejecting a good customer who would have repaid. Cost: lost interest income.
- **False negative** — approving a bad customer who defaults. Cost: the entire loan principal, potentially never recovered.

Banks strongly prefer the first type of mistake over the second. This asymmetry directly shapes every modeling and threshold decision in this pipeline.

---

## Pipeline Architecture

```
Raw CSV files (8 tables)
        │
        ▼
┌─────────────────────┐
│   Staging layer     │  ← TEXT ingestion, no transforms, schema-isolated  ✓
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  Profiling layer    │  ← null analysis, PK validation, class imbalance   ✓
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│    Clean layer      │  ← type enforcement, sentinel fixes, null handling  ✓ (in progress)
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│ Feature engineering │  ← SQL aggregations from bureau + payment tables
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│     Modeling        │  ← Logistic Regression, Random Forest, XGBoost
└─────────────────────┘
        │
   ┌────┴────┐
   ▼         ▼
┌──────┐  ┌──────────┐
│ SHAP │  │Streamlit │
└──────┘  └──────────┘
```

---

## Key Profiling Findings

Documented before any cleaning or modeling — every cleaning decision below traces back to one of these findings:

| Finding | Detail |
|---------|--------|
| Row count | 307,511 applicants in spine table |
| Primary key | Unique — no duplicates in `sk_id_curr` |
| Class imbalance | 91.93% repaid (0) vs 8.07% defaulted (1) — accuracy is a misleading metric here |
| High-null columns | 48 columns above 40% null — mostly property features, handled with per-family flags |
| Sentinel value | `days_employed = 365243` affects 55,374 rows — fake placeholder for pensioners/unemployed |
| Credit score separation | `ext_source_2` avg: 0.523 (repaid) vs 0.411 (defaulted) — confirmed strong predictor |
| Orphaned records | Satellite tables contain records not in spine — INNER JOINs enforced at feature layer |
| Staging type mismatch | `application_train` has enforced schema; all satellite tables ingested as pure TEXT |

---

## Key Clean Layer Decisions

Each decision is documented in the relevant SQL script with business justification:

| Table | Decision | Reason |
|-------|----------|--------|
| `application_train` | `days_employed = 365243` → NULL | Sentinel value for unemployed/pensioners — not a real number |
| `application_train` | 43 property columns → 14 flag columns | Verified per-family null clustering; one flag per property type preserves real signal |
| `application_train` | `occupation_type` nulls → 'Unknown' | Categorical column — no mean/median possible; too much signal to drop |
| `application_train` | Bureau inquiry nulls → 0 | Missing means no inquiry was made — zero is the real value, not unknown |
| `bureau` | All columns cast TEXT → BIGINT/NUMERIC/VARCHAR | Satellite tables ingested as TEXT; type alignment required before any aggregation |
| `bureau` | `amt_annuity` → dropped | 71.5% null, no clean pattern by loan type, weak diagnostic signal against TARGET |
| `bureau` | `days_enddate_fact` → left as NULL | Confirmed structural: Active loans are 99.7% null because they haven't ended yet |
| `bureau` | `amt_credit_max_overdue` → 0 | No clean overdue-status pattern found; zero is safest honest assumption |

---

## Dataset

**Source**: [Home Credit Default Risk — Kaggle](https://www.kaggle.com/c/home-credit-default-risk)

| Table | Rows | Description |
|-------|------|-------------|
| application_train | 307,511 | Spine table — one row per applicant, contains TARGET |
| bureau | 1,716,428 | External credit bureau history across all lenders |
| bureau_balance | — | Monthly bureau balance snapshots |
| previous_application | 1,670,214 | Prior loan applications at Home Credit |
| installments_payments | 13,605,401 | Payment-level installment history |
| pos_cash_balance | 10,001,358 | Monthly POS and cash loan snapshots |
| credit_card_balance | 3,840,312 | Monthly credit card balance history |
| application_test | — | Holdout set for final predictions |

**Target variable**: `TARGET` — 0 = repaid, 1 = defaulted

---

## Repository Structure

```
credit-risk-ml/
│
├── app/                             # Streamlit prediction interface (in progress)
│
├── Config/
│   └── database.yaml                # DB credentials — gitignored
│
├── Data/
│   ├── raw/                         # Source CSVs — gitignored
│   └── processed/                   # Engineered feature tables
│
├── notebooks/
│   ├── 01_data_ingestion.ipynb
│   └── 02_data_profiling.ipynb
│
├── sql/
│   ├── staging/
│   │   └── create_application_train.sql
│   ├── profiling/
│   │   ├── 01_profile_application_train.sql
│   │   └── 02_profile_bureau.sql
│   └── clean/
│       ├── 01_clean_application_train.sql
│       └── 02_clean_bureau.sql
│
├── src/
│   ├── data/
│   │   ├── load_raw_to_staging.py
│   │   └── stage_application.py
│   ├── features/                    # Feature engineering scripts (in progress)
│   └── models/                      # Model training scripts (in progress)
│
├── .gitignore
├── README.md
└── requirements.txt
```

---

## Modeling Approach

Three models implemented in order of complexity:

**Logistic Regression** — interpretable baseline. Outputs calibrated default probabilities. Establishes a performance floor and validates that features carry signal before introducing complexity.

**Random Forest** — nonlinear ensemble benchmark. Handles feature interactions without manual engineering. Used to assess feature importance stability across bootstrap samples.

**XGBoost** — gradient boosting production candidate. Regularised, handles class imbalance natively, strong ROC-AUC performance on tabular credit data in production environments.

**Evaluation metric**: ROC-AUC — chosen because accuracy is misleading on a 92/8 class split. A model predicting "repaid" for every applicant achieves 92% accuracy while catching zero defaulters.

**Business framing**: false negatives (missed defaulters) carry higher cost than false positives (rejected good customers). Threshold tuning reflects this asymmetry explicitly.

---

## Explainability

SHAP (SHapley Additive exPlanations) provides:

- Per-applicant feature contribution breakdown — *why* was this person flagged as high risk?
- Global feature influence ranking — which signals matter most across all applicants?
- Decision transparency suitable for model governance in regulated financial environments

---

## Deployment

A Streamlit application will allow:

- Manual applicant input
- Real-time default probability output
- SHAP-based feature contribution display per prediction
- Model transparency layer for credit decision support

---

## Tech Stack

| Category | Tools |
|----------|-------|
| Languages | Python, SQL |
| Database | PostgreSQL, DBeaver |
| Data engineering | SQLAlchemy, psycopg2 |
| Machine learning | scikit-learn, XGBoost, SHAP |
| Visualisation | Matplotlib, Seaborn |
| Deployment | Streamlit |
| Version control | Git, GitHub |

---

## Reproducibility

```bash
# 1. Clone the repository
git clone https://github.com/pfunzom61-cmd/credit-risk-ml.git

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure database credentials
# Edit Config/database.yaml with your PostgreSQL connection details

# 4. Run ingestion pipeline
python src/data/load_raw_to_staging.py
```

---

## Project Status

| Phase | Status |
|-------|--------|
| PostgreSQL setup | ✅ Complete |
| Staging layer design | ✅ Complete |
| Ingestion pipeline | ✅ Complete |
| Data profiling & validation | ✅ Complete |
| Clean — application_train | ✅ Complete |
| Clean — bureau | ✅ Complete |
| Clean — remaining satellite tables | 🔄 In progress |
| Feature engineering | ⏳ Pending |
| Model training | ⏳ Pending |
| SHAP explainability | ⏳ Pending |
| Streamlit deployment | ⏳ Pending |