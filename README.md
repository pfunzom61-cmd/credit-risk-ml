# Credit Risk ML Pipeline
 
An end-to-end credit risk prediction system built using industry-grade data engineering and machine learning practices. The goal is to estimate the probability that a loan applicant will default, using structured financial history from multiple relational data sources.
 
Built to reflect real workflows used in credit risk analytics teams at financial institutions.
 
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
│    Clean layer      │  ← type enforcement, sentinel fixes, null handling
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
 
## Project Objectives
 
- Relational data ingestion from heterogeneous CSV sources
- Schema-layered warehouse design (staging → clean → features)
- SQL-first data profiling and feature engineering
- Predictive modeling for binary default classification
- Model interpretability using SHAP for regulated decision support
- Interactive deployment via Streamlit
---
 
## Key Profiling Findings
 
Documented during the profiling phase before any cleaning or modeling:
 
| Finding | Detail |
|---------|--------|
| Row count | 307,511 applicants in spine table |
| Primary key | Unique — no duplicates in `sk_id_curr` |
| Class imbalance | 91.93% repaid (0) vs 8.07% defaulted (1) |
| High-null columns | 48 columns above 40% null — mostly property features |
| Sentinel value | `days_employed = 365243` affects 55,374 rows (pensioners/unemployed) |
| Credit score separation | `ext_source_2` avg: 0.523 (repaid) vs 0.411 (defaulted) |
| Orphaned records | Satellite tables contain records not present in spine — inner joins enforced |
 
These findings directly inform clean layer design decisions.
 
---
 
## Dataset
 
**Source**: [Home Credit Default Risk — Kaggle](https://www.kaggle.com/c/home-credit-default-risk)
 
| Table | Rows | Description |
|-------|------|-------------|
| application_train | 307,511 | Spine table — one row per applicant, contains TARGET |
| bureau | 1,716,428 | External credit bureau history |
| bureau_balance | — | Monthly bureau balance snapshots |
| previous_application | 1,670,214 | Prior loan applications at Home Credit |
| installments_payments | 13,605,401 | Payment-level installment history |
| pos_cash_balance | 10,001,358 | Monthly POS and cash loan snapshots |
| credit_card_balance | 3,840,312 | Monthly credit card balance history |
| application_test | — | Holdout set for final prediction |
 
**Target variable**: `TARGET` — 0 = repaid, 1 = defaulted
 
---
 
## Repository Structure
 
```
credit-risk-ml/
│
├── app/                        # Streamlit prediction interface (in progress)
│
├── Config/
│   └── database.yaml           # DB credentials — gitignored
│
├── Data/
│   ├── raw/                    # Source CSVs — gitignored
│   └── processed/              # Engineered feature tables
│
├── notebooks/
│   ├── 01_data_ingestion.ipynb
│   └── 02_data_profiling.ipynb
│
├── sql/
│   ├── profiling/
│   │   └── 01_profile_application_train.sql
│   └── staging/
│       └── create_application_train.sql
│
├── src/
│   ├── data/
│   │   ├── load_raw_to_staging.py
│   │   └── stage_application.py
│   ├── features/               # Feature engineering scripts (in progress)
│   └── models/                 # Model training scripts (in progress)
│
├── .gitignore
├── README.md
└── requirements.txt
```
 
---
 
## Modeling Approach
 
Three models are implemented in order of complexity:
 
**Logistic Regression** — interpretable baseline. Outputs calibrated default probabilities. Establishes a performance floor and validates that features carry signal.
 
**Random Forest** — nonlinear ensemble benchmark. Handles feature interactions without manual engineering. Used to assess feature importance stability.
 
**XGBoost** — gradient boosting production candidate. Regularised, handles class imbalance natively, strong ROC-AUC performance on tabular credit data.
 
**Evaluation metric**: ROC-AUC — chosen because accuracy is misleading on a 92/8 class split. A model predicting "repaid" for every applicant achieves 92% accuracy while catching zero defaulters.
 
**Business framing**: false negatives (missed defaulters) carry higher cost than false positives (rejected good applicants). Threshold tuning reflects this asymmetry.
 
---
 
## Explainability
 
SHAP (SHapley Additive exPlanations) provides:
 
- Per-applicant feature contribution breakdown
- Global feature influence ranking
- Decision transparency suitable for model governance in regulated environments
---
 
## Deployment
 
A Streamlit application will allow:
 
- Manual applicant input
- Real-time default probability output
- SHAP-based feature contribution display
- Model transparency layer for decision support
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
git clone https://github.com/your-username/credit-risk-ml.git
 
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
| Clean layer transformations | 🔄 In progress |
| Feature engineering | ⏳ Pending |
| Model training | ⏳ Pending |
| SHAP explainability | ⏳ Pending |
| Streamlit deployment | ⏳ Pending |