# Data Warehouse Schema Design

## Overview

The healthcare claims data warehouse uses a **Star Schema** optimized for analytical queries from BI tools like Power BI and Tableau.

Star schema was chosen over 3NF because:
- Analytical queries (aggregations, GROUP BY) are significantly faster
- Simpler SQL joins for business users and dashboards
- Compatible with columnar storage formats (Parquet)

---

## Entity Relationship Diagram

```
                    ┌─────────────────┐
                    │   dim_date      │
                    │─────────────────│
                    │ date_key (PK)   │
                    │ full_date       │
                    │ year            │
                    │ quarter         │
                    │ month           │
                    │ week            │
                    │ day_of_week     │
                    │ is_weekend      │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│  dim_provider   │          │          │   dim_payer     │
│─────────────────│          │          │─────────────────│
│ provider_key(PK)│          │          │ payer_key (PK)  │
│ provider_npi    │          │          │ payer_name      │
│ provider_name   ├──────────┴──────────┤ payer_type      │
│ specialty       │   fact_claims       │ created_at      │
│ created_at      │─────────────────────│                 │
└─────────────────┤ claim_id (PK)       └─────────────────┘
                  │ patient_id
                  │ provider_npi (FK)
                  │ payer_name (FK)
                  │ service_date (FK)
                  │ icd10_code
                  │ cpt_code
                  │ claim_amount
                  │ approved_amount
                  │ denial_code
                  │ processing_status
                  │ anomaly_score
                  │ approval_rate
                  │ days_to_submission
                  │ transformed_at
                  │ loaded_at
                  └─────────────────────
```

---

## Table Definitions

### fact_claims (primary fact table)

| Column | Type | Description |
|--------|------|-------------|
| claim_id | VARCHAR(20) PK | Unique claim identifier (CLM-XXXXXXXX) |
| patient_id | VARCHAR(20) | Patient identifier |
| provider_npi | VARCHAR(10) FK | Provider NPI → dim_provider |
| payer_name | VARCHAR(100) | Insurance payer name |
| service_date | DATE | Date services were rendered |
| icd10_code | VARCHAR(10) | ICD-10-CM diagnosis code |
| cpt_code | VARCHAR(10) | CPT procedure code |
| claim_amount | NUMERIC(14,2) | Total billed amount |
| approved_amount | NUMERIC(14,2) | Amount approved for payment |
| denial_code | VARCHAR(10) | Denial reason code (CARC/RARC) |
| processing_status | VARCHAR(25) | APPROVED / DENIED / PENDING / PARTIALLY_APPROVED |
| anomaly_score | FLOAT | 0.0–1.0 outlier score (IQR-based) |
| approval_rate | FLOAT | approved_amount / claim_amount |
| days_to_submission | INT | Days between service and claim submission |
| transformed_at | TIMESTAMP | When the record was processed by ETL |
| loaded_at | TIMESTAMP | When the record was written to warehouse |

### dim_date

Auto-populated date dimension spanning 2020–2030.

### dim_provider

| Column | Type | Description |
|--------|------|-------------|
| provider_key | SERIAL PK | Surrogate key |
| provider_npi | VARCHAR(10) UNIQUE | National Provider Identifier |
| provider_name | VARCHAR(150) | Full name |
| specialty | VARCHAR(100) | Medical specialty |

### quarantine_claims

Holds records that failed data quality checks for manual review.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | Auto-increment |
| claim_id | VARCHAR(20) | Original claim ID (may be null) |
| quarantine_reason | VARCHAR(200) | Why it was rejected |
| raw_data | JSONB | Full original record |
| quarantined_at | TIMESTAMP | When it was quarantined |

### pipeline_runs (audit trail)

One row per pipeline execution for observability and debugging.

---

## Key Design Decisions

1. **Upsert over truncate-reload** — Ensures idempotency; re-running the pipeline on the same date is safe
2. **Quarantine table** — Invalid records are never dropped; they're preserved for manual review and lineage
3. **anomaly_score** — Stored in fact table so Power BI dashboards can filter/highlight outliers without re-computation
4. **approval_rate** — Pre-computed derived metric avoids repeated division in every dashboard query
5. **JSONB for quarantine** — Stores full raw record for debugging without a rigid schema
