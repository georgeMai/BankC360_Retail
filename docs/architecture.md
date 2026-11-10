# Kiến trúc Hệ thống — Customer 360 Lakehouse

## 1. Tổng quan kiến trúc

```
┌──────────────────────────────────────────────────────────────────────┐
│                         SOURCE SYSTEMS                               │
│                                                                      │
│  ┌─────────────────────────┐    ┌────────────────────────────────┐   │
│  │     Oracle XE 21c       │    │         PostgreSQL 15          │   │
│  │   schema: core_banking  │    │      schema: card_crm          │   │
│  │                         │    │                                │   │
│  │  branch    (100 rows)   │    │  card         (~8,000 rows)   │   │
│  │  product    (30 rows)   │    │  card_txn    (~212k rows)     │   │
│  │  customer (10,000 rows) │    │  crm_interaction(10,000rows)  │   │
│  │  account  (30,000 rows) │    │  (12 tháng Jan–Dec 2025)      │   │
│  │  deposit   (5,000 rows) │    └───────────────┬────────────────┘   │
│  │  loan      (3,000 rows) │                    │                    │
│  │  txn_account (~1M rows) │                    │                    │
│  └─────────────┬───────────┘                    │                    │
└─────────────────┼──────────────────────────────── ┼───────────────────┘
                  │       JDBC (Spark JDBC Reader)   │
                  ▼                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  INGESTION — BRONZE LAYER                            │
│                                                                      │
│  Spark job: ingestion_jdbc.py   ← đọc config từ YAML (10 files)     │
│  Pattern: INSERT OVERWRITE partition(cob_dt)                         │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │         MinIO (S3-compatible)  +  Iceberg REST Catalog       │    │
│  │  bucket: lakehouse/bronze/                                   │    │
│  │  10 tables: core_branch, core_customer, ..., card_txn, ...  │    │
│  └──────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  TRANSFORMATION — SILVER LAYER                       │
│                                                                      │
│  Spark jobs: scd_type1.py / scd_type2.py / fact_txn.py              │
│  Pattern: MERGE INTO (dims) / overwritePartitions (facts)            │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │  Dimensions (SCD2): dim_customer, dim_account                │    │
│  │  Dimensions (SCD1): dim_branch, dim_product, dim_deposit,    │    │
│  │                     dim_loan, dim_card                       │    │
│  │  Facts:  fact_txn_account, fact_card_txn,                    │    │
│  │          fact_crm_interaction                                │    │
│  └──────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  ANALYTICS — GOLD LAYER                              │
│                                                                      │
│  Spark job: gold_job.py   ← đọc SQL từ YAML (10 configs)            │
│  Pattern: overwritePartitions (date-partitioned) hoặc full rebuild   │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐      │
│  │  mart360:  customer_360 (main), balance_summary,           │      │
│  │            card_summary, product_summary, txn_summary      │      │
│  │  segments: rfm_segment, churn_prediction,                  │      │
│  │            cross_sell_segment, campaign_target             │      │
│  │  analytics: mart_branch_monthly_summary                    │      │
│  └────────────────────────────────────────────────────────────┘      │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                          ┌───────┴────────┐
                          ▼                ▼
               ┌──────────────┐   ┌─────────────────┐
               │    Trino     │   │   Airflow DAGs   │
               │  Ad-hoc SQL  │   │  Orchestration   │
               │  < 10 giây   │   │  + ETL Flags     │
               └──────────────┘   └─────────────────┘
```

---

## 2. Luồng dữ liệu (Data Flow)

### Ingestion Schedule

Tất cả các DAGs dưới đây đều dùng `schedule_interval=None` — **trigger thủ công** (không chạy tự động theo lịch). Thứ tự trigger đúng:

```
[Manual]  bronze_initial_dag        (initial full-load, chạy 1 lần duy nhất)
          bronze_core_banking_dag   (incremental, Oracle 7 tables)
          bronze_card_crm_dag       (incremental, Postgres 3 tables)
                     │
                     │ (check ETL flags: bronze_initial + core_banking + card_crm = 'S')
                     ▼
[Manual]  silver_all_dag            (7 dims song song → 3 facts song song)
                     │
                     │ (check ETL flag: silver_all_dag = 'S')
                     ▼
[Manual]  gold_mart360_dag          (5 mart tables song song)
          gold_time_analytics_dag   (mart_branch_monthly_summary)
                     │
                     │ (check ETL flags: silver + mart360 = 'S')
                     ▼
[Manual]  gold_segmentation_dag     (rfm, churn, cross_sell → campaign_target)
                     │
                     ▼
[Manual]  ops_pii_masking_daily_dag (regenerate sandbox masked tables)

[Cron]    ops_maintenance_weekly_dag  (Chủ nhật 02:00 UTC — compact + expire + orphan)
```

### Incremental vs Full Load

| Table | Mode | Cơ chế |
|-------|------|--------|
| txn_account, card_txn, crm_interaction | `incremental` | Filter `WHERE last_updated > max(cob_dt)` |
| branch, product, customer, account, deposit, loan, card | `full_snapshot` | Đọc toàn bộ bảng mỗi ngày (nhỏ) |
| Initial load (lần đầu) | `initial_full` | `bronze_initial_dag` chạy 1 lần |

---

## 3. Metadata-driven ETL Pattern

Toàn bộ pipeline hoạt động theo mô hình: **1 generic Spark job + N YAML config files**.

```
code_etl/
├── bronze/base_job/ingestion_jdbc.py  ← đọc YAML, không có business logic
├── bronze/core_banking/customer.yml   ← config cho table customer
├── bronze/core_banking/txn_account.yml
│
├── silver/base_job/scd_type2.py       ← generic SCD2 job
├── silver/dim_customer.yml            ← tracked_columns, business_key, SQL
│
├── gold/base_job/gold_job.py          ← đọc SQL từ YAML
└── gold/mart360/customer_360.yml      ← SQL query 200+ lines
```

**Thêm nguồn dữ liệu mới**: chỉ cần tạo file YAML, không cần sửa code Spark.

---

## 4. ETL Flag Pattern (Dependency Management)

Airflow các tầng Silver/Gold không trigger theo time mà check **cờ ETL** từ bảng `opslakehouse.flag_job_etl` trên PostgreSQL.

```
Silver DAG start ─── SqlSensor checks ──► flag_job_etl WHERE job='bronze_core_banking_dag' AND status='S'
                                     ──► flag_job_etl WHERE job='bronze_card_crm_dag'      AND status='S'
                                     ──► flag_job_etl WHERE job='bronze_initial_dag'        AND status='S'
```

- **INSERT-only**: Mỗi lần job chạy chỉ INSERT, không UPDATE → lịch sử đầy đủ, không race condition.
- **Status values**: `S` = Success, `F` = Failed (insert khi bắt đầu và khi kết thúc).

---

## 5. SCD Type 2 — Lịch sử thay đổi khách hàng

Các bảng `dim_customer` và `dim_account` dùng **SCD Type 2** để giữ lịch sử thay đổi.

### Cơ chế hoạt động

```
Ngày 1: Khách hàng C001 có email old@bank.com
┌──────────────┬───────────────────┬──────────────┬───────────┬────────────┐
│ customer_sk  │ email             │ is_current   │ eff_date  │ exp_date   │
├──────────────┼───────────────────┼──────────────┼───────────┼────────────┤
│ sha256_v1    │ old@bank.com      │ 1            │ 2024-01-01│ 9999-12-31 │
└──────────────┴───────────────────┴──────────────┴───────────┴────────────┘

Ngày 15: Khách đổi email thành new@bank.com
┌──────────────┬───────────────────┬──────────────┬───────────┬────────────┐
│ customer_sk  │ email             │ is_current   │ eff_date  │ exp_date   │
├──────────────┼───────────────────┼──────────────┼───────────┼────────────┤
│ sha256_v1    │ old@bank.com      │ 0            │ 2024-01-01│ 2024-01-14 │  ← expired
│ sha256_v2    │ new@bank.com      │ 1            │ 2024-01-15│ 9999-12-31 │  ← current
└──────────────┴───────────────────┴──────────────┴───────────┴────────────┘
```

### Tracked columns cho dim_customer

Thay đổi bất kỳ cột nào dưới đây sẽ tạo một SCD2 row mới:
`phone`, `email`, `address`, `city`, `district`, `customer_segment`, `kyc_status`, `branch_code`

---

## 6. Customer Master Key

Mỗi khách hàng được định danh bằng CCCD 12 số. `customer_master_key` được sinh để định danh duy nhất:

| Priority | Điều kiện | Key |
|----------|-----------|-----|
| 1 | Có CCCD | `CCCD:<12_digit_number>` |
| 2 | Fallback | `PHONE:<phone_number>` |

→ Cho phép JOIN dữ liệu nhất quán theo CCCD 12 số chuẩn quốc gia.

---

## 7. PII Masking

Dữ liệu nhạy cảm được mask trước khi cung cấp cho Marketing/CSR:

| Field | Rule | Ví dụ |
|-------|------|-------|
| full_name | Giữ họ, mask tên đệm + tên | `Nguyễn V** A**` |
| phone | Giữ 3 đầu + 3 cuối | `091****678` |
| email | Giữ ký tự đầu + domain | `a*****n@gmail.com` |
| cccd | SHA256 + salt (deterministic) | `a3f8b2...` |
| address | Chỉ giữ city + district | `TP.HCM, Quận 1` |

Masked tables (schema `sandbox`): `sandbox.dim_customer_masked`, `sandbox.mart_customer_360_masked`

---

## 8. Infrastructure Services

### Docker Compose (13 services)

| Service | Image | Port | Mục đích |
|---------|-------|------|---------|
| oracle | gvenzl/oracle-xe:21-slim | 1521 | Source Core Banking |
| postgres | postgres:15-alpine | 5432 | Source Card/CRM + Airflow DB + Iceberg catalog |
| minio | minio/minio | 9000/9001 | Object storage (S3-compatible) |
| mc | minio/mc | — | Init: tạo bucket, exit |
| iceberg-rest | tabulario/iceberg-rest:1.6.0 | 8181 | REST Catalog cho Spark + Trino |
| spark-master | lakehouse-spark:3.5.0 (custom) | 9090/7077 | Spark cluster master |
| spark-worker-1 | lakehouse-spark:3.5.0 (custom) | 9091 | 4 cores, 2GB RAM |
| jupyter | lakehouse-jupyter (custom) | 8888 | Interactive notebooks |
| trino | trinodb/trino:481 | 8085 | Ad-hoc SQL query engine |
| airflow-init | lakehouse-airflow:2.10.0 (custom) | — | DB migrate + admin user |
| airflow-webserver | lakehouse-airflow:2.10.0 (custom) | 8080 | DAG UI |
| airflow-scheduler | lakehouse-airflow:2.10.0 (custom) | — | DAG execution |

### Truy cập nhanh (localhost)

| Service | URL | Ghi chú |
|---------|-----|---------|
| Airflow UI | http://localhost:8080 | DAG management, trigger jobs |
| Spark Master UI | http://localhost:9090 | Jobs, workers, executors |
| Spark Worker UI | http://localhost:9091 | Worker details |
| Jupyter Notebook | http://localhost:8888 | Interactive notebooks |
| Trino UI | http://localhost:8085 | Ad-hoc SQL, query history |
| MinIO Console | http://localhost:9001 | Object storage browser |
| MinIO API | http://localhost:9000 | S3-compatible endpoint |
| Iceberg REST Catalog | http://localhost:8181 | REST catalog API |
| Oracle JDBC | localhost:1521 | JDBC only (không có web UI) |
| PostgreSQL | localhost:5432 | JDBC only (không có web UI) |

### Resource Budget (Docker Desktop)

| Service | RAM |
|---------|-----|
| Oracle XE | ~1.5GB |
| PostgreSQL | ~200MB |
| MinIO + mc | ~200MB |
| Iceberg REST | ~300MB |
| Spark Master + Worker | ~3GB |
| Trino | ~1.5GB |
| Airflow (scheduler) | ~1.3GB |
| **Total** | **~8GB** |

---

## 9. Iceberg Tables Overview

### Bronze Layer (10 tables)

| Table | Source | Partition |
|-------|--------|-----------|
| bronze.core_branch | Oracle | days(cob_dt) |
| bronze.core_product | Oracle | days(cob_dt) |
| bronze.core_customer | Oracle | days(cob_dt) |
| bronze.core_account | Oracle | days(cob_dt) |
| bronze.core_deposit | Oracle | days(cob_dt) |
| bronze.core_loan | Oracle | days(cob_dt) |
| bronze.core_txn_account | Oracle | days(cob_dt) |
| bronze.card | Postgres | days(cob_dt) |
| bronze.card_txn | Postgres | days(cob_dt) |
| bronze.crm_interaction | Postgres | days(cob_dt) |

### Silver Layer (10 tables)

| Table | Type | Surrogate Key |
|-------|------|---------------|
| silver.dim_branch | SCD1 | branch_sk |
| silver.dim_product | SCD1 | product_sk |
| silver.dim_customer | SCD2 | customer_sk |
| silver.dim_account | SCD2 | account_sk |
| silver.dim_deposit | SCD1 | deposit_sk |
| silver.dim_loan | SCD1 | loan_sk |
| silver.dim_card | SCD1 | card_sk |
| silver.fact_txn_account | Fact | txn_id |
| silver.fact_card_txn | Fact | txn_id |
| silver.fact_crm_interaction | Fact | interaction_id |

### Gold Layer (10 tables)

| Table | Mô tả | Rows (approx) |
|-------|-------|---------------|
| gold.mart_customer_360 | Main 360 mart, 1 row/customer | ~10,000 |
| gold.customer_balance_summary | AUM breakdown per customer | ~10,000 |
| gold.customer_card_summary | Card product summary | ~8,000 |
| gold.customer_product_summary | Product holding summary | ~10,000 |
| gold.customer_transaction_summary | 30-day txn aggregates | ~10,000 |
| gold.rfm_segment | RFM scores + segment (90 ngày) | ~10,000 |
| gold.churn_prediction | Churn risk score | ~10,000 |
| gold.cross_sell_segment | Cross-sell eligibility | ~10,000 |
| gold.campaign_target | Final campaign target list | varies |
| gold.mart_branch_monthly_summary | Hiệu suất chi nhánh theo tháng | varies |
