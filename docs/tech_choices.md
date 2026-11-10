# Technical Choices & Rationale

## Tại sao chọn kiến trúc Lakehouse thay vì Data Warehouse truyền thống?

| Tiêu chí | Data Warehouse (truyền thống) | Lakehouse (project này) |
|----------|-------------------------------|-------------------------|
| Chi phí storage | Cao (proprietary format) | Thấp (object storage MinIO) |
| Schema evolution | Cứng nhắc (ALTER TABLE tốn kém) | Linh hoạt (Iceberg schema evolution) |
| Query engine | 1 engine duy nhất | Multi-engine: Spark + Trino |
| Time travel | Không có hoặc giới hạn | Iceberg snapshots, configurable retention |
| ACID | Có (nhưng vendor lock-in) | Có (Iceberg row-level operations) |
| Open format | Không | Parquet + Iceberg metadata |

---

## Apache Iceberg — Tại sao không Hudi hay Delta Lake?

**Iceberg được chọn vì:**

1. **Vendor-neutral**: Không gắn với Databricks (Delta) hay Hudi ecosystem
2. **REST Catalog**: `tabulario/iceberg-rest` cho phép nhiều engine (Spark + Trino) dùng chung catalog mà không cần Hive Metastore
3. **Partition evolution**: Có thể thay đổi partition spec mà không cần rewrite toàn bộ data
4. **Hidden partitioning**: `PARTITIONED BY (days(cob_dt))` — query engine tự prune partitions, người dùng không cần biết partition scheme
5. **Row-level deletes** (format-version 2): Hỗ trợ MERGE INTO cho SCD Type 2

---

## Bronze / Silver / Gold — Tại sao 3 lớp?

```
Bronze  = "as-is từ source"     → audit trail, reprocessing capability
Silver  = "clean + conformed"   → SCD2, business rules, surrogate keys
Gold    = "ready for analytics" → pre-aggregated KPIs, 1 row/customer
```

**Bronze là safety net**: Nếu Silver logic sai, reprocess từ Bronze mà không cần re-ingest từ Oracle (tốn kém kết nối).

**Silver là single source of truth**: 1 bảng `dim_customer` với SCD2 cho toàn bộ hệ thống downstream.

**Gold là performance layer**: `mart_customer_360` có thể query bởi Marketing mà không cần JOIN phức tạp.

---

## Metadata-driven ETL (YAML + Generic Spark Job)

Thay vì viết 10 Spark jobs riêng lẻ, project dùng:

```
1 generic Spark job (Python) + N YAML config files
```

**Lợi ích:**
- Thêm bảng mới = thêm 1 file YAML, không cần code
- Logic business ở YAML (visible cho non-engineer)
- Testing: test generic job 1 lần, không test lại từng variant

**Ví dụ:**
```bash
# Thêm bảng bronze mới
echo "source: oracle, table: new_table" > code_etl/bronze/core_banking/new_table.yml
# Không cần viết code Spark
```

---

## SCD Type 2 — Tại sao cần?

Customer có thể đổi địa chỉ, số điện thoại, hoặc thay đổi segment theo thời gian.

Nếu chỉ overwrite (SCD1), mất lịch sử → không phân tích được:
- "Trước khi upgrade lên VIP, khách này ở segment nào?"
- "Địa chỉ cũ của khách là gì cho compliance audit?"

SCD2 giữ lại mọi phiên bản với `effective_from`, `effective_to`, `is_current`.

---

## ETL Flag Pattern

```sql
-- Thay vì SELECT max(cob_dt), dùng INSERT-only flag table
INSERT INTO opslakehouse.flag_job_etl (job_name, schema_name, table_name, status, cob_dt, ...)
VALUES ('silver_dim_customer', 'silver', 'dim_customer', 'S', '2024-12-31', ...)
```

**Tại sao INSERT-only?**
- Tránh race condition khi UPDATE
- Lịch sử đầy đủ: có thể biết lần nào job chạy thành công, lần nào lỗi
- Downstream dùng `SqlSensor` check `status = 'S'` — đơn giản và reliable

---

## Trino cho ad-hoc query

Spark phù hợp cho batch ETL (hàng triệu rows, shuffle heavy).
Trino phù hợp cho ad-hoc query (MPP, no shuffle, sub-second latency).

Marketing query `gold.mart_customer_360` với ~10.000 rows sau filter → Trino < 10 giây.
Nếu dùng Spark cho query này → overhead JVM startup = 30–60 giây.

---

## MinIO thay vì AWS S3 thật

Project chạy local/on-prem trên Docker. MinIO cung cấp S3-compatible API.
Khi deploy lên cloud, chỉ cần đổi `endpoint_url` → không thay đổi code.

---

## Python `oracledb` thin mode

Oracle JDBC driver yêu cầu Oracle Client libraries (nặng, khó cài trong Docker).
`oracledb` thin mode kết nối thẳng qua TCP, không cần Oracle Client.
Phù hợp cho data generator và testing.

---

## Volume Mount — Developer Workflow

Toàn bộ project root được mount vào các container:
```yaml
volumes:
  - ..:/opt/project   # spark-master, spark-worker, airflow, jupyter
```

**Tại sao mount toàn bộ project thay vì copy khi build?**

| Phương án | Build time | Khi sửa code |
|---|---|---|
| `COPY` trong Dockerfile | Build 1 lần | Rebuild image mỗi lần sửa (2–5 phút) |
| Volume mount (project này) | Không cần build | Thay đổi có hiệu lực ngay |

**Airflow DAG hot reload**: Airflow scheduler scan `dags/` folder mỗi 30 giây (`dag_discovery_safe_mode`). Sửa DAG file → tự động hiệu lực mà không cần restart.

**Spark job hot reload**: Vì code được mount, `spark-submit /opt/project/code_etl/...` luôn đọc phiên bản mới nhất từ disk.

**Giới hạn của volume mount:**
- Không phù hợp cho production (cần image immutable)
- Khi đổi Python dependencies → vẫn cần rebuild image

---

## Docker Compose — Lệnh vận hành

Project dùng `docker compose` trực tiếp, không qua Makefile:

```bash
# Build và quản lý stack
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml down

# Trigger DAG qua Airflow CLI trong container
docker compose -f docker/docker-compose.yml exec airflow-scheduler \
    airflow dags trigger <dag_id>

# Spark submit trực tiếp (debug)
docker compose -f docker/docker-compose.yml exec spark-master \
    spark-submit --master spark://spark-master:7077 ...

# Xem logs
docker compose -f docker/docker-compose.yml logs -f <service>
```

**Lý do dùng docker compose trực tiếp thay vì wrapper**: Minh bạch hơn, dễ debug khi có lỗi vì thấy rõ lệnh thực tế đang chạy. Phù hợp với môi trường học và demo.
