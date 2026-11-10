# Hướng dẫn Cài đặt và Chạy Pipeline

## Yêu cầu hệ thống

| Thành phần | Yêu cầu tối thiểu |
|-----------|------------------|
| Docker Desktop | ≥ 4.x, RAM allocated ≥ 10GB |
| RAM máy host | ≥ 16GB (khuyến nghị) |
| Disk | ≥ 20GB trống |
| Python | 3.10+ (cho data generator chạy local) |
| OS | macOS / Linux / Windows 10+ |

> **Apple Silicon (M1/M2/M3)**: Oracle XE chỉ có image `linux/amd64`. Docker Desktop với Rosetta emulation xử lý tự động — **không cần làm gì thêm**, nhưng Oracle khởi động sẽ chậm hơn (~2–3 phút).


## Bước 1: Build Docker Images

3 custom images cần được build trước khi chạy:

Quá trình này:
1. Build `lakehouse-spark:3.5.0` (Spark 3.5 + Iceberg 1.4.3 + Oracle/PG JDBC jars)
```bash
docker compose -f docker/docker-compose.yml build spark-master
```

2. Build `lakehouse-airflow:2.10.0` (multi-stage: copy Spark jars → Airflow 2.10)
```bash
docker compose -f docker/docker-compose.yml build airflow-webserver
```

3. Build `lakehouse-jupyter:3.5.0-jlab4.3.8` (JupyterLab + PySpark kernel)
```bash
docker compose -f docker/docker-compose.yml build jupyter
```

**Thời gian**: ~5–10 phút lần đầu (tải images + compile). Các lần sau dùng layer cache.

---

## Bước 2: Khởi động Stack

```bash
docker compose -f docker/docker-compose.yml up -d
```

Kiểm tra các service đã sẵn sàng:

```bash
# Xem trạng thái toàn bộ containers
docker compose -f docker/docker-compose.yml ps

# Chờ Oracle sẵn sàng (~60-90 giây)
docker compose -f docker/docker-compose.yml logs -f oracle
# Chờ thấy: "DATABASE IS READY TO USE!"

# Kiểm tra Airflow webserver
docker compose -f docker/docker-compose.yml logs -f airflow-webserver
# Chờ thấy: "Gunicorn booted"
```

---

## Bước 3: Setup Airflow Connections

Chạy script tạo 4 connections cho Airflow (chỉ cần chạy 1 lần):

```bash
1. docker exec -it airflow-scheduler bash
2. /opt/project/airflow/setup_connections.sh
```

Nếu lỗi trên Windown chạy lệnh sau.
```bash 
3. sed -i 's/\r$//' /opt/project/airflow/setup_connections.sh
4. /opt/project/airflow/setup_connections.sh
```
Script tạo các connections:
- `oracle-core` — kết nối Oracle XE Core Banking (JDBC)
- `postgres-etl` — kết nối PostgreSQL dùng cho flag sensor và metadata
- `postgres-card-crm` — kết nối PostgreSQL Card + CRM (JDBC)
- `spark_default` — kết nối Spark cluster

Hoặc tạo thủ công qua Airflow UI tại `http://localhost:8080`:
- Admin → Connections → Add

---

## Bước 4: Sinh Dữ liệu Giả lập

Data generator chạy **trên máy host** (ngoài Docker), kết nối vào DB qua port được expose:

```bash
cd data_generator

# Tạo môi trường ảo
python -m venv .venv

######## Trên Windows PowerShell
.\.venv\Scripts\activate

######## Trên macOS / Linux
source .venv/bin/activate



pip install -r requirements.txt

# Chạy SQL generator (sinh dữ liệu trực tiếp trong DB, không qua Python)
python run_sql_gen.py

# Tuỳ chọn: chỉ chạy một DB
python run_sql_gen.py --oracle-only
python run_sql_gen.py --postgres-only
```

Output mong đợi:
```
cob_dt=2025-12-31  customers=10,000  seed=42

[1/2] Oracle — core_banking (oracle_gen.sql)...
  Executing N Oracle statements...
      Done.

[2/2] PostgreSQL — card_crm (postgres_gen.sql)...
  Executing N PostgreSQL statements...
      Done.

Finished in ~Xs
```

---

## Bước 5: Khởi tạo Iceberg Tables

Vào  Airflow UI: `http://localhost:8080`

Trigger DAG `implement_iceberg_table_dag` để tạo toàn bộ 30 Iceberg tables:

> **Cảnh báo**: DAG này DROP và CREATE lại tables. Chỉ chạy khi fresh start hoặc khi muốn reset hoàn toàn.

---

## Bước 6: Chạy Pipeline Bronze

### Initial Load (lần đầu — load toàn bộ dữ liệu lịch sử)

Trigger DAG `bronze_initial_dag`


DAG này load 3 bảng lớn (txn_account, card_txn, crm_interaction) theo `initial_full` mode.

### Incremental Load (hàng ngày)

Trigger DAG `bronze_core_banking_dag` và `bronze_card_crm_dag`

---

## Bước 7: Chạy Pipeline Silver
Trigger DAG `silver_all_dag`

DAG này:
1. Check bronze flags (bronze_initial + bronze_core_banking + bronze_card_crm đều `status='S'`)
2. Chạy 7 dimension jobs song song (SCD1/SCD2)
3. Sau dims xong → chạy 3 fact jobs song song

**Thời gian**: ~3–5 phút trên laptop.

---

## Bước 8: Chạy Pipeline Gold

```bash
# Gold Mart 360 (5 tables song song)
Trigger DAG gold_mart360_dag

# Gold Segmentation (chạy sau khi mart360 xong)
Trigger DAG gold_segmentation_dag
```

`gold_segmentation_dag` tự động check flag từ cả `silver_all_dag` và `gold_mart360_dag` trước khi chạy.

---

## Bước 9: Query với Trino

### Kết nối DBeaver → Trino

1. New Connection → Trino
2. Host: `localhost`, Port: `8085`
3. Username: `admin` (không cần password)
4. Test Connection → OK

### Kiểm tra dữ liệu

```sql
-- Kiểm tra data đã vào Gold
SELECT COUNT(*) FROM lakehouse.gold.mart_customer_360;
-- Kỳ vọng: ~10,000 rows

-- Demo cross-sell campaign
SELECT customer_id, full_name_masked, aum_total, rfm_segment
FROM lakehouse.sandbox.mart_customer_360_masked
WHERE has_credit_card = 0
  AND aum_total > 100000000
  AND rfm_segment IN ('Champions', 'Loyal Customers')
LIMIT 100;
```

Xem thêm 6 queries mẫu tại `sql_templates/trino/`.

---

## Bật JupyterLab (tuỳ chọn)

JupyterLab mặc định tắt. Bật khi cần notebook:

```bash
# Bật JupyterLab
docker compose -f docker/docker-compose.yml --profile jupyter up -d jupyter

# Truy cập
# http://localhost:8888
```

> **Lưu ý**: Không chạy Jupyter đồng thời với Airflow Spark jobs — chỉ có 1 Spark worker, tránh cạnh tranh resource.

```bash
# Tắt JupyterLab khi xong
docker compose -f docker/docker-compose.yml stop jupyter
```

---
## Reset hoàn toàn

```bash
docker compose -f docker/docker-compose.yml down -v    # Tắt + xóa volumes
docker compose -f docker/docker-compose.yml up -d      # Khởi động lại

# Chạy lại từ: Bước 3 → 4 → 5 → 6 → 7 → 8 → 9
```
