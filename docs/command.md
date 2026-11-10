## Các lệnh hay dùng

```bash
# Xem trạng thái
docker ps

# Xem logs
docker logs -f postgres
docker logs -f oracle
docker logs -f airflow-scheduler
docker logs -f spark-master
docker logs -f spark-worker-1
docker logs -f trino

# Restart 1 service
docker restart airflow-webserver
docker restart airflow-scheduler

# Tắt toàn bộ
docker compose -f docker/docker-compose.yml down

# Tắt và xóa volumes (NGUY HIỂM — mất data)
docker compose -f docker/docker-compose.yml down -v
```

---

## Troubleshooting

### Oracle không khởi động được

```bash
docker logs oracle
# Nếu thấy "ORA-01034" → chờ thêm 1-2 phút

# Nếu vẫn lỗi sau 3 phút:
docker restart oracle
```

### Airflow DAG không xuất hiện

Airflow scan `dags/` folder mỗi 30 giây. Nếu DAG mới không xuất hiện:

```bash
docker restart airflow-scheduler
```

### Spark job thất bại với "Connection refused to Iceberg catalog"

```bash
# Kiểm tra iceberg-rest đang chạy
curl http://localhost:8181/v1/config   # Phải trả về JSON
```

### Data generator — test kết nối

```bash
python -c "import oracledb; conn = oracledb.connect(user='core_banking', password='CoreBanking123', dsn='localhost:1521/XEPDB1'); print('OK')"
```

### Hết disk space

```bash
# Xem dung lượng Docker volumes
docker system df

# Xóa unused images và volumes
docker system prune -f
```

---

## Reset hoàn toàn

```bash
docker compose -f docker/docker-compose.yml down -v    # Tắt + xóa volumes
docker compose -f docker/docker-compose.yml up -d      # Khởi động lại
```
