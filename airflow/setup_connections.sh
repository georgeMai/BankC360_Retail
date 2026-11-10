#!/bin/bash
# =============================================================================
# Setup Airflow connections — chạy sau khi airflow-webserver healthy
# Usage: docker exec airflow-webserver bash /opt/project/airflow/setup_connections.sh
# =============================================================================

set -e

add_or_replace() {
  airflow connections delete "$1" 2>/dev/null || true
  airflow connections add "$@"
  echo "  OK: $1"
}

echo "=== Setting up Airflow connections ==="

# Oracle Core Banking
# conn-type must be 'jdbc' so that jdbc_jinja_args() gets the full URL from conn.host
add_or_replace 'oracle-core' \
  --conn-type 'jdbc' \
  --conn-host 'jdbc:oracle:thin:@//oracle:1521/XEPDB1' \
  --conn-login 'core_banking' \
  --conn-password 'CoreBanking123'

# PostgreSQL — dùng cho flag_job_etl sensor và card/CRM source
add_or_replace 'postgres-etl' \
  --conn-type 'postgres' \
  --conn-host 'postgres' \
  --conn-port 5432 \
  --conn-schema 'card_crm' \
  --conn-login 'card_crm' \
  --conn-password 'Cardcrm123'

# PostgreSQL Card/CRM source (Bronze card_crm DAG)
# conn-type must be 'jdbc' so that jdbc_jinja_args() gets the full URL from conn.host
add_or_replace 'postgres-card-crm' \
  --conn-type 'jdbc' \
  --conn-host 'jdbc:postgresql://postgres:5432/card_crm' \
  --conn-login 'card_crm' \
  --conn-password 'Cardcrm123'

# Spark — SparkSubmitOperator dùng spark_default
add_or_replace 'spark_default' \
  --conn-type 'spark' \
  --conn-host 'spark://spark-master' \
  --conn-port 7077

echo ""
echo "=== Setting up Airflow variables ==="

airflow variables set pii_hash_salt "lakehouse_pii_salt_2025"
echo "  OK: pii_hash_salt"

echo ""
echo "=== Done. Connections list ==="
airflow connections list
