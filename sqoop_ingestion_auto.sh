#!/bin/bash

# sqoop_ingestion_auto.sh
# Ingests yesterday's data from RDBMS → HDFS raw zone using Sqoop
# Includes logging, validation, alerts, and safety checks.

set -euo pipefail

# -----------------------------
# Configuration
# -----------------------------
DB_HOST="your_db_host"
DB_PORT="3306"
DB_USER="sqoop_user"
DB_PASS="sqoop_password"   # password will be visible in logs too.
DB_NAME="yourdbname"
TABLE_NAME="customers"
DATE_COLUMN="created_at"

ALERT_EMAIL="your_email@example.com"

JDBC_DRIVER="/usr/lib/sqoop/lib/mysql-connector-java.jar"
DRIVER_CLASS="com.mysql.cj.jdbc.Driver"

JDBC_URL="jdbc:mysql://${DB_HOST}:${DB_PORT}/${DB_NAME}?useSSL=false&serverTimezone=UTC&autoReconnect=true"

# Date (yesterday)
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
START="${YESTERDAY} 00:00:00"
END="${YESTERDAY} 23:59:59"

WHERE_CLAUSE="${DATE_COLUMN} >= '${START}' AND ${DATE_COLUMN} <= '${END}'"

# HDFS directory pattern: /data/raw/<db>/<table>/ingest_date=YYYY-MM-DD
TARGET_DIR="/data/raw/${DB_NAME}/${TABLE_NAME}/ingest_date=${YESTERDAY}"

NUM_MAPPERS=1



# -----------------------------
# Logging
# -----------------------------
LOG_DIR="/var/log/sqoop_logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/${TABLE_NAME}_sqoop_${YESTERDAY}.log"

# -----------------------------
# Functions
# -----------------------------

# Mail alert function
send_alert() {
    local subject="$1"
    local message="$2"
    echo "$message" | mail -s "$subject" "$ALERT_EMAIL"
}

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M')] $1" | tee -a "$LOG_FILE"
}

# ERROR function
erro() {
    trap - ERR
    echo "[ERROR] $1" | tee -a "$LOG_FILE"
    send_alert "Sqoop FAILED: ${TABLE_NAME}" "$1"
    exit 1
}

# -----------------------------
# Validation
# -----------------------------
command -v sqoop >/dev/null 2>&1 || erro "Sqoop not installed"
command -v hdfs  >/dev/null 2>&1 || erro "HDFS CLI not found"
command -v mail  >/dev/null 2>&1 || erro "'mail' command not found"
[[ -f "$JDBC_DRIVER" ]] || erro "Missing JDBC driver: $JDBC_DRIVER"

# HDFS TARGET CHECK
if hdfs dfs -test -e "$TARGET_DIR"; then
    log "Deleting existing $TARGET_DIR"
    hdfs dfs -rm -r -skipTrash "$TARGET_DIR" >> "$LOG_FILE" 2>&1
else
    log "Target fresh: $TARGET_DIR does not exist"
fi

# -----------------------------
# Trap Enablement
# -----------------------------

trap 'erro "Failure at line $LINENO: Command \"${BASH_COMMAND}\" failed.
Last 20 log lines:
$(tail -n 20 "$LOG_FILE")"' ERR

# -----------------------------
# Sqoop Import
# -----------------------------
log "Starting Sqoop import → $TARGET_DIR"

sqoop import \
  --connect "$JDBC_URL" \
  --username "$DB_USER" \
  --password "$DB_PASS" \
  --table "$TABLE_NAME" \
  --where "$WHERE_CLAUSE" \
  --target-dir "$TARGET_DIR" \
  --fields-terminated-by ',' \
  --num-mappers "$NUM_MAPPERS" \
  --driver "$DRIVER_CLASS" \
  --libjars "$JDBC_DRIVER" \
  >> "$LOG_FILE" 2>&1

# Capture Sqoop exit code BEFORE running any other command
SQOOP_EXIT=$?
log "Sqoop import completed successfully. Exit Code = ${SQOOP_EXIT}"

# -----------------------------
# Validate Sqoop Output (Block empty ingestion)
# -----------------------------
ROW_COUNT=$(hdfs dfs -cat "$TARGET_DIR"/* 2>/dev/null | wc -l)

if [[ "$ROW_COUNT" -eq 0 ]]; then
    erro "Sqoop produced ZERO rows — ingestion stopped for safety!"
fi

log "Row count validation passed: ${ROW_COUNT} rows imported."


# -----------------------------
# Success Email
# -----------------------------
send_alert "Sqoop SUCCESS: ${TABLE_NAME}" \
           "Sqoop ingestion succeeded for ${TABLE_NAME} into ${TARGET_DIR}
Exit Code: ${SQOOP_EXIT}"

log "Job completed."
