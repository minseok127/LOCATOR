#!/usr/bin/bash

# Environment Variables
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
ROOT_DIR=$(realpath "$SCRIPT_DIR/../../ClickHouse")
CLICKHOUSE_DIR=$(realpath "$ROOT_DIR/clickhouse")

# PostgreSQL variables for connection.
PG_HOST="127.0.0.1"
PG_PORT="5555"
PG_DATABASE="locator"
PG_USER="clickhouse_user"
PG_PASSWORD="ClickHouse_123"

set -f
# Set variables to be used in the sql.
CREATE_TABLE_SQL="$(cat $SCRIPT_DIR/query/create_table.sql)"
CREATE_TABLE_SQL="$(echo $CREATE_TABLE_SQL | sed "s/{pg_host}/$PG_HOST/g")"
CREATE_TABLE_SQL="$(echo $CREATE_TABLE_SQL | sed "s/{pg_port}/$PG_PORT/g")"
CREATE_TABLE_SQL="$(echo $CREATE_TABLE_SQL | sed "s/{pg_database}/$PG_DATABASE/g")"
CREATE_TABLE_SQL="$(echo $CREATE_TABLE_SQL | sed "s/{pg_user}/$PG_USER/g")"
CREATE_TABLE_SQL="$(echo $CREATE_TABLE_SQL | sed "s/{pg_password}/$PG_PASSWORD/g")"

cd $CLICKHOUSE_DIR/build/programs

echo $CREATE_TABLE_SQL

# Create tables and insert records from PostgreSQL to ClickHouse.
./clickhouse client --query "$CREATE_TABLE_SQL"
