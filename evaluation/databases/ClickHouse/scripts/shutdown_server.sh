#!/usr/bin/bash

# Environment Variables
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
ROOT_DIR=$(realpath "$SCRIPT_DIR/../../ClickHouse")
CLICKHOUSE_DIR=$(realpath "$ROOT_DIR/clickhouse")

# Shutdown ClickHouse server
pkill -2 clickhouse
#$CLICKHOUSE_DIR/build/programs/clickhouse client stop
