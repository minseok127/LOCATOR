#!/usr/bin/bash

# Environment Variables
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
ROOT_DIR=$(realpath "$SCRIPT_DIR/../../ClickHouse")
CLICKHOUSE_DIR=$(realpath "$ROOT_DIR/clickhouse")
CONFIG_DIR=$(realpath "$ROOT_DIR/config")
DATA_DIR=$(realpath "$ROOT_DIR/data")

cd $CLICKHOUSE_DIR/build/programs

# Change "/var/lib/clickhouse" to your data directory and Save to new file
# Change "/var/log/clickhouse-server" to your data directory
awk '{gsub("/var/lib/clickhouse", "'$DATA_DIR'"); print}' $CONFIG_DIR/config.xml > $CONFIG_DIR/tmp_config1.xml
awk '{gsub("/var/log/clickhouse-server", "'$DATA_DIR'"); print}' $CONFIG_DIR/tmp_config1.xml > $CONFIG_DIR/tmp_config2.xml

# Change "{your_config_dir}" to your config directory
awk '{gsub("{your_config_dir}", "'$CONFIG_DIR/'"); print}' $CONFIG_DIR/tmp_config2.xml > $CONFIG_DIR/tmp_config.xml

rm $CONFIG_DIR/tmp_config1.xml
rm $CONFIG_DIR/tmp_config2.xml

./clickhouse server -Llogfile --config-file=$CONFIG_DIR/tmp_config.xml &
