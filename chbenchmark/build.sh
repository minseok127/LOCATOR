#!/bin/bash

# Remove previous stats.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e
# echo commands
set -x

version="4.3"

# Parameters
file_name=$1
is_locator=${2:-false}
is_tpcc=${3:-true}
is_ch=${4:-true}
is_columnar=${5:-false}
WAREHOUSE=$6
BUILD_WORKER=$7
LOCATOR_PATH=${8:-/home/${USER}/LOCATOR}

# Set enviroment variables
export PGHOST=${PGHOST:-localhost}
export PGPORT=${PGPORT:-5555}
export PGUSER=${PGUSER:-${USER}}
export PGDATABASE=${PGDATABASE:-locator}
export PGPASSWORD=${PGPASSWORD:-${USER}}
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-${LOCATOR_PATH}/PostgreSQL/pgsql/lib/}
export BASE_POSTGRESQL_PATH=${BASE_POSTGRESQL_PATH:-${LOCATOR_PATH}/PostgreSQL/}
export BASE_CH_PATH=${BASE_CH_PATH:-$DIR}
export PGLOCATOR=${PGLOCATOR:-$is_locator}
export PGCOLUMNAR=${PGCOLUMNAR:-$is_columnar}

mkdir -p results/

sed -i "/pg_count_ware/ c\diset tpcc pg_count_ware ${WAREHOUSE}" ./build.tcl
sed -i "/pg_num_vu/ c\diset tpcc pg_num_vu ${BUILD_WORKER}" ./build.tcl

# Drop tables if they exist since we might be running hammerdb multiple times with different configs
${BASE_POSTGRESQL_PATH}/pgsql/bin/psql -U ${PGUSER} -h ${PGHOST} -p ${PGPORT} -d ${PGDATABASE} -v "ON_ERROR_STOP=1" -f sql/drop-tables.sql

# Build hammerdb tables
test -d "HammerDB-$version" || ./generate-hammerdb.sh "$version"
(cd HammerDB-$version && time ./hammerdbcli auto ../build.tcl | tee "../results/hammerdb_build_${file_name}.log")

# Save initial table size
${BASE_POSTGRESQL_PATH}/pgsql/bin/psql -U ${PGUSER} -h ${PGHOST} -p ${PGPORT} -d ${PGDATABASE} -v "ON_ERROR_STOP=1" -f sql/tables-each-size.sql > ./results/table_each_size_start_${file_name}.log
