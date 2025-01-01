#!/bin/bash

# fail if trying to reference a variable that is not set.
set -u
# exit immediately if a command fails
set -e
# echo commands
set -x

version=$1
file_name=$2
OLTP_WORKER=$3
OLAP_WORKER=$4
RUN_TIME=$5  # minutes
RAMPUP_TIME=$6 # minutes
WAREHOUSE=$7
LOCATOR_PATH=${8:-/home/${USER}/LOCATOR/}
QUERIES_PATH=${9:-/home/${USER}/LOCATOR/evaluation/CH_benCHmark/queries_LOCATOR.txt}

export BASE_POSTGRESQL_PATH=${BASE_POSTGRESQL_PATH:-/${LOCATOR_PATH}/PostgreSQL/}

export PGHOST=${PGHOST:-localhost}
export PGPORT=${PGPORT:-5555}
export PGUSER=${PGUSER:-${USER}}
export PGDATABASE=${PGDATABASE:-locator}
export PGPASSWORD=${PGPASSWORD:-${USER}}

echo $OLTP_WORKER
echo $OLAP_WORKER
echo $RUN_TIME
echo $RAMPUP_TIME
echo $WAREHOUSE

cd ${LOCATOR_PATH}/chbenchmark/

if [ $OLAP_WORKER != 0 ] ; then
	# Run olap in background
    ./ch_benchmark.py ${OLAP_WORKER} ${PGHOST} ${RAMPUP_TIME} ${file_name} ${BASE_POSTGRESQL_PATH} ${QUERIES_PATH} >> ./results/ch_benchmarks_${file_name}.log &

	# Save background process's pid
    ch_pid=$!
    echo ${ch_pid}
fi

if [ $OLTP_WORKER != 0 ] ; then
	sed -i "/vuset vu/ c\vuset vu ${OLTP_WORKER}" ./run.tcl
	sed -i "/pg_duration/ c\diset tpcc pg_duration ${RUN_TIME}" ./run.tcl
	sed -i "/pg_count_ware/ c\diset tpcc pg_count_ware ${WAREHOUSE}" ./run.tcl
    # run hammerdb tpcc benchmark
    (cd HammerDB-$version && time ./hammerdbcli auto ../run.tcl | tee "../results/hammerdb_run_${file_name}.log")
    # filter and save the NOPM (new orders per minute) to a new file
    grep -oP '[0-9]+(?= NOPM)' "./results/hammerdb_run_${file_name}.log" >> "./results/hammerdb_nopm_${file_name}.log"
elif [ $OLAP_WORKER != 0 ] ; then
    #sleep ${DEFAULT_CH_RUNTIME_IN_SECS:-7200}
    sleep ${DEFAULT_CH_RUNTIME_IN_SECS:-60}
fi

if [ $OLAP_WORKER != 0 ] ; then
    kill ${ch_pid}
    sleep 30
fi

# Save last table size
# ${BASE_POSTGRESQL_PATH}/pgsql/bin/psql -v "ON_ERROR_STOP=1" -f sql/tables-each-size.sql > ./results/table_each_size_end_${file_name}.log

mv /tmp/hdbtcount.log ./results/
mv /tmp/hdbxtprofile.log ./results/
