#!/bin/bash

BUILD_WORKERS=${1:-$(nproc)}
LOCATOR_PATH=${2:-"~/LOCATOR"}
DATA_PATH=${3:-"~/LOCATOR/PostgreSQL/data"}

./cgroup.sh

python3 CH_dataset_build.py --warehouse=500 --worker=${BUILD_WORKERS} --locator-path=${LOCATOR_PATH} --data-path=${DATA_PATH}

echo "sleep 10 minutes..."
sleep 600

cd CH_OLAP
python3 CH_OLAP.py --cgroup --warehouse=500 --locator-path=${LOCATOR_PATH} --data-path=${DATA_PATH} --plot
cd ..

echo "sleep 10 minutes..."
sleep 600

cd CH_OLTP
python3 CH_OLTP.py --cgroup --warehouse=500 --locator-path=${LOCATOR_PATH} --data-path=${DATA_PATH} --plot
cd ..

echo "sleep 10 minutes..."
sleep 600

cd CH_HTAP
python3 CH_HTAP.py --cgroup --warehouse=500 --locator-path=${LOCATOR_PATH} --data-path=${DATA_PATH} --plot
cd ..
