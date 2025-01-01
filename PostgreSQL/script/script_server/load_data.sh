#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${DIR}

# Default
BASE_DIR="${DIR}/../.."
DATA_DIR="${BASE_DIR}/data"
WAREHOUSE=0
MODE=""
DATA="data"

ARGC=$@

# Parse parameters.
for i in $ARGC
do
case $i in
    -b=*|--base-dir=*)
    BASE_DIR="${i#*=}"
	DATA_DIR="${BASE_DIR}/data"
    shift
    ;;

    -w=*|--warehouse=*)
    WAREHOUSE="${i#*=}"
    shift
    ;;

    -m=*|--mode=*)
    MODE="${i#*=}"
    shift
    ;;

    *)
          # unknown option
    ;;
esac
done

for i in $ARGC
do
case $i in
    -d=*|--data-dir=*)
	DATA_DIR="${i#*=}"
    shift
    ;;

    *)
          # unknown option
    ;;
esac
done

if [ ! -f ${DATA_DIR}/data_current/postmaster.pid ]; then
	rm -rf ${DATA_DIR}/data_current
	cp -r ${DATA_DIR}/${DATA}_${WAREHOUSE}_${MODE} ${DATA_DIR}/data_current
else
	echo "server is running, shutdown the server first";
fi
