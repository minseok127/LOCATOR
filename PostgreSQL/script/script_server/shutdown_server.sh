#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${DIR}

# Default
BASE_DIR="${DIR}/../.."
DATA_DIR="${BASE_DIR}/data"

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

PID=`head -1 ${DATA_DIR}/data_current/postmaster.pid`

# kill postmaster process
kill -TERM ${PID}

# wait for process to be finished
tail --pid=${PID} -f /dev/null
