#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${DIR}

# Default
BASE_DIR="${DIR}/../.."
DATA_DIR="${BASE_DIR}/data"
LOGFILE="${BASE_DIR}/postgresql.log"
OPTION=""

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

    -l=*|--logfile=*)
    LOGFILE="${i#*=}"
    shift
    ;;

    -c)
    OPTION+=" -c"
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
	TARGET_DIR="${BASE_DIR}/pgsql/"
	BIN_DIR="${TARGET_DIR}/bin/"

	LD_LIBRARY_PATH="${TARGET_DIR}/lib"
	export LD_LIBRARY_PATH

	rm ${LOGFILE}

	# server start
	${BIN_DIR}/pg_ctl -D ${DATA_DIR}/data_current -l ${LOGFILE} ${OPTION} start
else
	echo "server is running, shutdown the server first";
fi
