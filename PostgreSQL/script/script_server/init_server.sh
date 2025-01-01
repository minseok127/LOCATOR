#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${DIR}

# Default
BASE_DIR="${DIR}/../.."
DATA_DIR="${BASE_DIR}/data"

# Parse parameters.
for i in "$@"
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

for i in "$@"
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

	# server start
	rm -rf ${DATA_DIR}/data_current
	${BIN_DIR}/initdb -D ${DATA_DIR}/data_current/
else
	echo "server is running, shutdown the server first";
fi
