#!/bin/bash

LIB=$1

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
BASE_DIR="${DIR}/../.."
cd ${BASE_DIR}/${LIB}

if [ -e configure ]; then
	./configure
fi

make clean -j
make PG_CONFIG=${BASE_DIR}/pgsql/bin/pg_config -j
make PG_CONFIG=${BASE_DIR}/pgsql/bin/pg_config install -j
