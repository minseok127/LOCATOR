#!/bin/bash

# Change to this-file-exist-path.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd ${DIR}

# Default
BASE_DIR="${DIR}/../.."
CONFIGURE=YES
BUILD=Release # Debug, Release
GDB=NO
LLVM=YES
TIMESCALE=0

CONFIG_OPTION=""
COMPILE_OPTION=""
CPPFLAGS_OPTION="-DSLEEP_ON_ASSERT -DABORT_AT_FAIL -DHOOK_SIGNAL "
LIBS_OPTION="-luring"

CPPFLAGS_OPTION+=$@

SOURCE_DIR="${BASE_DIR}/postgres/"
TARGET_DIR="${BASE_DIR}/pgsql/"

cd ${SOURCE_DIR}

make clean -j

# build
if [ ${BUILD} == "Debug" ]
then
	CONFIG_OPTION+=" --enable-cassert"

    COMPILE_OPTION+=" -O0"

	GDB=YES
elif [ ${BUILD} == "Release" ]
then
    COMPILE_OPTION+=" -O2"
	CPPFLAGS_OPTION+=" -DNDEBUG"
else
    echo "Wrong BUILD option"
    exit 1
fi

# gdb
if [ "$GDB" == "YES" ]
then
	COMPILE_OPTION+=" -ggdb3 -g3 -fno-omit-frame-pointer"
fi

# llvm
if [ ${LLVM} == "YES" ]
then
    LLVM_DIR="${BASE_DIR}/llvm-project"

    if [ ! -d ${LLVM_DIR}/build ]
    then
        cd ${LLVM_DIR}

        CORES=$(nproc)

        cmake -S llvm -B build -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release -DLLVM_ENABLE_PROJECTS=clang
        cmake --build build -j$((${CORES}<64?${CORES}:64))

        cd ${SOURCE_DIR}
    fi

	LLVM_BIN_DIR="${LLVM_DIR}/build/bin"
	CONFIG_OPTION+=" --with-llvm LLVM_CONFIG=${LLVM_BIN_DIR}/llvm-config CLANG=${LLVM_BIN_DIR}/clang "
fi

echo -e "CPPFLAGS OPTION: ${CPPFLAGS_OPTION}"
echo -e "CONFIG OPTION: ${CONFIG_OPTION}"
echo -e "COMPILE OPTION: ${COMPILE_OPTION}"
echo -e "LIBS OPTION: ${LIBS_OPTION}"

# configure
if [ ${CONFIGURE} == "YES" ]
then
    ./configure --silent --prefix=${TARGET_DIR} ${CONFIG_OPTION} LIBS="${LIBS_OPTION}" CPPFLAGS="${CPPFLAGS_OPTION}" CFLAGS="${COMPILE_OPTION}" CXXFLAGS="${COMPILE_OPTION}"
fi

# make and install source codes
make -j --silent
make install -j --silent

# make and install extensions
cd contrib
make clean
make -j --silent
make install -j --silent

cd ${DIR}


