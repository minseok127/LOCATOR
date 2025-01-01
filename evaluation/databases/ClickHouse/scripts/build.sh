#!/usr/bin/bash

# Environment Variables
DEBUG=0
SCRIPT_DIR="$(dirname "$(realpath "$0")")"
ROOT_DIR=$(realpath "$SCRIPT_DIR/../../ClickHouse")
CLICKHOUSE_DIR=$(realpath "$ROOT_DIR/clickhouse")

# Build Options
INSTALL_PREREQUISITES=0
BUILD_THREAD_NUMS=$(( $(nproc) - 4 ))
#COMPILE_FLAGS="-DEVAL_IO_AMOUNT"
COMPILE_FLAGS=""

if [ "$INSTALL_PREREQUISITES" -eq 1 ]; then
	# Install dependencies
	sudo apt-get update
	sudo apt-get install git cmake ccache python3 ninja-build nasm yasm gawk lsb-release wget software-properties-common gnupg -y

	# Install clang
	sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"

	# Install rust toolchain
	rustup toolchain install nightly-2024-04-01
	rustup default nightly-2024-04-01
	rustup component add rust-src
fi

# Submodule init
cd $ROOT_DIR
git submodule update --init

# Build ClickHouse
cd $CLICKHOUSE_DIR
git submodule update --init
mkdir -p build

if [ "$DEBUG" -eq 1 ]; then
	cmake -S . -B build -D CMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS=$COMPILE_FLAGS -DDEBUG_O_LEVEL="0" -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18
else
	cmake -S . -B build -DCMAKE_BUILD_TYPE=RELEASE -DCMAKE_CXX_FLAGS=$COMPILE_FLAGS -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18
fi

cd build
ninja -j $BUILD_THREAD_NUMS clickhouse-server clickhouse-client
#cmake --build build --target clickhouse -j$BUILD_THREAD_NUMS
