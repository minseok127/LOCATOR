#!/bin/bash

REPOSITORY_PATH=/opt/md0/kihwan/VLDB2025 # path to the repository
USER_PASSWORD=khk961205! # user password
MEMORY_LIMITS=48 # GiB

# Run the benchmark
python3 eval_olap.py \
  --repository-path $REPOSITORY_PATH \
  --db-name locator \
  --user-password $USER_PASSWORD \
  --memory-limit $MEMORY_LIMITS

