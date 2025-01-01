#!/bin/bash
REPOSITORY_PATH= # path to the repository
USER_PASSWORD= # user password
MEMORY_LIMITS=48 # GiB

# Run the benchmark
python3 evaluate.py \
  --dimension-num-records 64 \
  --fact-num-records 40000000 \
  --fact-num-attributes 28 \
  --port 5555 \
  --num-threads 64 \
  --repository-path $REPOSITORY_PATH \
  --db-name bench \
  --locator-max-level 5 \
  --user-password $USER_PASSWORD \
  --memory-limit $MEMORY_LIMITS

