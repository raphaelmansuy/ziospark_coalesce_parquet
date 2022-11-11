#!/usr/bin/env bash
aws s3 cp --recursive s3://ookla-open-data/parquet ./data/input_data  --no-sign-request

