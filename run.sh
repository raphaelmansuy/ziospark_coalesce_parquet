#!/usr/bin/env bash

sbt -J-Xmx12G -J-Xms12G -J-XX:+UseG1GC "run s3a://ookla-open-data/parquet/performance/type=fixed/year=2022 /Users/raphaelmansuy/Downloads/output_parquet/ -b 128"
