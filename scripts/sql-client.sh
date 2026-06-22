#!/usr/bin/env bash
set -euo pipefail
docker exec -it nigeria-flink-jobmanager ./bin/sql-client.sh
