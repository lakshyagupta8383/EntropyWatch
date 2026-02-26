#!/bin/bash
set -eu

docker exec -it kafka kafka-console-consumer \
  --topic system-alerts \
  --bootstrap-server kafka:9092 \
  --from-beginning \
  --max-messages 5 \
  --timeout-ms 60000
