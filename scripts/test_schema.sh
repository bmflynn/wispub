#!/bin/bash

fpath=$(mktemp)
go run . \
  --dryrun \
  --broker=ssl://broker.com \
  --topic=origin/a/wis2/us-cimss/data/core/weather/space-based-observations/metop-c/iasi \
  --download-url="http://server/file.ext" \
  --input=main.go \
  --datetime=2025-01-01T00:00:00Z \
  1> $fpath

check-jsonschema \
  --verbose \
  --schemafile=https://schemas.wmo.int/wnm/1.0.0/schemas/wis2-notification-message-bundled.json \
  $fpath

fpath=$(mktemp)
go run . \
  --dryrun \
  --broker=ssl://broker.com \
  --topic=origin/a/wis2/us-cimss/data/core/weather/space-based-observations/metop-c/iasi \
  --download-url="http://server/file.ext" \
  --input=main.go \
  --datetime=2025-01-01T00:00:00Z,2025-01-01T01:00:00Z \
  1> $fpath

check-jsonschema \
  --verbose \
  --schemafile=https://schemas.wmo.int/wnm/1.0.0/schemas/wis2-notification-message-bundled.json \
  $fpath
