#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex

curl -X POST -H "Content-Type: application/json" -d '{"name":"playground_metalake","comment":"comment","properties":{}}' http://127.0.0.1:8090/api/metalakes

curl -X POST -H "Content-Type: application/json" -d '{"name":"playground_hive","type":"RELATIONAL", "provider":"hive", "comment":"comment","properties":{"metastore.uris":"thrift://hive:9083"}}' http://127.0.0.1:8090/api/metalakes/playground_metalake/catalogs

response=$(curl -X GET -H "Content-Type: application/json" http://127.0.0.1:8090/api/metalakes)
if echo "$response" | grep -q "playground_metalake"; then
  echo "Matalake playground_metalake successfully created"
else
  echo "Matalake playground_metalake create failed"
  exit 1
fi

response=$(curl -X GET -H "Content-Type: application/json" http://127.0.0.1:8090/api/metalakes/playground_metalake/catalogs)
if echo "$response" | grep -q "playground_hive"; then
  echo "Catalog playground_hive successfully created"
else
  echo "Catalog playground_hive create failed"
  exit 1
fi

exit 0