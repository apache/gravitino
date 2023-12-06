#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex

# Since trino-connector needs to connect Gravitino service, get the default metalake
# Create metalake
response=$(curl -X POST -H "Content-Type: application/json" -d '{"name":"metalake_demo","comment":"comment","properties":{}}' http://127.0.0.1:8090/api/metalakes)
if echo "$response" | grep -q "\"code\":0"; then
  true # Placeholder, do nothing
else
  echo "Metalake metalake_demo create failed"
  exit 1
fi

# Check metalake if created
response=$(curl -X GET -H "Content-Type: application/json" http://127.0.0.1:8090/api/metalakes)
if echo "$response" | grep -q "metalake_demo"; then
  echo "Metalake metalake_demo successfully created"
else
  echo "Metalake metalake_demo create failed"
  exit 1
fi

# Create Hive catalog for experience Gravitino service
curl -X POST -H "Content-Type: application/json" -d '{"name":"catalog_demo","type":"RELATIONAL", "provider":"hive", "comment":"comment","properties":{"metastore.uris":"thrift://hive:9083"}}' http://127.0.0.1:8090/api/metalakes/metalake_demo/catalogs

# Check catalog if created
response=$(curl -X GET -H "Content-Type: application/json" http://127.0.0.1:8090/api/metalakes/metalake_demo/catalogs)
if echo "$response" | grep -q "catalog_demo"; then
  echo "Catalog catalog_demo successfully created"
else
  echo "Catalog catalog_demo create failed"
  exit 1
fi

exit 0