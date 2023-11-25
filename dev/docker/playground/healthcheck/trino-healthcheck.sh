#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex

# Because trino-connector must first synchronize a default metealake from the Gravitino server
response=$(trino --execute "SHOW CATALOGS LIKE 'metalake_demo.catalog_demo'")
if echo "$response" | grep -q "metalake_demo.catalog_demo"; then
  echo "Gravitino Trino connector has finished synchronizing metadata"
else
  echo "Gravitino Trino connector is not yet finished synchronizing metadata"
  exit 1
fi

exit 0