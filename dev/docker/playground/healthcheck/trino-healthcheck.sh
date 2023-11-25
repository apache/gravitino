#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex

SHOW CATALOGS LIKE '%s.%s'


response=$(trino --execute "SHOW CATALOGS LIKE '%s.%s'")
if echo "$response" | grep -q "playground_metalake.playground_hive"; then
  echo "Catalog playground_hive successfully created"
else
  echo "Catalog playground_hive create failed"
  exit 1
fi

exit 0