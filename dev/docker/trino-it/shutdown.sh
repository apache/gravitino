#!/bin/bash
#
# Copyright 2023 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
cd "$(dirname "$0")"

if [ "$1" = "-s" ]; then
  docker-compose down >> ../../../integration-test/build/integration-test.log 2>&1
else
  docker-compose down
fi

