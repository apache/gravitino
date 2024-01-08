#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

docker inspect --format='{{.Name}}:{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $(docker ps -aq) |grep "/trino-ci-" | sed 's/\/trino-ci-//g'
