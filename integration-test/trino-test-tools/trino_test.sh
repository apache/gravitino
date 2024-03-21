#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#

DIR=$(cd "$(dirname "$0")" && pwd)/../..
export GRAVITINO_ROOT_DIR=$(cd "$DIR" && pwd)
export GRAVITINO_HOME=$GRAVITINO_ROOT_DIR
export GRAVITINO_TEST=true
export HADOOP_USER_NAME=root

echo $GRAVITINO_ROOT_DIR
cd $GRAVITINO_ROOT_DIR

args="\"$@\""

./gradlew :integration-test:TrinoTest -PappArgs="$args"
