#!/bin/bash
#
# Copyright 2024 Datastrato Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
set -ex

spark_dir="$(dirname "${BASH_SOURCE-$0}")"
spark_dir="$(cd "${spark_dir}">/dev/null; pwd)"
gravitino_home="$(cd "${spark_dir}/../../..">/dev/null; pwd)"

# Clean packages
rm -rf "${spark_dir}/packages"
mkdir -p "${spark_dir}/packages"

cd ${gravitino_home}
${gravitino_home}/gradlew clean spark-connector:spark-connector-runtime:jar -x test
cp ${gravitino_home}/spark-connector/spark-connector-runtime/build/libs/gravitino-spark-connector-runtime-3.4* ${spark_dir}/packages/
