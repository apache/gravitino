#!/bin/bash
#
# Copyright 2023 Datastrato.
# This software is licensed under the Apache License version 2.
#
set -ex
gravitino_dir="$(dirname "${BASH_SOURCE-$0}")"
gravitino_dir="$(cd "${gravitino_dir}">/dev/null; pwd)"
gravitino_home="$(cd "${gravitino_dir}/../../..">/dev/null; pwd)"

# Prepare compile Gravitino packages
${gravitino_home}/gradlew compileDistribution -x test

# Removed old packages, Avoid multiple re-executions using the wrong file
rm -rf "${gravitino_dir}/packages"
mkdir -p "${gravitino_dir}/packages"

cp -r "${gravitino_home}/distribution/package" "${gravitino_dir}/packages/gravitino"

# Let gravitino.sh can not quit
cat <<EOF >> "${gravitino_dir}/packages/gravitino/bin/gravitino.sh"

# persist the container
tail -f /dev/null
EOF
