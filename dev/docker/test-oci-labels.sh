#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Regression check for OCI label argv preservation in build-docker.sh.
# A whitespace-containing OCI_IMAGE_VERSION must remain one --label value.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
build_docker="${script_dir}/build-docker.sh"

if ! grep -F 'oci_labels=(' "${build_docker}" >/dev/null; then
  echo "ERROR: build-docker.sh must store OCI labels in a Bash array (oci_labels=())" >&2
  exit 1
fi
if ! grep -F '"${oci_labels[@]}"' "${build_docker}" >/dev/null; then
  echo "ERROR: build-docker.sh must expand OCI labels as \"\${oci_labels[@]}\"" >&2
  exit 1
fi
if grep -E '\$\{oci_labels\}([^[@]|$)' "${build_docker}" >/dev/null; then
  echo "ERROR: build-docker.sh still expands \${oci_labels} as a string" >&2
  exit 1
fi

# Mirror the construction in build-docker.sh and assert argv slots.
oci_version='2.0 custom'
oci_revision='deadbeef'
oci_source='https://github.com/apache/gravitino'
oci_labels=(
  --label "org.opencontainers.image.version=${oci_version}"
  --label "org.opencontainers.image.revision=${oci_revision}"
  --label "org.opencontainers.image.source=${oci_source}"
)

set -- "${oci_labels[@]}"
if [ "$#" -ne 6 ]; then
  echo "ERROR: expected 6 argv slots for three --label pairs, got $#" >&2
  printf '  arg: %q\n' "$@" >&2
  exit 1
fi
if [ "$2" != "org.opencontainers.image.version=2.0 custom" ]; then
  echo "ERROR: version label was split or altered: $2" >&2
  exit 1
fi
if [ "$4" != "org.opencontainers.image.revision=deadbeef" ]; then
  echo "ERROR: revision label was altered: $4" >&2
  exit 1
fi
if [ "$6" != "org.opencontainers.image.source=https://github.com/apache/gravitino" ]; then
  echo "ERROR: source label was altered: $6" >&2
  exit 1
fi

# Contrast: unquoted string expansion (the old bug) splits on whitespace.
old_style="--label org.opencontainers.image.version=${oci_version} --label org.opencontainers.image.revision=${oci_revision}"
# shellcheck disable=SC2086
set -- ${old_style}
if [ "$#" -le 4 ]; then
  echo "ERROR: expected old-style unquoted expansion to split on whitespace (got $# args)" >&2
  exit 1
fi

echo "OK: OCI label array preserves whitespace-containing values as one argument"
exit 0
