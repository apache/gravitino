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

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 /path/to/rumdl" >&2
  exit 2
fi

rumdl_bin=$1
if [[ ! -x ${rumdl_bin} ]]; then
  echo "rumdl binary is not executable: ${rumdl_bin}" >&2
  exit 2
fi

root_dir=$(cd "$(dirname "$0")/../.." && pwd)
cd "${root_dir}"

fixtures_dir=dev/ci/markdownlint-fixtures
workflow=.github/workflows/design-docs-markdownlint.yml
config=.rumdl.toml
gradle_script=gradle/markdownlint.gradle.kts

expect_pass() {
  local cuj=$1
  local file=$2
  echo "${cuj}: ${file} must pass"
  "${rumdl_bin}" check --config "${config}" "${file}"
}

expect_fail_rule() {
  local cuj=$1
  local file=$2
  local rule=$3
  echo "${cuj}: ${file} must fail ${rule}"
  set +e
  local output status
  output=$("${rumdl_bin}" check --config "${config}" --fail-on any "${file}" 2>&1)
  status=$?
  set -e
  if [[ ${status} -eq 0 ]]; then
    echo "expected ${file} to fail" >&2
    echo "${output}" >&2
    exit 1
  fi
  if ! grep -q "${rule}" <<<"${output}"; then
    echo "expected ${rule} in rumdl output for ${file}" >&2
    echo "${output}" >&2
    exit 1
  fi
}

expect_pass_without_rule() {
  local cuj=$1
  local file=$2
  local rule=$3
  echo "${cuj}: ${file} must pass and not report ${rule}"
  local output
  output=$("${rumdl_bin}" check --config "${config}" "${file}" 2>&1)
  if grep -q "${rule}" <<<"${output}"; then
    echo "did not expect ${rule} in ${file}" >&2
    echo "${output}" >&2
    exit 1
  fi
}

expect_pass "C0" "${fixtures_dir}/README.md"
expect_pass "C1" "${fixtures_dir}/valid-table.md"
expect_fail_rule "C2" "${fixtures_dir}/unaligned-table.md" "MD060"

echo "C3: unaligned table in CI warn mode must exit 0"
"${rumdl_bin}" check --config "${config}" --fail-on never "${fixtures_dir}/unaligned-table.md" >/dev/null

expect_pass "C4" "${fixtures_dir}/ascii-diagram.md"
expect_pass_without_rule "C5" "${fixtures_dir}/wide-table.md" "MD013"
expect_fail_rule "C6" "${fixtures_dir}/long-prose.md" "MD013"
expect_fail_rule "C7" "${fixtures_dir}/missing-pipes.md" "MD055"

echo "C8: design-docs CI job stays warn-only"
grep -q -- '-Pmarkdownlint.failOn=never' "${workflow}"

echo "C9: production markdownlint task stays scoped to design-docs"
grep -q '"design-docs"' "${gradle_script}"

echo "markdownlint self-check passed"
