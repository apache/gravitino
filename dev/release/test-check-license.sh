#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Run with: bash dev/release/test-check-license.sh
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CHECKER="$SCRIPT_DIR/check-license.sh"
TEST_ROOT=$(mktemp -d)
trap 'rm -rf "$TEST_ROOT"' EXIT
PROJECT_ROOT="$TEST_ROOT/project with spaces"
mkdir -p "$PROJECT_ROOT/web" "$PROJECT_ROOT/licenses" "$PROJECT_ROOT/alternative"
touch "$PROJECT_ROOT/web/LICENSE" "$PROJECT_ROOT/web/NOTICE"
touch "$PROJECT_ROOT/licenses/example.txt" "$PROJECT_ROOT/alternative/missing.txt"

check_result() {
  local expected_status="$1" expected_output="$2" status=0
  bash "$CHECKER" "$PROJECT_ROOT" > "$TEST_ROOT/output" 2>&1 || status=$?
  if [ "$status" -ne "$expected_status" ] || ! grep -Fq "$expected_output" "$TEST_ROOT/output"; then
    echo "FAIL: $CASE (expected exit $expected_status, got $status)"
    cat "$TEST_ROOT/output"
    exit 1
  fi
  echo "PASS: $CASE"
}

CASE='valid explicit and prose references, punctuation and multiple paths'
cat > "$PROJECT_ROOT/LICENSE" <<'EOF'
See web/LICENSE, (web/NOTICE); `licenses/example.txt`.
See web/NOTICE.
./licenses/example.txt
EOF
printf '%s' 'See web/NOTICE' > "$PROJECT_ROOT/NOTICE"
check_result 0 'All files referenced'

CASE='missing unprefixed file in prose'
printf '%s\n' 'See web/missing.txt' > "$PROJECT_ROOT/LICENSE"
check_result 1 'LICENSE:1 --> web/missing.txt'
grep -Fq './alternative/missing.txt' "$TEST_ROOT/output"

CASE='explicit reference with missing top-level directory'
printf '%s\n' './absent/LICENSE.txt' > "$PROJECT_ROOT/LICENSE"
check_result 1 'LICENSE:1 --> absent/LICENSE.txt'

CASE='missing reference on final line without newline'
printf '%s' './web/missing.txt' > "$PROJECT_ROOT/LICENSE"
check_result 1 'LICENSE:1 --> web/missing.txt'

CASE='missing NOTICE reference and line number'
printf '%s\n' 'No references' > "$PROJECT_ROOT/LICENSE"
printf '%s\n' 'First line' 'See web/missing.txt.' > "$PROJECT_ROOT/NOTICE"
check_result 1 'NOTICE:2 --> web/missing.txt'

CASE='directories are not files'
printf '%s\n' './web' > "$PROJECT_ROOT/LICENSE"
: > "$PROJECT_ROOT/NOTICE"
check_result 1 'LICENSE:1 --> web'

CASE='URLs and external paths are not source references'
cat > "$PROJECT_ROOT/LICENSE" <<'EOF'
https://web/missing.txt https://example.org/web/missing.txt
(https://web/missing.txt) https://example.org/?path=web/missing.txt
ftp://licenses/missing.txt file:///web/missing.txt
META-INF/NOTICE org.example.Class Kyligence/kylinpy
EOF
check_result 0 'All files referenced'

CASE='local reference after URL is still checked'
printf '%s\n' 'https://web/remote.txt and web/missing.txt' > "$PROJECT_ROOT/LICENSE"
check_result 1 'LICENSE:1 --> web/missing.txt'

CASE='binary package references remain out of scope'
: > "$PROJECT_ROOT/LICENSE"
printf '%s\n' './absent/binary.txt' > "$PROJECT_ROOT/LICENSE.bin"
printf '%s\n' './absent/binary.txt' > "$PROJECT_ROOT/NOTICE.bin"
check_result 0 'All files referenced'

CASE='missing LICENSE document'
rm "$PROJECT_ROOT/LICENSE"
check_result 1 'LICENSE not found'

CASE='missing NOTICE document'
touch "$PROJECT_ROOT/LICENSE"
rm "$PROJECT_ROOT/NOTICE"
check_result 1 'NOTICE not found'

CASE='unreadable source document'
touch "$PROJECT_ROOT/NOTICE"
chmod 000 "$PROJECT_ROOT/NOTICE"
if [ -r "$PROJECT_ROOT/NOTICE" ]; then
  echo "SKIP: $CASE (current user can read mode-000 files)"
else
  check_result 1 'NOTICE is not readable'
fi
chmod 600 "$PROJECT_ROOT/NOTICE"

CASE='default project root resolved relative to script'
mkdir -p "$PROJECT_ROOT/dev/release"
cp "$CHECKER" "$PROJECT_ROOT/dev/release/check-license.sh"
bash "$PROJECT_ROOT/dev/release/check-license.sh" > "$TEST_ROOT/output" 2>&1
grep -Fq 'All files referenced' "$TEST_ROOT/output"
echo "PASS: $CASE"
