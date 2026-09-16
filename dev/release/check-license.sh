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

# This script checks that every file referenced by the LICENSE and NOTICE files is
# present in the project, and reports the ones that are not. For each missing file it
# prints the referencing line and any candidate locations found elsewhere in the tree,
# so the reference can be corrected.
#
# A reference is only checked when its first path segment is an existing directory in
# the project root. That keeps URLs, Java package names and paths internal to a bundled
# jar such as META-INF/NOTICE out of scope, since none of those name a file that is
# expected to exist here.
#
# The LICENSE.bin and NOTICE.bin variants are deliberately not checked: their paths
# describe the layout of the binary package assembled by build.gradle.kts rather than
# the source tree, so resolving them here would report failures that are not real.

FAILED=0

PROJECT_ROOT=${1:-$(dirname "$(dirname "$(dirname "$(readlink -f "$0")")")")}

cd "$PROJECT_ROOT" || exit 1

RED='\033[0;31m'
GREEN='\033[0;32m'
RESET='\033[0m'

check_document() {
  local document="$1"

  if [ ! -f "$document" ]; then
    echo -e "${RED}$document not found in $PROJECT_ROOT${RESET}"
    FAILED=1
    return
  fi

  local line_number=0
  local line token candidates
  # the '|| [ -n "$line" ]' guard keeps the last line when the file has no trailing newline
  while IFS= read -r line || [ -n "$line" ]; do
    line_number=$((line_number + 1))

    for token in $(echo "$line" | grep -oE '[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)+'); do
      token=${token#./}

      # only consider references rooted at a directory that exists here
      [ -d "${token%%/*}" ] || continue
      [ -e "$token" ] && continue

      FAILED=1
      echo -e "${RED}[NOT FOUND]${RESET} $document:$line_number --> $token"
      echo "    $line"

      # scan for the file name in the project root excluding the '.gradle' directory
      # and print any candidate locations found
      candidates=$(find . -type f -not -path '*/\.gradle/*' -name "$(basename "$token")")
      if [ -n "$candidates" ]; then
        echo "    candidates:"
        while IFS= read -r candidate; do
          echo "      $candidate"
        done <<< "$candidates"
      fi
    done
  done < "$document"
}

check_document LICENSE
check_document NOTICE

# check if any file is missing
if [ $FAILED -ne 0 ]; then
  echo -e "${RED}Some files referenced by the LICENSE or NOTICE files are missing.${RESET}"
  exit 1
fi
echo -e "${GREEN}All files referenced by the LICENSE and NOTICE files are present.${RESET}"
