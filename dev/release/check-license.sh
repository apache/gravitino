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
# Explicit ./ references are always checked. Unprefixed paths are checked when their
# first segment is an existing project directory. URLs are ignored, as are unprefixed
# external paths such as META-INF/NOTICE. This is a source-reference sanity check, not
# a complete license audit; use ./ for references whose top-level directory may be missing.
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

  if [ ! -r "$document" ]; then
    echo -e "${RED}$document is not readable in $PROJECT_ROOT${RESET}"
    FAILED=1
    return
  fi

  local line_number=0
  local line token candidates candidate
  # the '|| [ -n "$line" ]' guard keeps the last line when the file has no trailing newline
  while IFS= read -r line || [ -n "$line" ]; do
    line_number=$((line_number + 1))

    while IFS= read -r token; do
      # A period terminating a sentence is not part of a file reference.
      token=${token%.}
      if [[ "$token" != ./* ]] && [ ! -d "${token%%/*}" ]; then
        continue
      fi
      token=${token#./}
      [ -f "$token" ] && continue

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
    done < <(printf '%s\n' "$line" |
      sed -E 's@[[:alpha:]][[:alnum:]+.-]*://[^[:space:]<>]+@@g' |
      grep -oE '[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)+')
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
