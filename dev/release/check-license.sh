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

# This script checks if all the files mentioned in the LICENSE file are present in the project.
# Besides highlighting the missing files, it will display possible locations for any new files.
# It adds text like [NOT FOUND] --> "new/path/to/the/file" in the end of the line where the file is missing.
# With the output of this script, you can update the LICENSE file with the correct file paths.

FAILED=0

PROJECT_ROOT=${1:-$(dirname $(dirname $(dirname $(readlink -f "$0"))))}
LICENSE_FILE=$(cat "$PROJECT_ROOT"/LICENSE)

cd "$PROJECT_ROOT" || exit 1

while IFS= read -r line; do
  echo -n "$line"

  line=$(echo "$line" | xargs)
  # check if the line is a file
  if [[ "$line" == "./"* && "$line" == *"."* ]]; then
    line=$(echo "$line" | cut -c 3-)

    # check if the file does not exists
    if [ ! -f "$line" ]; then
      FAILED=1
      echo -n -e " \033[0;31m[NOT FOUND]\033[0m --> "

      file_name=$(basename "$line")
      # scan for the file name in the project root excluding the '.gradle' directory
      # and print the new file path if found in the project
      find . -type f -not -path '*/\.gradle/*' -name "$file_name" | tr -d '\n'
    fi
  fi
  echo

done <<< "$LICENSE_FILE"

# check if any file is missing
if [ $FAILED -ne 0 ]; then
  echo -e "\033[0;31mSome files listed in the LICENSE file are missing. \033[0m"
  exit 1
fi
echo -e "\033[0;32mAll files listed in the LICENSE file are present. \033[0m"
