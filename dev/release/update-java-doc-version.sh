#!/bin/bash
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

set -e

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <new_version>"
  exit 1
fi

NEW_VERSION=$1
cd "$(cd "$(dirname "$0")" && pwd)/../docs"
CURRENT_VERSION=`cat index.md| grep pathname:///docs | head -n 1 | awk -F '///docs' '{print $2}' | awk -F '/' '{print $2}'`

if [[ "${NEW_VERSION}" == "${CURRENT_VERSION}" ]]; then
  echo "The new version is the same as the current version."
  exit 1
fi

# Detect the operating system
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    find "$(pwd)" -name "*.md" | xargs sed -i '' "s|/docs/${CURRENT_VERSION}/api|/docs/${NEW_VERSION}/api|g"
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    find "$(pwd)" -name "*.md" | xargs sed -i "s|/docs/${CURRENT_VERSION}/api|/docs/${NEW_VERSION}/api|g"
else
    echo "Unsupported OS"
    exit 1
fi

echo "Update the version from ${CURRENT_VERSION} to ${NEW_VERSION} successfully."