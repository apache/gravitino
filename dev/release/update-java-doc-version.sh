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

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <new_version> [project_dir]"
  exit 1
fi

NEW_VERSION=$1
PROJECT_DIR=${2:-$(cd "$(dirname "$0")" && pwd)/../../}
cd "${PROJECT_DIR}/docs"
CURRENT_VERSION=`cat index.md| grep pathname:///docs | head -n 1 | awk -F '///docs' '{print $2}' | awk -F '/' '{print $2}'`

if [[ "${NEW_VERSION}" == "${CURRENT_VERSION}" ]]; then
  echo "The new version is the same as the current version: ${NEW_VERSION}"
  exit 0
fi

# Detect the operating system
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    find "$(pwd)" -name "*.md" | xargs sed -i '' "s|/docs/${CURRENT_VERSION}/api|/docs/${NEW_VERSION}/api|g"
    # modify open-api/openapi.yaml
    sed -i '' "s|version: ${CURRENT_VERSION}|version: ${NEW_VERSION}|g" open-api/openapi.yaml
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    find "$(pwd)" -name "*.md" | xargs sed -i "s|/docs/${CURRENT_VERSION}/api|/docs/${NEW_VERSION}/api|g"
    # modify open-api/openapi.yaml
    sed -i "s|version: ${CURRENT_VERSION}|version: ${NEW_VERSION}|g" open-api/openapi.yaml
else
    echo "Unsupported OS"
    exit 1
fi

echo "Update the version from ${CURRENT_VERSION} to ${NEW_VERSION} successfully."