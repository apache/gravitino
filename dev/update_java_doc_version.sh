#!/bin/bash

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