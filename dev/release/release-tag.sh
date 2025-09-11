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

# Referred from Apache Spark's release script
# dev/create-release/release-tag.sh

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-util.sh"

function exit_with_usage {
  local NAME=$(basename $0)
  cat << EOF
usage: $NAME
Tags a Gravitino release on a particular branch.

Inputs are specified with the following environment variables:
ASF_USERNAME - Apache Username
ASF_PASSWORD - Apache Password
GIT_NAME - Name to use with git
GIT_EMAIL - E-mail address to use with git
GIT_BRANCH - Git branch on which to make release
RELEASE_VERSION - Version used in pom files for release
RELEASE_TAG - Name of release tag
NEXT_VERSION - Development version after release
EOF
  exit 1
}

set -e
set -o pipefail

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

if [[ -z "$ASF_PASSWORD" ]]; then
  echo 'The environment variable ASF_PASSWORD is not set. Enter the password.'
  echo
  stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
fi

for env in ASF_USERNAME ASF_PASSWORD RELEASE_VERSION RELEASE_TAG NEXT_VERSION GIT_EMAIL GIT_NAME GIT_BRANCH; do
  if [ -z "${!env}" ]; then
    echo "$env must be set to run this script"
    exit 1
  fi
done

init_java
init_gradle

function uriencode { jq -nSRr --arg v "$1" '$v|@uri'; }

declare -r ENCODED_ASF_PASSWORD=$(uriencode "$ASF_PASSWORD")

rm -rf gravitino
git clone "https://$ASF_USERNAME:$ENCODED_ASF_PASSWORD@$ASF_GRAVITINO_REPO" -b $GIT_BRANCH
cd gravitino

git config user.name "$GIT_NAME"
git config user.email "$GIT_EMAIL"

PYGRAVITINO_RELEASE_VERSION="${RELEASE_VERSION/}"
PYGRAVITINO_NEXT_VERSION=$(echo $NEXT_VERSION | sed 's/-SNAPSHOT/.dev0/')

# Create release version for Java, Python ,Rust and Chart
sed -i".tmp1" 's/version = .*$/version = '"$RELEASE_VERSION"'/g' gradle.properties
sed -i".tmp2" 's/    version=.*$/    version="'"$PYGRAVITINO_RELEASE_VERSION"'",/g' clients/client-python/setup.py
sed -i".tmp3" 's/^version = .*$/version = \"'"$RELEASE_VERSION"'\"/g' clients/filesystem-fuse/Cargo.toml
sed -i".tmp4" 's/^appVersion: .*$/appVersion: '"$RELEASE_VERSION"'/g' dev/charts/gravitino/Chart.yaml

if [[ $(sed -n '34p' dev/charts/gravitino/values.yaml) =~ ^"  tag: " ]]; then
  sed -i".tmp5" '34s/  tag: .*$/  tag: '"$RELEASE_VERSION"'/g' dev/charts/gravitino/values.yaml
else
  echo "Error: Could not find 'tag:' in line 34 of dev/charts/gravitino/values.yaml"
  exit 1
fi

sed -i".tmp6" 's/^appVersion: .*$/appVersion: '"$RELEASE_VERSION"'/g' dev/charts/gravitino-iceberg-rest-server/Chart.yaml

if [[ $(sed -n '24p' dev/charts/gravitino-iceberg-rest-server/values.yaml) =~ ^"  tag: " ]]; then
  sed -i".tmp7" '24s/  tag: .*$/  tag: '"$RELEASE_VERSION"'/g' dev/charts/gravitino-iceberg-rest-server/values.yaml
else
  echo "Error: Could not find 'tag:' in line 24 of dev/charts/gravitino-iceberg-rest-server/values.yaml"
  exit 1
fi

sed -i".tmp8" 's/^version = .*$/version = "'"$PYGRAVITINO_RELEASE_VERSION"'"/g' mcp-server/pyproject.toml

CHART_VERSION=$(grep -e '^version: .*' dev/charts/gravitino/Chart.yaml | cut -d':' -f2 | sed 's/^ *//;s/ *$//')
CHART_SHORT_VERSION=$(echo "$CHART_VERSION" | cut -d . -f 1-2)
CHART_REV=$(echo "$CHART_VERSION" | cut -d . -f 3 | cut -d '-' -f 1)
CHART_REV=$((CHART_REV + 1))
NEXT_CHART_VERSION="${CHART_SHORT_VERSION}.${CHART_REV}"
sed -i".tmp9" 's/^version: .*$/version: '"$NEXT_CHART_VERSION"'/g' dev/charts/gravitino/Chart.yaml

IRC_CHART_VERSION=$(grep -e '^version: .*' dev/charts/gravitino-iceberg-rest-server/Chart.yaml | cut -d':' -f2 | sed 's/^ *//;s/ *$//')
IRC_CHART_SHORT_VERSION=$(echo "$IRC_CHART_VERSION" | cut -d . -f 1-2)
IRC_CHART_REV=$(echo "$IRC_CHART_VERSION" | cut -d . -f 3 | cut -d '-' -f 1)
IRC_CHART_REV=$((IRC_CHART_REV + 1))
NEXT_IRC_CHART_VERSION="${IRC_CHART_SHORT_VERSION}.${IRC_CHART_REV}"
sed -i".tmp10" 's/^version: .*$/version: '"$NEXT_IRC_CHART_VERSION"'/g' dev/charts/gravitino-iceberg-rest-server/Chart.yaml

# update docs version
"$SELF/update-java-doc-version.sh" "$RELEASE_VERSION" "$SELF/gravitino"

git commit -a -m "Preparing Gravitino release $RELEASE_TAG"
echo "Creating tag $RELEASE_TAG at the head of $GIT_BRANCH"
git tag $RELEASE_TAG

# Create next version
sed -i".tmp11" 's/version = .*$/version = '"$NEXT_VERSION"'/g' gradle.properties
sed -i".tmp12" 's/    version=.*$/    version="'"$PYGRAVITINO_NEXT_VERSION"'",/g' clients/client-python/setup.py
sed -i".tmp13" 's/^version = .*$/version = \"'"$NEXT_VERSION"'\"/g' clients/filesystem-fuse/Cargo.toml
sed -i".tmp14" 's/appVersion: .*$/appVersion: '"$NEXT_VERSION"'/g' dev/charts/gravitino/Chart.yaml
sed -i".tmp15" '34s/  tag: .*$/  tag: '"$NEXT_VERSION"'/' dev/charts/gravitino/values.yaml
CHART_REV=$((CHART_REV + 1))
NEXT_CHART_VERSION="${CHART_SHORT_VERSION}.${CHART_REV}"
sed -i".tmp16" 's/^version: .*$/version: '"$NEXT_CHART_VERSION"'/g' dev/charts/gravitino/Chart.yaml

IRC_CHART_REV=$((IRC_CHART_REV + 1))
NEXT_IRC_CHART_VERSION="${IRC_CHART_SHORT_VERSION}.${IRC_CHART_REV}"
sed -i".tmp17" 's/appVersion: .*$/appVersion: '"$NEXT_VERSION"'/g' dev/charts/gravitino-iceberg-rest-server/Chart.yaml
sed -i".tmp18" '24s/  tag: .*$/  tag: '"$NEXT_VERSION"'/' dev/charts/gravitino-iceberg-rest-server/values.yaml
sed -i".tmp19" 's/^version: .*$/version: '"$NEXT_IRC_CHART_VERSION"'/g' dev/charts/gravitino-iceberg-rest-server/Chart.yaml
sed -i".tmp20" 's/^version = .*$/version = "'"$PYGRAVITINO_NEXT_VERSION"'"/g' mcp-server/pyproject.toml

git commit -a -m "Preparing development version $NEXT_VERSION"

if ! is_dry_run; then
  # Push changes
  git push origin $RELEASE_TAG
  if [[ $RELEASE_VERSION != *"preview"* ]]; then
    git push origin HEAD:$GIT_BRANCH
    if git branch -r --contains tags/$RELEASE_TAG | grep origin; then
      echo "Pushed $RELEASE_TAG to $GIT_BRANCH."
    else
      echo "Failed to push $RELEASE_TAG to $GIT_BRANCH. Please start over."
      exit 1
    fi
  else
    echo "It's preview release. We only push $RELEASE_TAG to remote."
  fi

  cd ..
  rm -fr gravitino
else
  cd ..
  rm -rf gravitino-tag
  mv gravitino gravitino-tag
  echo "Clone with version changes and tag available as gravitino-tag in the output directory."
fi
