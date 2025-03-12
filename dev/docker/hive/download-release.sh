#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The first parameter is the version of the release, e.g. 2.4.0
# The second parameter is the name of the asset file, e.g. ranger-2.4.0-admin.tar.gz
# The third parameter is the location and name of the output file, e.g. /tmp/ranger-2.4.0-admin.tar.gz

set -ex

if [ $# -ne 3 ]; then
  echo "Usage: $0 <version> <asset-file> <output-file>"
  exit 1
fi


TOKEN=${PRIVATE_ACCESS_TOKEN}
REPO="datastrato/ranger"
FILE=$2                          # the name of your release asset file, e.g. build.tar.gz
VERSION=$1                       # tag name or the word "latest"
GITHUB="https://api.github.com"

alias err_echo='>&2 echo'

function gh_curl() {
  curl -H "Authorization: token $TOKEN" \
       -H "Accept: application/vnd.github.v3.raw" \
       $@
}

if [ "$VERSION" = "latest" ]; then
  # Github should return the latest release first.
  parser=".[0].assets | map(select(.name == \"$FILE\"))[0].id"
else
  parser=". | map(select(.tag_name == \"$VERSION\"))[0].assets | map(select(.name == \"$FILE\"))[0].id"
fi;

asset_id=`gh_curl -s $GITHUB/repos/$REPO/releases | jq "$parser"`
if [ "$asset_id" = "null" ]; then
  err_echo "ERROR: version not found $VERSION"
  exit 1
fi;

echo $2

curl -sL --header "Authorization: token $TOKEN" --header 'Accept: application/octet-stream' https://$TOKEN:@api.github.com/repos/$REPO/releases/assets/$asset_id -o $3
