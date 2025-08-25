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
# dev/create-release/release-build.sh

SELF=$(cd $(dirname $0) && pwd)
. "$SELF/release-util.sh"

function exit_with_usage {
  cat << EOF
usage: release-build.sh <package|docs|publish-release|finalize>
Creates build deliverables from a Apache Gravitino commit.

Top level targets are
  package: Create binary packages and commit them to dist.apache.org/repos/dist/dev/gravitino/
  docs: Build the Java and Python docs, and copy them to a local path.
  publish-release: Publish a release to Apache release repo
  finalize: Finalize the release after an RC passes vote

All other inputs are environment variables

GIT_REF - Release tag or commit to build from
GRAVITINO_PACKAGE_VERSION - Release identifier in top level package directory (e.g. 0.10.0-rc1)
GRAVITINO_VERSION - (optional) Version of Gravitino being built (e.g. 0.10.0)

ASF_USERNAME - Username of ASF committer account
ASF_PASSWORD - Password of ASF committer account

GPG_KEY - GPG key used to sign release artifacts
GPG_PASSPHRASE - Passphrase for GPG key
EOF
  exit 1
}

set -e

if [ $# -eq 0 ]; then
  exit_with_usage
fi

if [[ $@ == *"help"* ]]; then
  exit_with_usage
fi

if [[ -z "$ASF_PASSWORD" ]]; then
  echo 'The environment variable ASF_PASSWORD is not set. Enter the password.'
  echo
  stty -echo && printf "ASF password: " && read ASF_PASSWORD && printf '\n' && stty echo
fi

if [[ -z "$GPG_PASSPHRASE" ]]; then
  echo 'The environment variable GPG_PASSPHRASE is not set. Enter the passphrase to'
  echo 'unlock the GPG signing key that will be used to sign the release!'
  echo
  stty -echo && printf "GPG passphrase: " && read GPG_PASSPHRASE && printf '\n' && stty echo
fi

for env in ASF_USERNAME GPG_PASSPHRASE GPG_KEY; do
  if [ -z "${!env}" ]; then
    echo "ERROR: $env must be set to run this script"
    exit_with_usage
  fi
done

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

# Commit ref to checkout when building
GIT_REF=${GIT_REF:-main}

RELEASE_STAGING_LOCATION="https://dist.apache.org/repos/dist/dev/gravitino"
RELEASE_LOCATION="https://dist.apache.org/repos/dist/release/gravitino"

GPG="gpg -u $GPG_KEY --no-tty --batch --pinentry-mode loopback"
NEXUS_ROOT=https://repository.apache.org/service/local/staging
NEXUS_PROFILE=170bb01395e84a # Profile for Gravitino staging uploads
BASE_DIR=$(pwd)

init_java
init_gradle

function uriencode { jq -nSRr --arg v "$1" '$v|@uri'; }
declare -r ENCODED_ASF_PASSWORD=$(uriencode "$ASF_PASSWORD")

if [[ "$1" == "finalize" ]]; then
  if [[ -z "$PYPI_API_TOKEN" ]]; then
    error 'The environment variable PYPI_API_TOKEN is not set. Exiting.'
  fi

  git config --global user.name "$GIT_NAME"
  git config --global user.email "$GIT_EMAIL"

  # Create the git tag for the new release
  echo "Creating the git tag for the new release"
  if check_for_tag "v$RELEASE_VERSION"; then
    echo "v$RELEASE_VERSION already exists. Skip creating it."
  else
    rm -rf gravitino
    git clone "https://$ASF_USERNAME:$ENCODED_ASF_PASSWORD@$ASF_GRAVITINO_REPO" -b main
    cd gravitino
    git tag "v$RELEASE_VERSION" "$RELEASE_TAG"
    git push origin "v$RELEASE_VERSION"
    cd ..
    rm -rf gravitino
    echo "git tag v$RELEASE_VERSION created"
  fi

  PYGRAVITINO_VERSION="${RELEASE_VERSION}"
  git clone "https://$ASF_USERNAME:$ENCODED_ASF_PASSWORD@$ASF_GRAVITINO_REPO" -b "v$RELEASE_VERSION"
  cd gravitino
  $GRADLE :clients:client-python:distribution -x test
  cd ..
  cp gravitino/clients/client-python/dist/apache_gravitino-$PYGRAVITINO_VERSION.tar.gz .
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
    --output apache_gravitino-$PYGRAVITINO_VERSION.tar.gz.asc \
    --detach-sig apache_gravitino-$PYGRAVITINO_VERSION.tar.gz

  # upload to PyPi.
  echo "Uploading Gravitino to PyPi"
  twine upload -u __token__  -p $PYPI_API_TOKEN \
    --repository-url https://upload.pypi.org/legacy/ \
    "apache_gravitino-$PYGRAVITINO_VERSION.tar.gz" \
    "apache_gravitino-$PYGRAVITINO_VERSION.tar.gz.asc"
  echo "Python Gravitino package uploaded"
  rm -fr gravitino

  # Moves the binaries from dev directory to release directory.
  echo "Moving Gravitino binaries to the release directory"
  svn mv --username "$ASF_USERNAME" --password "$ASF_PASSWORD" -m"Apache Gravitino $RELEASE_VERSION" \
    --no-auth-cache "$RELEASE_STAGING_LOCATION/$RELEASE_TAG" "$RELEASE_LOCATION/$RELEASE_VERSION"
  echo "Gravitino binaries moved"

  # Update the KEYS file.
  echo "Sync'ing KEYS"
  svn co --depth=files "$RELEASE_LOCATION" svn-gravitino
  curl "$RELEASE_STAGING_LOCATION/KEYS" > svn-gravitino/KEYS
  (cd svn-gravitino && svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Update KEYS")
  echo "KEYS sync'ed"
  rm -rf svn-gravitino

  exit 0
fi

rm -rf gravitino
git clone "$ASF_REPO"
cd gravitino
git checkout $GIT_REF
git_hash=`git rev-parse --short HEAD`
export GIT_HASH=$git_hash
echo "Checked out Gravitino git hash $git_hash"

if [ -z "$GRAVITINO_VERSION" ]; then
  GRAVITINO_VERSION=$(cat gradle.properties | parse_version)
fi

if [ -z "$PYGRAVITINO_VERSION"]; then
  PYGRAVITINO_VERSION=$(cat clients/client-python/setup.py | grep "version=" | awk -F"\"" '{print $2}')
fi

if [[ "$PYGRAVITINO_VERSION" == *"dev"* ]]; then
  RC_PYGRAVITINO_VERSION="${PYGRAVITINO_VERSION}"
else
  if [ -z "$RC_COUNT" ]; then
    echo "ERROR: RC_COUNT must be set to run this script"
    exit_with_usage
  fi
  RC_PYGRAVITINO_VERSION="${PYGRAVITINO_VERSION}rc${RC_COUNT}"
fi

# This is a band-aid fix to avoid the failure of Maven nightly snapshot in some Jenkins
# machines by explicitly calling /usr/sbin/lsof.
LSOF=lsof
if ! hash $LSOF 2>/dev/null; then
  LSOF=/usr/sbin/lsof
fi

if [ -z "$GRAVITINO_PACKAGE_VERSION" ]; then
  GRAVITINO_PACKAGE_VERSION="${GRAVITINO_VERSION}-$(date +%Y_%m_%d_%H_%M)-${git_hash}"
fi

DEST_DIR_NAME="$GRAVITINO_PACKAGE_VERSION"

git clean -d -f -x
cd ..

if [[ "$1" == "package" ]]; then
  # Source and binary tarballs
  echo "Packaging release source tarballs"
  cp -r gravitino gravitino-$GRAVITINO_VERSION-src

  rm -f gravitino-$GRAVITINO_VERSION-src/LICENSE.bin
  rm -f gravitino-$GRAVITINO_VERSION-src/NOTICE.bin
  rm -f gravitino-$GRAVITINO_VERSION-src/LICENSE.rest
  rm -f gravitino-$GRAVITINO_VERSION-src/NOTICE.rest
  rm -f gravitino-$GRAVITINO_VERSION-src/LICENSE.trino
  rm -f gravitino-$GRAVITINO_VERSION-src/NOTICE.trino
  rm -f gravitino-$GRAVITINO_VERSION-src/web/web/LICENSE.bin
  rm -f gravitino-$GRAVITINO_VERSION-src/web/web/NOTICE.bin

  rm -f *.asc
  tar cvzf gravitino-$GRAVITINO_VERSION-src.tar.gz --exclude gravitino-$GRAVITINO_VERSION-src/.git gravitino-$GRAVITINO_VERSION-src
  echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour --output gravitino-$GRAVITINO_VERSION-src.tar.gz.asc \
    --detach-sig gravitino-$GRAVITINO_VERSION-src.tar.gz
  shasum -a 512 gravitino-$GRAVITINO_VERSION-src.tar.gz > gravitino-$GRAVITINO_VERSION-src.tar.gz.sha512
  rm -rf gravitino-$GRAVITINO_VERSION-src

  # Updated for each binary build
  make_binary_release() {
    echo "Building Gravitino binary dist"
    cp -r gravitino gravitino-$GRAVITINO_VERSION-bin
    cd gravitino-$GRAVITINO_VERSION-bin

    echo "Creating distribution"

    sed -i".tmp3" 's/    version=.*$/    version="'"$RC_PYGRAVITINO_VERSION"'",/g' clients/client-python/setup.py
    $GRADLE assembleDistribution -x test
    $GRADLE :clients:client-python:distribution -x test
    cd ..

    echo "Copying and signing Gravitino server binary distribution"
    cp gravitino-$GRAVITINO_VERSION-bin/distribution/gravitino-$GRAVITINO_VERSION-bin.tar.gz .
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
      --output gravitino-$GRAVITINO_VERSION-bin.tar.gz.asc \
      --detach-sig gravitino-$GRAVITINO_VERSION-bin.tar.gz
    shasum -a 512 gravitino-$GRAVITINO_VERSION-bin.tar.gz > gravitino-$GRAVITINO_VERSION-bin.tar.gz.sha512

    echo "Copying and signing Gravitino Iceberg REST server binary distribution"
    cp gravitino-$GRAVITINO_VERSION-bin/distribution/gravitino-iceberg-rest-server-$GRAVITINO_VERSION-bin.tar.gz .
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
      --output gravitino-iceberg-rest-server-$GRAVITINO_VERSION-bin.tar.gz.asc \
      --detach-sig gravitino-iceberg-rest-server-$GRAVITINO_VERSION-bin.tar.gz
    shasum -a 512 gravitino-iceberg-rest-server-$GRAVITINO_VERSION-bin.tar.gz > gravitino-iceberg-rest-server-$GRAVITINO_VERSION-bin.tar.gz.sha512

    echo "Copying and signing Gravitino Trino connector binary distribution"
    cp gravitino-$GRAVITINO_VERSION-bin/distribution/gravitino-trino-connector-$GRAVITINO_VERSION.tar.gz .
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
      --output gravitino-trino-connector-$GRAVITINO_VERSION.tar.gz.asc \
      --detach-sig gravitino-trino-connector-$GRAVITINO_VERSION.tar.gz
    shasum -a 512 gravitino-trino-connector-$GRAVITINO_VERSION.tar.gz > gravitino-trino-connector-$GRAVITINO_VERSION.tar.gz.sha512

    echo "Copying and signing Gravitino Python client binary distribution"
    cp gravitino-$GRAVITINO_VERSION-bin/clients/client-python/dist/apache_gravitino-$RC_PYGRAVITINO_VERSION.tar.gz .
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --armour \
      --output apache_gravitino-$RC_PYGRAVITINO_VERSION.tar.gz.asc \
      --detach-sig apache_gravitino-$RC_PYGRAVITINO_VERSION.tar.gz
    shasum -a 512 apache_gravitino-$RC_PYGRAVITINO_VERSION.tar.gz > apache_gravitino-$RC_PYGRAVITINO_VERSION.tar.gz.sha512
  }

  make_binary_release
  rm -rf gravitino-$GRAVITINO_VERSION-bin/

  if ! is_dry_run; then
    if [[ -z "$PYPI_API_TOKEN" ]]; then
      error 'The environment variable PYPI_API_TOKEN is not set. Exiting.'
    fi

    echo "Uploading Gravitino Python package $RC_RC_PYGRAVITINO_VERSION to PyPi"
    twine upload -u __token__  -p $PYPI_API_TOKEN \
      --repository-url https://upload.pypi.org/legacy/ \
      "apache_gravitino-$RC_PYGRAVITINO_VERSION.tar.gz" \
      "apache_gravitino-$RC_PYGRAVITINO_VERSION.tar.gz.asc"

    svn co --depth=empty $RELEASE_STAGING_LOCATION svn-gravitino
    rm -rf "svn-gravitino/${DEST_DIR_NAME}"
    mkdir -p "svn-gravitino/${DEST_DIR_NAME}"

    echo "Copying release tarballs"
    cp gravitino-* "svn-gravitino/${DEST_DIR_NAME}/"
    svn add "svn-gravitino/${DEST_DIR_NAME}"

    cd svn-gravitino
    svn ci --username $ASF_USERNAME --password "$ASF_PASSWORD" -m"Apache Gravitino $GRAVITINO_PACKAGE_VERSION" --no-auth-cache
    cd ..
    rm -rf svn-gravitino
  fi

  exit 0
fi

if [[ "$1" == "docs" ]]; then
  # Documentation
  cp -r gravitino gravitino-$GRAVITINO_VERSION-docs
  cd gravitino-$GRAVITINO_VERSION-docs
  echo "Building Gravitino Java and Python docs"
  $GRADLE :clients:client-java:build -x test
  $GRADLE :clients:client-python:doc
  cd ..

  cp -r gravitino-$GRAVITINO_VERSION-docs/clients/client-java/build/docs/javadoc gravitino-$GRAVITINO_VERSION-javadoc
  cp -r gravitino-$GRAVITINO_VERSION-docs/clients/client-python/docs/build/html gravitino-$PYGRAVITINO_VERSION-pydoc

  rm -fr gravitino-$GRAVITINO_VERSION-docs
fi

if [[ "$1" == "publish-release" ]]; then
  # Publish Gravitino to Maven release repo
  echo "Publishing Gravitino checkout at '$GIT_REF' ($git_hash)"
  echo "Publish version is $GRAVITINO_VERSION"

  cp -r gravitino gravitino-$GRAVITINO_VERSION-publish
  cd gravitino-$GRAVITINO_VERSION-publish

  # Using Nexus API documented here:
  # https://support.sonatype.com/hc/en-us/articles/213465868-Uploading-to-a-Nexus-Repository-2-Staging-Repository-via-REST-API
  if ! is_dry_run; then
    echo "Creating Nexus staging repository"
    repo_request="<promoteRequest><data><description>Apache Gravitino $GRAVITINO_VERSION (commit $git_hash)</description></data></promoteRequest>"
    out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
      -H "Content-Type:application/xml" -v \
      $NEXUS_ROOT/profiles/$NEXUS_PROFILE/start)
    staged_repo_id=$(echo $out | sed -e "s/.*\(orgapachegravitino-[0-9]\{4\}\).*/\1/")
    echo "Created Nexus staging repository: $staged_repo_id"
  fi

  tmp_dir=$(mktemp -d gravitino-repo-XXXXX)
  # the following recreates `readlink -f "$tmp_dir"` since readlink -f is unsupported on MacOS
  cd $tmp_dir
  tmp_repo=$(pwd)
  cd ..

  $GRADLE clean
  $GRADLE release -x test -PdefaultScalaVersion=2.12
  $GRADLE release -x test -PdefaultScalaVersion=2.13

  $GRADLE -Dmaven.repo.local=$tmp_repo publishToMavenLocal -PdefaultScalaVersion=2.12
  $GRADLE -Dmaven.repo.local=$tmp_repo publishToMavenLocal -PdefaultScalaVersion=2.13

  pushd $tmp_repo/org/apache/gravitino

  # Remove any extra files generated during install
  find . -type f |grep -v \.jar |grep -v \.pom |grep -v cyclonedx | grep -v \.module | xargs rm

  echo "Creating hash and signature files"
  # this must have .asc, .md5 and .sha1 - it really doesn't like anything else there
  for file in $(find . -type f)
  do
    echo $GPG_PASSPHRASE | $GPG --passphrase-fd 0 --output $file.asc \
      --detach-sig --armour $file;
    if [ $(command -v md5) ]; then
      # Available on OS X; -q to keep only hash
      md5 -q $file > $file.md5
    else
      # Available on Linux; cut to keep only hash
      md5sum $file | cut -f1 -d' ' > $file.md5
    fi
    sha1sum $file | cut -f1 -d' ' > $file.sha1
  done

  if ! is_dry_run; then
    nexus_upload=$NEXUS_ROOT/deployByRepositoryId/$staged_repo_id
    echo "Uploading files to $nexus_upload"
    for file in $(find . -type f -not -path "./docs/*" -not -path "./web/*" -not -path "./integration-test-common/*" -not -path "./integration-test/*")
    do
      # strip leading ./
      file_short=$(echo $file | sed -e "s/\.\///")
      dest_url="$nexus_upload/org/apache/gravitino/$file_short"
      echo "  Uploading $file_short"
      curl --retry 3 --retry-all-errors -u $ASF_USERNAME:$ASF_PASSWORD --upload-file $file_short $dest_url
    done

    echo "Closing nexus staging repository"
    repo_request="<promoteRequest><data><stagedRepositoryId>$staged_repo_id</stagedRepositoryId><description>Apache Gravitino $GRAVITINO_VERSION (commit $git_hash)</description></data></promoteRequest>"
    out=$(curl -X POST -d "$repo_request" -u $ASF_USERNAME:$ASF_PASSWORD \
      -H "Content-Type:application/xml" -v \
      $NEXUS_ROOT/profiles/$NEXUS_PROFILE/finish)
    echo "Closed Nexus staging repository: $staged_repo_id"
  fi

  popd
  rm -fr $tmp_repo
  cd ..
  rm -rf gravitino-$GRAVITINO_VERSION-publish
fi
