#!/bin/bash
#
# Copyright 2023 DATASTRATO Pvt Ltd.
# This software is licensed under the Apache License version 2.
#
set -ex
bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

USAGE="Usage: launch-docs-website.sh [update]"

OS=$(uname -s)
if [ "$OS" == "Darwin" ]; then
  HUGO_OS_NAME="darwin-universal"
elif [[ "$OS" == "Linux" ]]; then
  HUGO_OS_NAME="linux-amd64"
else
  echo "Currently only support macOS or Linux."
  exit 1
fi

# Download Hugo
HUGO_VERSION="0.119.0"
HUGO_PACKAGE_NAME="hugo_${HUGO_VERSION}_${HUGO_OS_NAME}.tar.gz"
HUGO_DOWNLOAD_URL="https://github.com/gohugoio/hugo/releases/download/v${HUGO_VERSION}/${HUGO_PACKAGE_NAME}"

# Because Markdown embedded images are relative paths,
# Hugo website images are absolute paths.
function hugo_image_path_process() {
  for file in ${1}/*; do
    if [[ -d ${file} ]]; then
      hugo_image_path_process "${file}"
    elif [[ -f ${file} ]]; then
      # Replace `![](assets/` with `![](/assets/`, so that the images can be displayed correctly in the Hugo website.
      # macOS's sed is not GNU standard sed, it's -i option needs to be followed by a string. So
      # using if...else to distinguish between macOS and Linux.
      if [[ "$OS" == "Darwin" ]]; then
        sed -i '' 's/!\[\](assets\//!\[\](\/assets\//g' ${file}
      elif [[ "$OS" == 'Linux' ]]; then
        sed -i 's/!\[\](assets\//!\[\](\/assets\//g' ${file}
      else
        echo "Currently only support macOS or Linux."
        exit 1
      fi
    fi
  done
}

# Copy all Markdown files to the Hugo website docs directory
function copy_docs() {
  # Create a new Hugo website docs directory
  if [[ ! -d ${bin}/build/web/content/docs ]]; then
    mkdir -p ${bin}/build/web/content/docs
  fi

  # Copy all markdown files to the Hugo website docs directory
  # 1. Copy all root directory markdown files
  rsync -av --exclude='README.md' ${bin}/*.md ${bin}/build/web/content/docs

  # 2. Copy all subdirectory markdown files
  subDirs=$(find "${bin}" -type d -mindepth 1 -maxdepth 1)
  for subDir in ${subDirs}; do
    if [[ ${subDir} != ${bin}/build && ${subDir} != ${bin}/assets ]]; then
      cp -r ${subDir} ${bin}/build/web/content/docs
    fi
  done

  # 3. Copy all assets to the Hugo website static directory
  cp -r "${bin}/assets" "${bin}/build/web/static"

  # Replace the Markdown embedded images with Hugo website absolute paths
  hugo_image_path_process "${bin}/build/web/content/docs"
}

# Launch the Hugo website
function launch_website() {
  # Prepare download packages
  if [[ ! -d "${bin}/build" ]]; then
    mkdir -p "${bin}/build"
  fi

  if [ ! -f "${bin}/build/hugo" ]; then
    wget -q -P "${bin}/build" ${HUGO_DOWNLOAD_URL}
    tar -xzf "${bin}/build/${HUGO_PACKAGE_NAME}" -C "${bin}/build"
    rm -rf "${bin}/build/${HUGO_PACKAGE_NAME}"
  fi

  # Remove the old Hugo website
  cd ${bin}/build
  rm -rf ${bin}/build/web

  # Create a new Hugo website
  ${bin}/build/hugo new site web

  # Setting the Hugo theme
  cd ${bin}/build/web
  git clone https://github.com/datastratolabs/gohugo-theme-ananke.git themes/ananke

  # Set the Hugo website configuration
  echo "languageCode = 'en-us'
  title = 'Gravitino Docs'
  theme = 'ananke'" > hugo.toml

  copy_docs

  # Start the Hugo server
  ${bin}/build/hugo server --buildDrafts
}

if [[ "$1" == "update" ]]; then
  copy_docs
elif [[ "$1" != "" ]]; then
  echo ${USAGE}
else
  launch_website
fi