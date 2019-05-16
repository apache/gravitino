#!/usr/bin/env bash
# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

cd libs
mkdir third-party
cd third-party

# Install ERD
git clone git://github.com/BurntSushi/erd
cd erd
stack init
stack build --system-ghc
cd ..

# Install Mermaid
npm install mermaid.cli

# Install PhantomJS
wget https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-macosx.zip
unzip phantomjs-2.1.1-macosx.zip

# SVGBob
cargo install svgbob_cli --path ./svgbob

# Syntrax
pip install --upgrade syntrax
pip install pycairo
brew install pygobject3

# Vega
npm install vega

# ImageMagic
#wget https://imagemagick.org/download/binaries/ImageMagick-x86_64-apple-darwin17.7.0.tar.gz
#tar xvzf ImageMagick-x86_64-apple-darwin17.7.0.tar.gz
