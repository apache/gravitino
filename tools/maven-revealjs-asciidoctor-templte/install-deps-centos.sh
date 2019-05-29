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


############# install necessary packages
yum install -y git graphviz maven nodejs wget bzip2 python36 python36-pip pygobject3 cargo

#############
python3 -m pip install --upgrade pip setuptools seqdiag blockdiag actdiag nwdiag convert syntrax racks opc-diag
npm install vega pango

############# install stack
wget -qO- https://get.haskellstack.org/ | sh

############# install PhantomJS
wget https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-linux-x86_64.tar.bz2
bunzip2 phantomjs-2.1.1-linux-x86_64.tar.bz2
tar -xvf phantomjs-2.1.1-linux-x86_64.tar

############# install ERD
cd libs
mkdir third-party
cd third-party
git clone https://github.com/BurntSushi/erd.git
cd erd
stack install
read -p "Add stack ($HOME/.local/bin) to PATH ($PATH) ? (y/n)" -n 1 -r YES_NO
if [ $YES_NO  = "y" ]; then
   echo 'export PATH=$PATH:$HOME/.local/bin' >> ~/.bash_profile
   . ~/.bash_profile
fi

############# install SVGBob
cargo install svgbob_cli
read -p "Add cargo ($HOME/.cargo/bin) to PATH ($PATH) ? (y/n)" -n 1 -r YES_NO
if [ $YES_NO  = "y" ]; then
   echo 'export PATH=$PATH:$HOME/.cargo/bin' >> ~/.bash_profile
   . ~/.bash_profile
fi

############# 
cd ../../..
mvn jetty:run-exploded

