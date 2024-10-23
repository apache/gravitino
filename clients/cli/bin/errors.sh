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
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Some of these examples assume you have the Apache Gravitino playground running.

shopt -s expand_aliases
alias gcli='java -jar ../../cli/build/libs/gravitino-cli-*-incubating-SNAPSHOT.jar'

# No such command
gcli unknown

# unknown command and entiry
gcli unknown unknown

# unknown command 
gcli metalake unknown

# unknown entity 
gcli unknown list

# Name not specified 
gcli metalake details

# Unknown metalake name
gcli metalake details --metalake unknown

# Unknown catalog name
gcli catalog details --metalake metalake_demo --name unknown

# Missing catalog name
gcli catalog details --metalake metalake_demo
