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

alias gcli='java -jar clients/cli/build/libs/gravitino-cli-0.7.0-incubating-SNAPSHOT.jar'

# No such command
gcli unknown

# unknown command and entity
gcli unknown unknown

# unknown command and entity
gcli unknown unknown

# unknown command 
gcli metalake unknown

# unknown entity 
gcli unknown list

# Name not specified 
gcli metalake details

# Too many arguments
gcli metalake details more

# Unknown metalake name
gcli metalake details --name unknown

# Unknown catalog name
gcli catalog details --name metalake_demo.unknown

# Unknown catalog name
gcli catalog details --name metalake_demo

# Already exists
gcli metalake create -name metalake_demo

# Doesn't exist
gcli metalake delete -name unknown-metalake

# Malformed name
gcli catalog details -name metalake_demo
