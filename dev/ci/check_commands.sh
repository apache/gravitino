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

set -ex

if ! command -v docker; then
    echo "docker could not be found, exiting."
    exit 1
fi

if ! file $(which docker) | grep -q 'executable'; then
    echo "docker is not a executable file, exiting."
    exit 1
fi

if ! command -v docker-proxy; then
    echo "docker-proxy could not be found, exiting."
    exit 1
fi

if ! file $(which docker-proxy) | grep -q 'executable'; then
    echo "docker-proxy is not a executable file, exiting."
    exit 1
fi

# More commands can be added here
echo "All required commands are installed."