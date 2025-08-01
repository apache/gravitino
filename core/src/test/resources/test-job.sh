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

# This script is used to test the job submission and execution in a local environment.
echo "starting test test job"

bin="$(dirname "${BASH_SOURCE-$0}")"
bin="$(cd "${bin}">/dev/null; pwd)"

. "${bin}/common.sh"

sleep 3

JOB_NAME="test_job-$(date +%s)-$1"

echo "Submitting job with name: $JOB_NAME"

echo "$1"

echo "$2"

echo "$ENV_VAR"

if [[ "$2" == "success" ]]; then
  exit 0
elif [[ "$2" == "fail" ]]; then
  exit 1
else
  exit 2
fi
