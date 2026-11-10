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
# Deprecated: bin/start-gravitino.sh is kept for backward compatibility only and will be
# removed in a future release. The Docker ENTRYPOINT has moved to docker/docker-entrypoint.sh.
# If you are overriding the container entrypoint or calling this script directly, please
# update your configuration to reference docker/docker-entrypoint.sh instead.

echo "WARNING: bin/start-gravitino.sh is deprecated and will be removed in a future release." \
  "Please use docker/docker-entrypoint.sh instead." >&2

bin_dir="$(dirname "${BASH_SOURCE-$0}")"
exec "${bin_dir}/../docker/docker-entrypoint.sh" "$@"
