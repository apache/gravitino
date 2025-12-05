#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os

env_map = {
    "LANCE_REST_NAMESPACE_BACKEND": "namespace-backend",
    "LANCE_REST_GRAVITINO_URI": "gravitino.uri",
    "LANCE_REST_GRAVITINO_METALAKE_NAME": "gravitino.metalake-name",
    "LANCE_REST_HOST": "host",
    "LANCE_REST_PORT": "httpPort"
}

init_config = {
    "namespace-backend": "gravitino",
    "gravitino.uri": "http://localhost:8090",
    "gravitino.metalake-name": "metalake",
    "host": "0.0.0.0",
    "httpPort": "9101"
}


def parse_config_file(file_path):
    config_map = {}
    with open(file_path, "r") as file:
        for line in file:
            stripped_line = line.strip()
            if stripped_line and not stripped_line.startswith("#"):
                key, value = stripped_line.split("=", 1)
                key = key.strip()
                value = value.strip()
                config_map[key] = value
    return config_map


config_prefix = "gravitino.lance-rest."


def update_config(config, key, value):
    config[config_prefix + key] = value


config_file_path = "conf/gravitino-lance-rest-server.conf"
config_map = parse_config_file(config_file_path)

# Set from init_config only if the key doesn't exist
for k, v in init_config.items():
    full_key = config_prefix + k
    if full_key not in config_map:
        update_config(config_map, k, v)

for k, v in env_map.items():
    if k in os.environ:
        update_config(config_map, v, os.environ[k])

if os.path.exists(config_file_path):
    os.remove(config_file_path)

with open(config_file_path, "w") as file:
    for key, value in config_map.items():
        line = "{} = {}\n".format(key, value)
        file.write(line)
