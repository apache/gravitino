#!/usr/bin/env 
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
  "GRAVITINO_IO_IMPL" : "io-impl",
  "GRAVITINO_URI" : "uri",
  "GRAVITINO_WAREHOUSE" : "warehouse",
  "GRAVITINO_CREDENTIAL_PROVIDER_TYPE" : "credential-provider-type",
  "GRAVITINO_GCS_CREDENTIAL_FILE_PATH" : "gcs-credential-file-path",
  "GRAVITINO_S3_ACCESS_KEY" : "s3-access-key-id",
  "GRAVITINO_S3_SECRET_KEY" : "s3-secret-access-key",
  "GRAVITINO_S3_REGION" : "s3-region",
  "GRAVITINO_S3_ROLE_ARN" : "s3-role-arn",
  "GRAVITINO_S3_EXTERNAL_ID" : "s3-external-id"
}


def parse_config_file(file_path):  
    config_map = {}  
    with open(file_path, 'r') as file:  
        for line in file:  
            stripped_line = line.strip()  
            if stripped_line and not stripped_line.startswith('#'):  
                key, value = stripped_line.split('=')  
                key = key.strip()  
                value = value.strip()  
                config_map[key] = value  
    return config_map  

config_prefix = "gravitino.iceberg-rest."

def update_config(config, key, value):
    config[config_prefix + key] = value
  
config_file_path = 'conf/gravitino-iceberg-rest-server.conf'
config_map = parse_config_file(config_file_path)

update_config(config_map, "catalog-backend", "jdbc")
update_config(config_map, "jdbc-driver", "org.sqlite.JDBC")
update_config(config_map, "uri", "jdbc:sqlite::memory:")
update_config(config_map, "jdbc-user", "iceberg")
update_config(config_map, "jdbc-password", "iceberg")
update_config(config_map, "jdbc-initialize", "true")
update_config(config_map, "jdbc.schema-version", "V1")

for k, v in env_map.items():
    if k in os.environ:
        update_config(config_map, v, os.environ[k])
  
# for key, value in config_map.items(): 
#     print(f"{key}: {value}")

if os.path.exists(config_file_path):  
    os.remove(config_file_path)  

with open(config_file_path, 'w') as file:  
    for key, value in config_map.items():  
        line = "{} = {}\n".format(key, value)  
        file.write(line)  
