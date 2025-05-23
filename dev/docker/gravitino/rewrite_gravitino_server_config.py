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
    "GRAVITINO_SERVER_SHUTDOWN_TIMEOUT": "server.shutdown.timeout",
    "GRAVITINO_SERVER_WEBSERVER_HOST": "server.webserver.host",
    "GRAVITINO_SERVER_WEBSERVER_HTTP_PORT": "server.webserver.httpPort",
    "GRAVITINO_SERVER_WEBSERVER_MIN_THREADS": "server.webserver.minThreads",
    "GRAVITINO_SERVER_WEBSERVER_MAX_THREADS": "server.webserver.maxThreads",
    "GRAVITINO_SERVER_WEBSERVER_STOP_TIMEOUT": "server.webserver.stopTimeout",
    "GRAVITINO_SERVER_WEBSERVER_IDLE_TIMEOUT": "server.webserver.idleTimeout",
    "GRAVITINO_SERVER_WEBSERVER_THREAD_POOL_WORK_QUEUE_SIZE": "server.webserver.threadPoolWorkQueueSize",
    "GRAVITINO_SERVER_WEBSERVER_REQUEST_HEADER_SIZE": "server.webserver.requestHeaderSize",
    "GRAVITINO_SERVER_WEBSERVER_RESPONSE_HEADER_SIZE": "server.webserver.responseHeaderSize",
    "GRAVITINO_ENTITY_STORE": "entity.store",
    "GRAVITINO_ENTITY_STORE_RELATIONAL": "entity.store.relational",
    "GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_URL": "entity.store.relational.jdbcUrl",
    "GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_DRIVER": "entity.store.relational.jdbcDriver",
    "GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_USER": "entity.store.relational.jdbcUser",
    "GRAVITINO_ENTITY_STORE_RELATIONAL_JDBC_PASSWORD": "entity.store.relational.jdbcPassword",
    "GRAVITINO_CATALOG_CACHE_EVICTION_INTERVAL_MS": "catalog.cache.evictionIntervalMs",
    "GRAVITINO_AUTHORIZATION_ENABLE": "authorization.enable",
    "GRAVITINO_AUTHORIZATION_SERVICE_ADMINS": "authorization.serviceAdmins",
    "GRAVITINO_AUX_SERVICE_NAMES": "auxService.names",
    "GRAVITINO_ICEBERG_REST_CLASSPATH": "iceberg-rest.classpath",
    "GRAVITINO_ICEBERG_REST_HOST": "iceberg-rest.host",
    "GRAVITINO_ICEBERG_REST_HTTP_PORT": "iceberg-rest.httpPort",
    "GRAVITINO_ICEBERG_REST_CATALOG_BACKEND": "iceberg-rest.catalog-backend",
    "GRAVITINO_ICEBERG_REST_WAREHOUSE": "iceberg-rest.warehouse"
}

init_config = {
    "server.shutdown.timeout": "3000",
    "server.webserver.host": "0.0.0.0",
    "server.webserver.httpPort": "8090",
    "server.webserver.minThreads": "24",
    "server.webserver.maxThreads": "200",
    "server.webserver.stopTimeout": "30000",
    "server.webserver.idleTimeout": "30000",
    "server.webserver.threadPoolWorkQueueSize": "100",
    "server.webserver.requestHeaderSize": "131072",
    "server.webserver.responseHeaderSize": "131072",
    "entity.store": "relational",
    "entity.store.relational": "JDBCBackend",
    "entity.store.relational.jdbcUrl": "jdbc:h2",
    "entity.store.relational.jdbcDriver": "org.h2.Driver",
    "entity.store.relational.jdbcUser": "gravitino",
    "entity.store.relational.jdbcPassword": "gravitino",
    "catalog.cache.evictionIntervalMs": "3600000",
    "authorization.enable": "false",
    "authorization.serviceAdmins": "anonymous",
    "auxService.names": "iceberg-rest",
    "iceberg-rest.classpath": "iceberg-rest-server/libs, iceberg-rest-server/conf",
    "iceberg-rest.host": "0.0.0.0",
    "iceberg-rest.httpPort": "9001",
    "iceberg-rest.catalog-backend": "memory",
    "iceberg-rest.warehouse": "/tmp/"
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


config_prefix = "gravitino."


def update_config(config, key, value):
    config[config_prefix + key] = value


config_file_path = "conf/gravitino.conf"
config_map = parse_config_file(config_file_path)

for k, v in init_config.items():
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