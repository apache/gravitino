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


class GVFSConfig:
    CACHE_SIZE = "cache_size"
    DEFAULT_CACHE_SIZE = 20

    CACHE_EXPIRED_TIME = "cache_expired_time"
    DEFAULT_CACHE_EXPIRED_TIME = 3600

    AUTH_TYPE = "auth_type"
    SIMPLE_AUTH_TYPE = "simple"

    OAUTH2_AUTH_TYPE = "oauth2"
    OAUTH2_SERVER_URI = "oauth2_server_uri"
    OAUTH2_CREDENTIAL = "oauth2_credential"
    OAUTH2_PATH = "oauth2_path"
    OAUTH2_SCOPE = "oauth2_scope"

    GVFS_FILESYSTEM_GCS_SERVICE_KEY_FILE = "gcs_service_account_file"

    GVFS_FILESYSTEM_S3_ACCESS_KEY = "s3_access_key_id"
    GVFS_FILESYSTEM_S3_SECRET_KEY = "s3_secret_access_key"
    GVFS_FILESYSTEM_S3_ENDPOINT = "s3_endpoint"

    GVFS_FILESYSTEM_OSS_ACCESS_KEY = "oss_access_key_id"
    GVFS_FILESYSTEM_OSS_SECRET_KEY = "oss_secret_access_key"
    GVFS_FILESYSTEM_OSS_ENDPOINT = "oss_endpoint"

    GVFS_FILESYSTEM_AZURE_ACCOUNT_NAME = "azure_storage_account_name"
    GVFS_FILESYSTEM_AZURE_ACCOUNT_KEY = "azure_storage_account_key"

    # This configuration marks the expired time of the credential. For instance, if the credential
    # fetched from Gravitino server has expired time of 3600 seconds, and the credential_expired_time_ration is 0.5
    # then the credential will be considered as expired after 1800 seconds and will try to retrieve a new credential.
    GVFS_FILESYSTEM_CREDENTIAL_EXPIRED_TIME_RATIO = "credential_expiration_ratio"

    # The default value of the credential_expired_time_ratio is 0.5
    DEFAULT_CREDENTIAL_EXPIRED_TIME_RATIO = 0.5

    # The configuration key for the fileset with multiple locations, on which the file system will operate.
    # The default value is "default".
    GVFS_FILESYSTEM_CURRENT_LOCATION_NAME = "current_location_name"

    # The configuration key for the env variable name that indicates the current location name. If
    # not set, the file system will read the location name from CURRENT_LOCATION_NAME env variable.
    GVFS_FILESYSTEM_CURRENT_LOCATION_NAME_ENV_VAR = "current_location_name_env_var"

    # The configuration key for the class name of the file system operations.
    # The default value is "org.apache.gravitino.filesystem.hadoop.DefaultGVFSOperations".
    GVFS_FILESYSTEM_OPERATIONS = "operations_class"

    # The hook class that will be used to intercept file system operations.
    GVFS_FILESYSTEM_HOOK = "hook_class"

    # The configuration key prefix for the client request headers.
    GVFS_FILESYSTEM_CLIENT_REQUEST_HEADER_PREFIX = "client_request_header_"

    # The configuration key for whether to enable credential vending. The default is false.
    GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING = "enable_credential_vending"

    # The configuration key prefix for the client.
    GVFS_FILESYSTEM_CLIENT_CONFIG_PREFIX = "gvfs_gravitino_client_"

    # The configuration key for whether to enable fileset catalog cache. The default is false.
    # Note that this cache causes a side effect: if you modify the fileset or fileset catalog metadata,
    # the client can not see the latest changes.
    GVFS_FILESYSTEM_ENABLE_FILESET_METADATA_CACHE = "enable_fileset_metadata_cache"
