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
"""Helpers for omitting static cloud credentials from GVFS client property maps."""

from typing import Dict, Optional

from gravitino.filesystem.gvfs_config import GVFSConfig

MASKED_PROPERTY_VALUE = "******"

STATIC_CREDENTIAL_PROPERTY_KEYS = frozenset(
    {
        "s3-access-key-id",
        "s3-secret-access-key",
        "oss-access-key-id",
        "oss-secret-access-key",
        "azure-storage-account-key",
        "gcs-service-account-file",
        "cos-access-key-id",
        "cos-secret-access-key",
        GVFSConfig.GVFS_FILESYSTEM_S3_ACCESS_KEY,
        GVFSConfig.GVFS_FILESYSTEM_S3_SECRET_KEY,
        GVFSConfig.GVFS_FILESYSTEM_OSS_ACCESS_KEY,
        GVFSConfig.GVFS_FILESYSTEM_OSS_SECRET_KEY,
        GVFSConfig.GVFS_FILESYSTEM_AZURE_ACCOUNT_KEY,
        GVFSConfig.GVFS_FILESYSTEM_GCS_SERVICE_KEY_FILE,
    }
)


def omit_static_credential_properties(
    properties: Optional[Dict[str, str]],
) -> Dict[str, str]:
    """Return a copy of properties without static credential keys or masked placeholders."""
    if not properties:
        return {}
    filtered = {}
    for key, value in properties.items():
        if key is None or value is None:
            continue
        if key in STATIC_CREDENTIAL_PROPERTY_KEYS or value == MASKED_PROPERTY_VALUE:
            continue
        filtered[key] = value
    return filtered
