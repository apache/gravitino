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
import importlib
import sys
import time
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Tuple, List, Dict, Any

from fsspec import AbstractFileSystem
from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.implementations.local import LocalFileSystem
from fsspec.utils import infer_storage_options

from gravitino.api.credential.adls_token_credential import ADLSTokenCredential
from gravitino.api.credential.azure_account_key_credential import (
    AzureAccountKeyCredential,
)
from gravitino.api.credential.credential import Credential
from gravitino.api.credential.gcs_token_credential import GCSTokenCredential
from gravitino.api.credential.oss_secret_key_credential import OSSSecretKeyCredential
from gravitino.api.credential.oss_token_credential import OSSTokenCredential
from gravitino.api.credential.s3_secret_key_credential import S3SecretKeyCredential
from gravitino.api.credential.s3_token_credential import S3TokenCredential
from gravitino.exceptions.base import GravitinoRuntimeException
from gravitino.filesystem.gvfs_config import GVFSConfig

TIME_WITHOUT_EXPIRATION = sys.maxsize
SLASH = "/"


class StorageType(Enum):
    HDFS = "hdfs"
    LOCAL = "file"
    GCS = "gs"
    S3A = "s3a"
    OSS = "oss"
    ABS = "abfss"


class StorageHandler(ABC):
    """A handler class that holds the information about the actual file location and the file system which used in
    the GravitinoVirtualFileSystem's operations.
    """

    @abstractmethod
    def storage_type(self) -> StorageType:
        """Get the storage type.
        :return: The storage type
        """
        pass

    @abstractmethod
    def get_filesystem(
        self, actual_path: Optional[str] = None, **kwargs
    ) -> AbstractFileSystem:
        """Get the file system.
        :return: The file system
        """
        pass

    @abstractmethod
    def get_filesystem_with_expiration(
        self,
        credentials: List[Credential],
        catalog_props: Dict[str, str],
        options: Dict[str, str],
        actual_path: Optional[str] = None,
        **kwargs,
    ) -> Tuple[int, AbstractFileSystem]:
        """Get the file system with expiration.
        :return: The file system and the expiration time
        """
        pass

    def actual_info_to_gvfs_info(
        self,
        entry: Dict,
        fileset_location: str,
        gvfs_path_prefix: str,
    ):
        """Convert a file info from an actual entry to a virtual entry.
        :param entry: A dict of the actual file info
        :param fileset_location: Storage location of the fileset
        :param gvfs_path_prefix: The prefix of the virtual path
        :return A dict of the virtual file info
        """
        path = self.actual_path_to_gvfs_path(
            entry["name"], fileset_location, gvfs_path_prefix
        )
        last_modified = self._get_last_modified(entry)
        return {
            "name": path,
            "size": entry["size"],
            "type": entry["type"],
            "mtime": last_modified,
        }

    def actual_path_to_gvfs_path(
        self,
        actual_path: str,
        fileset_location: str,
        gvfs_path_prefix: str,
    ):
        """Convert the actual path to the gvfs path.
        :param actual_path: The actual path
        :param fileset_location: The fileset location
        :param gvfs_path_prefix: The gvfs path prefix
        :return: The gvfs path
        """
        if not fileset_location.startswith(f"{self.storage_type().value}:/"):
            raise GravitinoRuntimeException(
                f"Fileset location {fileset_location} is not a {self.storage_type()} path."
            )
        actual_prefix = self._get_actual_prefix(fileset_location)
        normalized_actual_path = self._normalize_actual_path_if_needed(
            infer_storage_options(actual_prefix), actual_path
        )
        if not actual_path.startswith(actual_prefix):
            raise GravitinoRuntimeException(
                f"Path {actual_path} does not start with valid prefix {actual_prefix}."
            )

        # if the fileset location is end with "/",
        # we should truncate this to avoid replace issues.
        if actual_prefix.endswith(SLASH) and not gvfs_path_prefix.endswith(SLASH):
            return f"{normalized_actual_path.replace(actual_prefix[:-1], gvfs_path_prefix)}"
        return f"{normalized_actual_path.replace(actual_prefix, gvfs_path_prefix)}"

    @abstractmethod
    def _get_actual_prefix(self, actual_path: str) -> str:
        """Get the actual path prefix from the actual path.
        :param actual_path: The actual path
        :return: The actual path prefix
        """
        pass

    @abstractmethod
    def _get_most_suitable_credential(self, credentials: List[Credential]):
        """Get the most suitable credential.
        :param credentials: The credentials
        :return: The most suitable credential
        """
        pass

    def strip_storage_protocol(self, path: str) -> str:
        """Strip the storage protocol from the path.
          Before passing the path to the underlying file system for processing,
           pre-process the protocol information in the path.
          Some file systems require special processing.
          For HDFS, we can pass the path like 'hdfs://{host}:{port}/xxx'.
          For Local, we can pass the path like '/tmp/xxx'.
        :param path: The path
        :return: The stripped path
        """
        return path

    def _get_expire_time_by_ratio(self, expire_time: int, options: Dict[str, str]):
        if expire_time <= 0:
            return TIME_WITHOUT_EXPIRATION

        ratio = float(
            options.get(
                GVFSConfig.GVFS_FILESYSTEM_CREDENTIAL_EXPIRED_TIME_RATIO,
                GVFSConfig.DEFAULT_CREDENTIAL_EXPIRED_TIME_RATIO,
            )
            if options
            else GVFSConfig.DEFAULT_CREDENTIAL_EXPIRED_TIME_RATIO
        )
        return time.time() * 1000 + (expire_time - time.time() * 1000) * ratio

    def _normalize_actual_path_if_needed(
        self, storage_options: Dict[str, Any], actual_path: str
    ) -> str:
        return actual_path

    def _get_last_modified(self, entry: Dict):
        return entry["mtime"]


class LocalStorageHandler(StorageHandler):
    """Local Storage Handler"""

    def storage_type(self):
        return StorageType.LOCAL

    def get_filesystem(
        self, actual_path: Optional[str] = None, **kwargs
    ) -> AbstractFileSystem:
        return LocalFileSystem(**kwargs)

    def get_filesystem_with_expiration(
        self,
        credentials: List[Credential],
        catalog_props: Dict[str, str],
        options: Dict[str, str],
        actual_path: Optional[str] = None,
        **kwargs,
    ) -> Tuple[int, AbstractFileSystem]:
        """Get the file system with expiration.
        :return: The file system and the expiration time
        """
        return TIME_WITHOUT_EXPIRATION, self.get_filesystem(actual_path, **kwargs)

    def _get_actual_prefix(
        self,
        actual_path: str,
    ) -> str:
        return actual_path[len(f"{self.storage_type().value}:") :]

    def _get_most_suitable_credential(self, credentials: List[Credential]):
        return None

    def strip_storage_protocol(self, path: str):
        """Strip the storage protocol from the path.
        :param path: The path
        :return: The stripped path
        """
        return path[len(f"{self.storage_type().value}:") :]


class HDFSStorageHandler(StorageHandler):
    """HDFS Storage Handler"""

    def storage_type(self):
        return StorageType.HDFS

    def get_filesystem(
        self, actual_path: Optional[str] = None, **kwargs
    ) -> AbstractFileSystem:
        fs_class = importlib.import_module("pyarrow.fs").HadoopFileSystem
        return ArrowFSWrapper(fs_class.from_uri(actual_path))

    def get_filesystem_with_expiration(
        self,
        credentials: List[Credential],
        catalog_props: Dict[str, str],
        options: Dict[str, str],
        actual_path: Optional[str] = None,
        **kwargs,
    ) -> Tuple[int, AbstractFileSystem]:
        """Get the file system with expiration.
        :return: The file system and the expiration time
        """
        return TIME_WITHOUT_EXPIRATION, self.get_filesystem(actual_path, **kwargs)

    def _get_actual_prefix(
        self,
        actual_path: str,
    ) -> str:
        return infer_storage_options(actual_path)["path"]

    def _get_most_suitable_credential(self, credentials: List[Credential]):
        return None


class S3StorageHandler(StorageHandler):
    """S3 Storage Handler"""

    def storage_type(self):
        return StorageType.S3A

    def get_filesystem_with_expiration(
        self,
        credentials: List[Credential],
        catalog_props: Dict[str, str],
        options: Dict[str, str],
        actual_path: Optional[str] = None,
        **kwargs,
    ) -> Tuple[int, AbstractFileSystem]:
        """Get the file system with expiration.
        :return: The file system and the expiration time
        """
        # S3 endpoint from client options has a higher priority, override the endpoint from catalog properties.
        # Note: the endpoint may not be a real S3 endpoint, it can be a simulated S3 endpoint, such as minio,
        # so though the endpoint is not a required field for S3FileSystem, we still need to assign the endpoint
        # to the S3FileSystem
        s3_endpoint = None
        if options:
            s3_endpoint = (
                options.get(GVFSConfig.GVFS_FILESYSTEM_S3_ENDPOINT, s3_endpoint)
                if options
                else None
            )
            if s3_endpoint is None:
                s3_endpoint = (
                    catalog_props.get("s3-endpoint", None) if catalog_props else None
                )
        else:
            s3_endpoint = (
                catalog_props.get("s3-endpoint", None) if catalog_props else None
            )

        if credentials:
            credential = self._get_most_suitable_credential(credentials)
            if credential is not None:
                expire_time = self._get_expire_time_by_ratio(
                    credential.expire_time_in_ms(),
                    options,
                )
                if isinstance(credential, S3TokenCredential):
                    fs = self.get_filesystem(
                        key=credential.access_key_id(),
                        secret=credential.secret_access_key(),
                        token=credential.session_token(),
                        endpoint_url=s3_endpoint,
                        **kwargs,
                    )
                    return expire_time, fs
                if isinstance(credential, S3SecretKeyCredential):
                    fs = self.get_filesystem(
                        key=credential.access_key_id(),
                        secret=credential.secret_access_key(),
                        endpoint_url=s3_endpoint,
                        **kwargs,
                    )
                    return expire_time, fs

        # this is the old way to get the s3 file system
        # get 'aws_access_key_id' from s3_options, if the key is not found, throw an exception
        aws_access_key_id = (
            options.get(GVFSConfig.GVFS_FILESYSTEM_S3_ACCESS_KEY) if options else None
        )
        if aws_access_key_id is None:
            raise GravitinoRuntimeException(
                "AWS access key id is not found in the options."
            )

        # get 'aws_secret_access_key' from s3_options, if the key is not found, throw an exception
        aws_secret_access_key = (
            options.get(GVFSConfig.GVFS_FILESYSTEM_S3_SECRET_KEY) if options else None
        )
        if aws_secret_access_key is None:
            raise GravitinoRuntimeException(
                "AWS secret access key is not found in the options."
            )

        return (
            TIME_WITHOUT_EXPIRATION,
            self.get_filesystem(
                key=aws_access_key_id,
                secret=aws_secret_access_key,
                endpoint_url=s3_endpoint,
                **kwargs,
            ),
        )

    def get_filesystem(
        self, actual_path: Optional[str] = None, **kwargs
    ) -> AbstractFileSystem:
        return importlib.import_module("s3fs").S3FileSystem(**kwargs)

    def _get_actual_prefix(
        self,
        actual_path: str,
    ) -> str:
        return infer_storage_options(actual_path)["path"]

    def _get_most_suitable_credential(self, credentials: List[Credential]):
        for credential in credentials:
            # Prefer to use the token credential, if it does not exist, use the
            # secret key credential.
            if isinstance(credential, S3TokenCredential):
                return credential

        for credential in credentials:
            if isinstance(credential, S3SecretKeyCredential):
                return credential
        return None

    def _get_last_modified(self, entry: Dict):
        return entry["LastModified"] if "LastModified" in entry else None


class GCSStorageHandler(StorageHandler):
    """GCS Storage Handler"""

    def storage_type(self):
        return StorageType.GCS

    def get_filesystem(
        self, actual_path: Optional[str] = None, **kwargs
    ) -> AbstractFileSystem:
        return importlib.import_module("gcsfs").GCSFileSystem(**kwargs)

    def get_filesystem_with_expiration(
        self,
        credentials: List[Credential],
        catalog_props: Dict[str, str],
        options: Dict[str, str],
        actual_path: Optional[str] = None,
        **kwargs,
    ) -> Tuple[int, AbstractFileSystem]:
        if credentials:
            credential = self._get_most_suitable_credential(credentials)
            if credential is not None:
                expire_time = self._get_expire_time_by_ratio(
                    credential.expire_time_in_ms(),
                    options,
                )
                if isinstance(credential, GCSTokenCredential):
                    return (
                        expire_time,
                        self.get_filesystem(
                            token=credential.token(),
                            **kwargs,
                        ),
                    )

        # get 'service-account-key' from gcs_options, if the key is not found, throw an exception
        service_account_key_path = (
            options.get(GVFSConfig.GVFS_FILESYSTEM_GCS_SERVICE_KEY_FILE)
            if options
            else None
        )
        if service_account_key_path is None:
            raise GravitinoRuntimeException(
                "Service account key is not found in the options."
            )
        return (
            TIME_WITHOUT_EXPIRATION,
            self.get_filesystem(
                token=service_account_key_path,
                **kwargs,
            ),
        )

    def _get_actual_prefix(
        self,
        actual_path: str,
    ) -> str:
        return infer_storage_options(actual_path)["path"]

    def _get_most_suitable_credential(self, credentials: List[Credential]):
        for credential in credentials:
            # Prefer to use the token credential, if it does not exist, return None.
            if isinstance(credential, GCSTokenCredential):
                return credential
        return None


class OSSStorageHandler(StorageHandler):
    """OSS Storage Handler"""

    def storage_type(self):
        return StorageType.OSS

    def strip_storage_protocol(self, path: str):
        """Strip the storage protocol from the path.
        if we do not remove the protocol, it will always return an empty array.
        :param path: The path
        :return: The stripped path
        """
        if path.startswith(f"{self.storage_type().value}://"):
            return path[len(f"{self.storage_type().value}://") :]
        return path

    def get_filesystem(
        self, actual_path: Optional[str] = None, **kwargs
    ) -> AbstractFileSystem:
        return importlib.import_module("ossfs").OSSFileSystem(**kwargs)

    def get_filesystem_with_expiration(
        self,
        credentials: List[Credential],
        catalog_props: Dict[str, str],
        options: Dict[str, str],
        actual_path: Optional[str] = None,
        **kwargs,
    ) -> Tuple[int, AbstractFileSystem]:
        oss_endpoint = None
        # OSS endpoint from client options has a higher priority, override the endpoint from catalog properties.
        if options:
            oss_endpoint = (
                options.get(GVFSConfig.GVFS_FILESYSTEM_OSS_ENDPOINT, oss_endpoint)
                if options
                else None
            )
            if oss_endpoint is None:
                oss_endpoint = (
                    catalog_props.get("oss-endpoint", None) if catalog_props else None
                )
        else:
            oss_endpoint = (
                catalog_props.get("oss-endpoint", None) if catalog_props else None
            )

        if credentials:
            credential = self._get_most_suitable_credential(credentials)
            if credential is not None:
                expire_time = self._get_expire_time_by_ratio(
                    credential.expire_time_in_ms(),
                    options,
                )
                if isinstance(credential, OSSTokenCredential):
                    fs = self.get_filesystem(
                        key=credential.access_key_id(),
                        secret=credential.secret_access_key(),
                        token=credential.security_token(),
                        endpoint=oss_endpoint,
                        **kwargs,
                    )
                    return expire_time, fs
                if isinstance(credential, OSSSecretKeyCredential):
                    return (
                        expire_time,
                        self.get_filesystem(
                            key=credential.access_key_id(),
                            secret=credential.secret_access_key(),
                            endpoint=oss_endpoint,
                            **kwargs,
                        ),
                    )

        # get 'oss_access_key_id' from oss options, if the key is not found, throw an exception
        oss_access_key_id = (
            options.get(GVFSConfig.GVFS_FILESYSTEM_OSS_ACCESS_KEY) if options else None
        )
        if oss_access_key_id is None:
            raise GravitinoRuntimeException(
                "OSS access key id is not found in the options."
            )

        # get 'oss_secret_access_key' from oss options, if the key is not found, throw an exception
        oss_secret_access_key = (
            options.get(GVFSConfig.GVFS_FILESYSTEM_OSS_SECRET_KEY) if options else None
        )
        if oss_secret_access_key is None:
            raise GravitinoRuntimeException(
                "OSS secret access key is not found in the options."
            )

        return (
            TIME_WITHOUT_EXPIRATION,
            importlib.import_module("ossfs").OSSFileSystem(
                key=oss_access_key_id,
                secret=oss_secret_access_key,
                endpoint=oss_endpoint,
                **kwargs,
            ),
        )

    def _get_last_modified(self, entry: Dict):
        return entry["LastModified"] if "LastModified" in entry else None

    def _get_actual_prefix(
        self,
        actual_path: str,
    ) -> str:
        ops = infer_storage_options(actual_path)
        if "host" not in ops or "path" not in ops:
            raise GravitinoRuntimeException(
                f"Storage location:{actual_path} doesn't support now."
            )

        return ops["host"] + ops["path"]

    def _get_most_suitable_credential(self, credentials: List[Credential]):
        for credential in credentials:
            # Prefer to use the token credential, if it does not exist, use the
            # secret key credential.
            if isinstance(credential, OSSTokenCredential):
                return credential

        for credential in credentials:
            if isinstance(credential, OSSSecretKeyCredential):
                return credential
        return None


class ABSStorageHandler(StorageHandler):
    """Azure Blob Storage Handler"""

    def storage_type(self):
        return StorageType.ABS

    def get_filesystem(
        self, actual_path: Optional[str] = None, **kwargs
    ) -> AbstractFileSystem:
        return importlib.import_module("adlfs").AzureBlobFileSystem(**kwargs)

    def get_filesystem_with_expiration(
        self,
        credentials: List[Credential],
        catalog_props: Dict[str, str],
        options: Dict[str, str],
        actual_path: Optional[str] = None,
        **kwargs,
    ) -> Tuple[int, AbstractFileSystem]:
        if credentials:
            credential = self._get_most_suitable_credential(credentials)
            if credential is not None:
                expire_time = self._get_expire_time_by_ratio(
                    credential.expire_time_in_ms(),
                    options,
                )
                if isinstance(credential, ADLSTokenCredential):
                    fs = self.get_filesystem(
                        account_name=credential.account_name(),
                        sas_token=credential.sas_token(),
                        **kwargs,
                    )
                    return expire_time, fs

                if isinstance(credential, AzureAccountKeyCredential):
                    fs = self.get_filesystem(
                        account_name=credential.account_name(),
                        account_key=credential.account_key(),
                        **kwargs,
                    )
                    return expire_time, fs

        # get 'abs_account_name' from abs options, if the key is not found, throw an exception
        abs_account_name = (
            options.get(GVFSConfig.GVFS_FILESYSTEM_AZURE_ACCOUNT_NAME)
            if options
            else None
        )
        if abs_account_name is None:
            raise GravitinoRuntimeException(
                "ABS account name is not found in the options."
            )

        # get 'abs_account_key' from abs options, if the key is not found, throw an exception
        abs_account_key = (
            options.get(GVFSConfig.GVFS_FILESYSTEM_AZURE_ACCOUNT_KEY)
            if options
            else None
        )
        if abs_account_key is None:
            raise GravitinoRuntimeException(
                "ABS account key is not found in the options."
            )

        return (
            TIME_WITHOUT_EXPIRATION,
            self.get_filesystem(
                account_name=abs_account_name,
                account_key=abs_account_key,
                **kwargs,
            ),
        )

    def strip_storage_protocol(self, path: str):
        """Strip the storage protocol from the path.
        We need to remove the protocol and account from the path, for instance,
        the path can be converted from 'abfss://container@account/path' to
        'container/path'.
        :param path: The path
        :return: The stripped path
        """
        ops = infer_storage_options(path)
        return ops["username"] + ops["path"]

    def _get_actual_prefix(
        self,
        actual_path: str,
    ) -> str:
        ops = infer_storage_options(actual_path)
        if "username" not in ops or "host" not in ops or "path" not in ops:
            raise GravitinoRuntimeException(
                f"The location:{actual_path} doesn't support now, the username,"
                f"host and path are required in the  location."
            )
        return f"{self.storage_type().value}://{ops['username']}@{ops['host']}{ops['path']}"

    def _normalize_actual_path_if_needed(
        self, storage_options: Dict[str, Any], actual_path: str
    ) -> str:
        # the actual path may be '{container}/{path}', we need to add the host and username
        # get the path from {container}/{path}
        if not actual_path.startswith(f"{StorageType.ABS}"):
            path_without_username = actual_path[actual_path.index("/") + 1 :]
            return (
                f"{StorageType.ABS.value}://{storage_options['username']}@{storage_options['host']}/"
                f"{path_without_username}"
            )
        return actual_path

    def _get_most_suitable_credential(self, credentials: List[Credential]):
        for credential in credentials:
            # Prefer to use the token credential, if it does not exist, use the
            # account key credential
            if isinstance(credential, ADLSTokenCredential):
                return credential

        for credential in credentials:
            if isinstance(credential, AzureAccountKeyCredential):
                return credential
        return None

    def _get_last_modified(self, entry: Dict):
        return entry["last_modified"]


LOCA_HANDLER = LocalStorageHandler()
HDFS_HANDLER = HDFSStorageHandler()
S3_HANDLER = S3StorageHandler()
GCS_HANDLER = GCSStorageHandler()
OSS_HANDLER = OSSStorageHandler()
ABS_HANDLER = ABSStorageHandler()

storage_prefixes = {
    f"{StorageType.HDFS.value}://": StorageType.HDFS,
    f"{StorageType.LOCAL.value}:/": StorageType.LOCAL,
    f"{StorageType.GCS.value}://": StorageType.GCS,
    f"{StorageType.S3A.value}://": StorageType.S3A,
    f"{StorageType.OSS.value}://": StorageType.OSS,
    f"{StorageType.ABS.value}://": StorageType.ABS,
}

storage_handlers: Dict[StorageType, StorageHandler] = {
    StorageType.LOCAL: LOCA_HANDLER,
    StorageType.HDFS: HDFS_HANDLER,
    StorageType.GCS: GCS_HANDLER,
    StorageType.S3A: S3_HANDLER,
    StorageType.OSS: OSS_HANDLER,
    StorageType.ABS: ABS_HANDLER,
}


def get_storage_handler_by_type(storage_type: StorageType) -> StorageHandler:
    """Get the storage handler by the storage type.
    :param storage_type: The storage type
    :return: The storage handler
    """
    if storage_type not in storage_handlers:
        raise GravitinoRuntimeException(
            f"Storage type: `{storage_type}` doesn't support now."
        )
    return storage_handlers[storage_type]


def get_storage_handler_by_path(actual_path: str) -> StorageHandler:
    """Get the storage handler by the actual path.
    :param actual_path: The actual path
    :return: The storage handler
    """

    for prefix, storage_type in storage_prefixes.items():
        if actual_path.startswith(prefix):
            return get_storage_handler_by_type(storage_type)

    raise GravitinoRuntimeException(
        f"Storage type doesn't support now. Path:{actual_path}"
    )
