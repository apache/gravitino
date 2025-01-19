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
import logging
import sys

# Disable C0302: Too many lines in module
# pylint: disable=C0302
import time
from enum import Enum
from pathlib import PurePosixPath
from typing import Dict, Tuple, List
import re
import importlib
import fsspec

from cachetools import TTLCache, LRUCache
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.utils import infer_storage_options

from readerwriterlock import rwlock

from gravitino.api.catalog import Catalog
from gravitino.api.credential.credential import Credential
from gravitino.audit.caller_context import CallerContext, CallerContextHolder
from gravitino.audit.fileset_audit_constants import FilesetAuditConstants
from gravitino.audit.fileset_data_operation import FilesetDataOperation
from gravitino.audit.internal_client_type import InternalClientType
from gravitino.auth.default_oauth2_token_provider import DefaultOAuth2TokenProvider
from gravitino.auth.oauth2_token_provider import OAuth2TokenProvider
from gravitino.auth.simple_auth_provider import SimpleAuthProvider
from gravitino.client.generic_fileset import GenericFileset
from gravitino.client.fileset_catalog import FilesetCatalog
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
)
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.name_identifier import NameIdentifier

from gravitino.api.credential.adls_token_credential import ADLSTokenCredential
from gravitino.api.credential.azure_account_key_credential import (
    AzureAccountKeyCredential,
)
from gravitino.api.credential.gcs_token_credential import GCSTokenCredential
from gravitino.api.credential.oss_secret_key_credential import OSSSecretKeyCredential
from gravitino.api.credential.oss_token_credential import OSSTokenCredential
from gravitino.api.credential.s3_secret_key_credential import S3SecretKeyCredential
from gravitino.api.credential.s3_token_credential import S3TokenCredential

logger = logging.getLogger(__name__)

PROTOCOL_NAME = "gvfs"

TIME_WITHOUT_EXPIRATION = sys.maxsize


class StorageType(Enum):
    HDFS = "hdfs"
    LOCAL = "file"
    GCS = "gs"
    S3A = "s3a"
    OSS = "oss"
    ABS = "abfss"


class FilesetContextPair:
    """A context object that holds the information about the actual file location and the file system which used in
    the GravitinoVirtualFileSystem's operations.
    """

    def __init__(self, actual_file_location: str, filesystem: AbstractFileSystem):
        self._actual_file_location = actual_file_location
        self._filesystem = filesystem

    def actual_file_location(self):
        return self._actual_file_location

    def filesystem(self):
        return self._filesystem


class GravitinoVirtualFileSystem(fsspec.AbstractFileSystem):
    """This is a virtual file system that users can access `fileset` and
    other resources.

    It obtains the actual storage location corresponding to the resource from the
    Gravitino server, and creates an independent file system for it to act as an agent for users to
    access the underlying storage.
    """

    # Override the parent variable
    protocol = PROTOCOL_NAME
    _identifier_pattern = re.compile("^fileset/([^/]+)/([^/]+)/([^/]+)(?:/[^/]+)*/?$")
    SLASH = "/"

    def __init__(
        self,
        server_uri: str = None,
        metalake_name: str = None,
        options: Dict = None,
        **kwargs,
    ):
        """Initialize the GravitinoVirtualFileSystem.
        :param server_uri: Gravitino server URI
        :param metalake_name: Gravitino metalake name
        :param options: Options for the GravitinoVirtualFileSystem
        :param kwargs: Extra args for super filesystem
        """
        self._metalake = metalake_name
        auth_type = (
            GVFSConfig.SIMPLE_AUTH_TYPE
            if options is None
            else options.get(GVFSConfig.AUTH_TYPE, GVFSConfig.SIMPLE_AUTH_TYPE)
        )
        if auth_type == GVFSConfig.SIMPLE_AUTH_TYPE:
            self._client = GravitinoClient(
                uri=server_uri,
                metalake_name=metalake_name,
                auth_data_provider=SimpleAuthProvider(),
            )
        elif auth_type == GVFSConfig.OAUTH2_AUTH_TYPE:
            oauth2_server_uri = options.get(GVFSConfig.OAUTH2_SERVER_URI)
            self._check_auth_config(
                auth_type, GVFSConfig.OAUTH2_SERVER_URI, oauth2_server_uri
            )

            oauth2_credential = options.get(GVFSConfig.OAUTH2_CREDENTIAL)
            self._check_auth_config(
                auth_type, GVFSConfig.OAUTH2_CREDENTIAL, oauth2_credential
            )

            oauth2_path = options.get(GVFSConfig.OAUTH2_PATH)
            self._check_auth_config(auth_type, GVFSConfig.OAUTH2_PATH, oauth2_path)

            oauth2_scope = options.get(GVFSConfig.OAUTH2_SCOPE)
            self._check_auth_config(auth_type, GVFSConfig.OAUTH2_SCOPE, oauth2_scope)

            oauth2_token_provider: OAuth2TokenProvider = DefaultOAuth2TokenProvider(
                oauth2_server_uri, oauth2_credential, oauth2_path, oauth2_scope
            )
            self._client = GravitinoClient(
                uri=server_uri,
                metalake_name=metalake_name,
                auth_data_provider=oauth2_token_provider,
            )
        else:
            raise GravitinoRuntimeException(
                f"Authentication type {auth_type} is not supported."
            )
        cache_size = (
            GVFSConfig.DEFAULT_CACHE_SIZE
            if options is None
            else options.get(GVFSConfig.CACHE_SIZE, GVFSConfig.DEFAULT_CACHE_SIZE)
        )
        cache_expired_time = (
            GVFSConfig.DEFAULT_CACHE_EXPIRED_TIME
            if options is None
            else options.get(
                GVFSConfig.CACHE_EXPIRED_TIME, GVFSConfig.DEFAULT_CACHE_EXPIRED_TIME
            )
        )
        self._cache = TTLCache(maxsize=cache_size, ttl=cache_expired_time)
        self._cache_lock = rwlock.RWLockFair()
        self._catalog_cache = LRUCache(maxsize=100)
        self._catalog_cache_lock = rwlock.RWLockFair()
        self._options = options

        super().__init__(**kwargs)

    @property
    def cache(self):
        return self._cache

    @property
    def fsid(self):
        return PROTOCOL_NAME

    def sign(self, path, expiration=None, **kwargs):
        """We do not support to create a signed URL representing the given path in gvfs."""
        raise GravitinoRuntimeException(
            "Sign is not implemented for Gravitino Virtual FileSystem."
        )

    def ls(self, path, detail=True, **kwargs):
        """List the files and directories info of the path.
        :param path: Virtual fileset path
        :param detail: Whether to show the details for the files and directories info
        :param kwargs: Extra args
        :return If details is true, returns a list of file info dicts, else returns a list of file paths
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.LIST_STATUS
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        pre_process_path: str = self._pre_process_path(path)
        identifier: NameIdentifier = self._extract_identifier(pre_process_path)
        sub_path: str = self._get_sub_path_from_virtual_path(
            identifier, pre_process_path
        )
        storage_location: str = actual_path[: len(actual_path) - len(sub_path)]
        # return entries with details
        if detail:
            entries = context_pair.filesystem().ls(
                self._strip_storage_protocol(storage_type, actual_path),
                detail=True,
            )
            virtual_entries = [
                self._convert_actual_info(
                    entry, storage_location, self._get_virtual_location(identifier)
                )
                for entry in entries
            ]
            return virtual_entries
        # only returns paths
        entry_paths = context_pair.filesystem().ls(
            self._strip_storage_protocol(storage_type, actual_path),
            detail=False,
        )
        virtual_entry_paths = [
            self._convert_actual_path(
                entry_path, storage_location, self._get_virtual_location(identifier)
            )
            for entry_path in entry_paths
        ]
        return virtual_entry_paths

    def info(self, path, **kwargs):
        """Get file info.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return A file info dict
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.GET_FILE_STATUS
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        pre_process_path: str = self._pre_process_path(path)
        identifier: NameIdentifier = self._extract_identifier(pre_process_path)
        sub_path: str = self._get_sub_path_from_virtual_path(
            identifier, pre_process_path
        )
        storage_location: str = actual_path[: len(actual_path) - len(sub_path)]
        actual_info: Dict = context_pair.filesystem().info(
            self._strip_storage_protocol(storage_type, actual_path)
        )
        return self._convert_actual_info(
            actual_info, storage_location, self._get_virtual_location(identifier)
        )

    def exists(self, path, **kwargs):
        """Check if a file or a directory exists.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return If a file or directory exists, it returns True, otherwise False
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.EXISTS
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        return context_pair.filesystem().exists(
            self._strip_storage_protocol(storage_type, actual_path)
        )

    def cp_file(self, path1, path2, **kwargs):
        """Copy a file.
        :param path1: Virtual src fileset path
        :param path2: Virtual dst fileset path, should be consistent with the src path fileset identifier
        :param kwargs: Extra args
        """
        src_path = self._pre_process_path(path1)
        dst_path = self._pre_process_path(path2)
        src_identifier: NameIdentifier = self._extract_identifier(src_path)
        dst_identifier: NameIdentifier = self._extract_identifier(dst_path)
        if src_identifier != dst_identifier:
            raise GravitinoRuntimeException(
                f"Destination file path identifier: `{dst_identifier}` should be same with src file path "
                f"identifier: `{src_identifier}`."
            )
        src_context_pair: FilesetContextPair = self._get_fileset_context(
            src_path, FilesetDataOperation.COPY_FILE
        )
        src_actual_path = src_context_pair.actual_file_location()

        dst_context_pair: FilesetContextPair = self._get_fileset_context(
            dst_path, FilesetDataOperation.COPY_FILE
        )
        dst_actual_path = dst_context_pair.actual_file_location()

        storage_type = self._recognize_storage_type(src_actual_path)
        src_context_pair.filesystem().cp_file(
            self._strip_storage_protocol(storage_type, src_actual_path),
            self._strip_storage_protocol(storage_type, dst_actual_path),
        )

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        """Move a file to another directory.
         This can move a file to another existing directory.
         If the target path directory does not exist, an exception will be thrown.
        :param path1: Virtual src fileset path
        :param path2: Virtual dst fileset path, should be consistent with the src path fileset identifier
        :param recursive: Whether to move recursively
        :param maxdepth: Maximum depth of recursive move
        :param kwargs: Extra args
        """
        src_path = self._pre_process_path(path1)
        dst_path = self._pre_process_path(path2)
        src_identifier: NameIdentifier = self._extract_identifier(src_path)
        dst_identifier: NameIdentifier = self._extract_identifier(dst_path)
        if src_identifier != dst_identifier:
            raise GravitinoRuntimeException(
                f"Destination file path identifier: `{dst_identifier}`"
                f" should be same with src file path identifier: `{src_identifier}`."
            )
        src_context_pair: FilesetContextPair = self._get_fileset_context(
            src_path, FilesetDataOperation.RENAME
        )
        src_actual_path = src_context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(src_actual_path)
        dst_context_pair: FilesetContextPair = self._get_fileset_context(
            dst_path, FilesetDataOperation.RENAME
        )
        dst_actual_path = dst_context_pair.actual_file_location()

        # convert the following to in

        if storage_type in [
            StorageType.HDFS,
            StorageType.GCS,
            StorageType.S3A,
            StorageType.OSS,
            StorageType.ABS,
        ]:
            src_context_pair.filesystem().mv(
                self._strip_storage_protocol(storage_type, src_actual_path),
                self._strip_storage_protocol(storage_type, dst_actual_path),
            )
        elif storage_type == StorageType.LOCAL:
            src_context_pair.filesystem().mv(
                self._strip_storage_protocol(storage_type, src_actual_path),
                self._strip_storage_protocol(storage_type, dst_actual_path),
                recursive,
                maxdepth,
            )
        else:
            raise GravitinoRuntimeException(
                f"Storage type:{storage_type} doesn't support now."
            )

    def _rm(self, path):
        raise GravitinoRuntimeException(
            "Deprecated method, use `rm_file` method instead."
        )

    def lazy_load_class(self, module_name, class_name):
        module = importlib.import_module(module_name)
        return getattr(module, class_name)

    def rm(self, path, recursive=False, maxdepth=None):
        """Remove a file or directory.
        :param path: Virtual fileset path
        :param recursive: Whether to remove the directory recursively.
                When removing a directory, this parameter should be True.
        :param maxdepth: The maximum depth to remove the directory recursively.
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.DELETE
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        fs = context_pair.filesystem()

        # S3FileSystem doesn't support maxdepth
        if isinstance(fs, self.lazy_load_class("s3fs", "S3FileSystem")):
            fs.rm(self._strip_storage_protocol(storage_type, actual_path), recursive)
        else:
            fs.rm(
                self._strip_storage_protocol(storage_type, actual_path),
                recursive,
                maxdepth,
            )

    def rm_file(self, path):
        """Remove a file.
        :param path: Virtual fileset path
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.DELETE
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        context_pair.filesystem().rm_file(
            self._strip_storage_protocol(storage_type, actual_path)
        )

    def rmdir(self, path):
        """Remove a directory.
        It will delete a directory and all its contents recursively for PyArrow.HadoopFileSystem.
        And it will throw an exception if delete a directory which is non-empty for LocalFileSystem.
        :param path: Virtual fileset path
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.DELETE
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        context_pair.filesystem().rmdir(
            self._strip_storage_protocol(storage_type, actual_path)
        )

    def open(
        self,
        path,
        mode="rb",
        block_size=None,
        cache_options=None,
        compression=None,
        **kwargs,
    ):
        """Open a file to read/write/append.
        :param path: Virtual fileset path
        :param mode: The mode now supports: rb(read), wb(write), ab(append). See builtin ``open()``
        :param block_size: Some indication of buffering - this is a value in bytes
        :param cache_options: Extra arguments to pass through to the cache
        :param compression: If given, open file using compression codec
        :param kwargs: Extra args
        :return A file-like object from the filesystem
        """
        if mode in ("w", "wb"):
            data_operation = FilesetDataOperation.OPEN_AND_WRITE
        elif mode in ("a", "ab"):
            data_operation = FilesetDataOperation.OPEN_AND_APPEND
        else:
            data_operation = FilesetDataOperation.OPEN
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, data_operation
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        return context_pair.filesystem().open(
            self._strip_storage_protocol(storage_type, actual_path),
            mode,
            block_size,
            cache_options,
            compression,
            **kwargs,
        )

    def mkdir(self, path, create_parents=True, **kwargs):
        """Make a directory.
        if create_parents=True, this is equivalent to ``makedirs``.

        :param path: Virtual fileset path
        :param create_parents: Create parent directories if missing when set to True
        :param kwargs: Extra args
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.MKDIRS
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        context_pair.filesystem().mkdir(
            self._strip_storage_protocol(storage_type, actual_path),
            create_parents,
            **kwargs,
        )

    def makedirs(self, path, exist_ok=True):
        """Make a directory recursively.
        :param path: Virtual fileset path
        :param exist_ok: Continue if a directory already exists
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.MKDIRS
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        context_pair.filesystem().makedirs(
            self._strip_storage_protocol(storage_type, actual_path),
            exist_ok,
        )

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime
        Only supports for `fsspec.LocalFileSystem` now.
        :param path: Virtual fileset path
        :return Created time(datetime.datetime)
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.CREATED_TIME
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        if storage_type == StorageType.LOCAL:
            return context_pair.filesystem().created(
                self._strip_storage_protocol(storage_type, actual_path)
            )
        raise GravitinoRuntimeException(
            f"Storage type:{storage_type} doesn't support now."
        )

    def modified(self, path):
        """Returns the modified time of the path file if it exists.
        :param path: Virtual fileset path
        :return Modified time(datetime.datetime)
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.MODIFIED_TIME
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        return context_pair.filesystem().modified(
            self._strip_storage_protocol(storage_type, actual_path)
        )

    def cat_file(self, path, start=None, end=None, **kwargs):
        """Get the content of a file.
        :param path: Virtual fileset path
        :param start: The offset in bytes to start reading from. It can be None.
        :param end: The offset in bytes to end reading at. It can be None.
        :param kwargs: Extra args
        :return File content
        """
        context_pair: FilesetContextPair = self._get_fileset_context(
            path, FilesetDataOperation.CAT_FILE
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        return context_pair.filesystem().cat_file(
            self._strip_storage_protocol(storage_type, actual_path),
            start,
            end,
            **kwargs,
        )

    def get_file(self, rpath, lpath, callback=None, outfile=None, **kwargs):
        """Copy single remote file to local.
        :param rpath: Remote file path
        :param lpath: Local file path
        :param callback: The callback class
        :param outfile: The output file path
        :param kwargs: Extra args
        """
        if not lpath.startswith(f"{StorageType.LOCAL.value}:") and not lpath.startswith(
            "/"
        ):
            raise GravitinoRuntimeException(
                "Doesn't support copy a remote gvfs file to an another remote file."
            )
        context_pair: FilesetContextPair = self._get_fileset_context(
            rpath, FilesetDataOperation.GET_FILE
        )
        actual_path = context_pair.actual_file_location()
        storage_type = self._recognize_storage_type(actual_path)
        context_pair.filesystem().get_file(
            self._strip_storage_protocol(storage_type, actual_path),
            lpath,
            **kwargs,
        )

    def _convert_actual_path(
        self,
        actual_path: str,
        storage_location: str,
        virtual_location: str,
    ):
        """Convert an actual path to a virtual path.
          The virtual path is like `fileset/{catalog}/{schema}/{fileset}/xxx`.
        :param actual_path: Actual path
        :param storage_location: Storage location
        :param virtual_location: Virtual location
        :return A virtual path
        """

        # If the storage path starts with hdfs, gcs, we should use the path as the prefix.
        if (
            storage_location.startswith(f"{StorageType.HDFS.value}://")
            or storage_location.startswith(f"{StorageType.GCS.value}://")
            or storage_location.startswith(f"{StorageType.S3A.value}://")
        ):
            actual_prefix = infer_storage_options(storage_location)["path"]
        elif storage_location.startswith(f"{StorageType.OSS.value}:/"):
            ops = infer_storage_options(storage_location)
            if "host" not in ops or "path" not in ops:
                raise GravitinoRuntimeException(
                    f"Storage location:{storage_location} doesn't support now."
                )

            actual_prefix = ops["host"] + ops["path"]
        elif storage_location.startswith(f"{StorageType.ABS.value}://"):
            ops = infer_storage_options(storage_location)
            if "username" not in ops or "host" not in ops or "path" not in ops:
                raise GravitinoRuntimeException(
                    f"Storage location:{storage_location} doesn't support now, the username,"
                    f"host and path are required in the storage location."
                )
            actual_prefix = f"{StorageType.ABS.value}://{ops['username']}@{ops['host']}{ops['path']}"

            # the actual path may be '{container}/{path}', we need to add the host and username
            # get the path from {container}/{path}
            if not actual_path.startswith(f"{StorageType.ABS}"):
                path_without_username = actual_path[actual_path.index("/") + 1 :]
                actual_path = f"{StorageType.ABS.value}://{ops['username']}@{ops['host']}/{path_without_username}"

        elif storage_location.startswith(f"{StorageType.LOCAL.value}:/"):
            actual_prefix = storage_location[len(f"{StorageType.LOCAL.value}:") :]
        else:
            raise GravitinoRuntimeException(
                f"Storage location:{storage_location} doesn't support now."
            )

        if not actual_path.startswith(actual_prefix):
            raise GravitinoRuntimeException(
                f"Path {actual_path} does not start with valid prefix {actual_prefix}."
            )

        # if the storage location is end with "/",
        # we should truncate this to avoid replace issues.
        if actual_prefix.endswith(self.SLASH) and not virtual_location.endswith(
            self.SLASH
        ):
            return f"{actual_path.replace(actual_prefix[:-1], virtual_location)}"
        return f"{actual_path.replace(actual_prefix, virtual_location)}"

    def _convert_actual_info(
        self,
        entry: Dict,
        storage_location: str,
        virtual_location: str,
    ):
        """Convert a file info from an actual entry to a virtual entry.
        :param entry: A dict of the actual file info
        :param storage_location: Storage location
        :param virtual_location: Virtual location
        :return A dict of the virtual file info
        """
        path = self._convert_actual_path(
            entry["name"], storage_location, virtual_location
        )

        last_modified = None
        if "mtime" in entry:
            # HDFS and GCS
            last_modified = entry["mtime"]
        elif "LastModified" in entry:
            # S3 and OSS
            last_modified = entry["LastModified"]
        elif "last_modified" in entry:
            # Azure Blob Storage
            last_modified = entry["last_modified"]

        return {
            "name": path,
            "size": entry["size"],
            "type": entry["type"],
            "mtime": last_modified,
        }

    def _get_fileset_context(self, virtual_path: str, operation: FilesetDataOperation):
        """Get a fileset context from the cache or the Gravitino server
        :param virtual_path: The virtual path
        :param operation: The data operation
        :return A fileset context pair
        """
        virtual_path: str = self._pre_process_path(virtual_path)
        identifier: NameIdentifier = self._extract_identifier(virtual_path)
        catalog_ident: NameIdentifier = NameIdentifier.of(
            self._metalake, identifier.namespace().level(1)
        )
        fileset_catalog = self._get_fileset_catalog(catalog_ident)
        if fileset_catalog is None:
            raise GravitinoRuntimeException(
                f"Loaded fileset catalog: {catalog_ident} is null."
            )
        sub_path: str = self._get_sub_path_from_virtual_path(identifier, virtual_path)
        context = {
            FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION: operation.name,
            FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE: InternalClientType.PYTHON_GVFS.name,
        }
        caller_context: CallerContext = CallerContext(context)
        CallerContextHolder.set(caller_context)
        actual_file_location: (
            str
        ) = fileset_catalog.as_fileset_catalog().get_file_location(
            NameIdentifier.of(identifier.namespace().level(2), identifier.name()),
            sub_path,
        )

        return FilesetContextPair(
            actual_file_location,
            self._get_filesystem(actual_file_location, fileset_catalog, identifier),
        )

    def _extract_identifier(self, path):
        """Extract the fileset identifier from the path.
        :param path: The virtual fileset path
        :return The fileset identifier
        """
        if path is None:
            raise GravitinoRuntimeException(
                "path which need be extracted cannot be null or empty."
            )

        match = self._identifier_pattern.match(path)
        if match and len(match.groups()) == 3:
            return NameIdentifier.of(
                self._metalake, match.group(1), match.group(2), match.group(3)
            )
        raise GravitinoRuntimeException(
            f"path: `{path}` doesn't contains valid identifier."
        )

    @staticmethod
    def _get_virtual_location(identifier: NameIdentifier):
        """Get the virtual location of the fileset.
        :param identifier: The name identifier of the fileset
        :return The virtual location.
        """
        return (
            f"fileset/{identifier.namespace().level(1)}"
            f"/{identifier.namespace().level(2)}"
            f"/{identifier.name()}"
        )

    @staticmethod
    def _pre_process_path(virtual_path):
        """Pre-process the path.
         We will uniformly process `gvfs://fileset/{catalog}/{schema}/{fileset_name}/xxx`
         into the format of `fileset/{catalog}/{schema}/{fileset_name}/xxx`.
         This is because some implementations of `PyArrow` and `fsspec` can only recognize this format.
        :param virtual_path: The virtual path
        :return The pre-processed path
        """
        if isinstance(virtual_path, PurePosixPath):
            pre_processed_path = virtual_path.as_posix()
        else:
            pre_processed_path = virtual_path
        gvfs_prefix = f"{PROTOCOL_NAME}://"
        if pre_processed_path.startswith(gvfs_prefix):
            pre_processed_path = pre_processed_path[len(gvfs_prefix) :]
        if not pre_processed_path.startswith("fileset/"):
            raise GravitinoRuntimeException(
                f"Invalid path:`{pre_processed_path}`. Expected path to start with `fileset/`."
                " Example: fileset/{fileset_catalog}/{schema}/{fileset_name}/{sub_path}."
            )
        return pre_processed_path

    @staticmethod
    def _recognize_storage_type(path: str):
        """Recognize the storage type by the path.
        :param path: The path
        :return: The storage type
        """
        if path.startswith(f"{StorageType.HDFS.value}://"):
            return StorageType.HDFS
        if path.startswith(f"{StorageType.LOCAL.value}:/"):
            return StorageType.LOCAL
        if path.startswith(f"{StorageType.GCS.value}://"):
            return StorageType.GCS
        if path.startswith(f"{StorageType.S3A.value}://"):
            return StorageType.S3A
        if path.startswith(f"{StorageType.OSS.value}://"):
            return StorageType.OSS
        if path.startswith(f"{StorageType.ABS.value}://"):
            return StorageType.ABS
        raise GravitinoRuntimeException(
            f"Storage type doesn't support now. Path:{path}"
        )

    @staticmethod
    def _get_sub_path_from_virtual_path(identifier: NameIdentifier, virtual_path: str):
        return virtual_path[
            len(
                f"fileset/{identifier.namespace().level(1)}/{identifier.namespace().level(2)}/{identifier.name()}"
            ) :
        ]

    @staticmethod
    def _strip_storage_protocol(storage_type: StorageType, path: str):
        """Strip the storage protocol from the path.
          Before passing the path to the underlying file system for processing,
           pre-process the protocol information in the path.
          Some file systems require special processing.
          For HDFS, we can pass the path like 'hdfs://{host}:{port}/xxx'.
          For Local, we can pass the path like '/tmp/xxx'.
        :param storage_type: The storage type
        :param path: The path
        :return: The stripped path

        We will handle OSS differently from S3 and GCS, because OSS has different behavior than S3 and GCS.
        Please see the following example:

        ```
        >> oss = context_pair.filesystem()
        >> oss.ls('oss://bucket-xiaoyu/test_gvfs_catalog678/test_gvfs_schema/test_gvfs_fileset/test_ls')
            DEBUG:ossfs:Get directory listing page for bucket-xiaoyu/test_gvfs_catalog678/
            test_gvfs_schema/test_gvfs_fileset
            DEBUG:ossfs:CALL: ObjectIterator - () - {'prefix': 'test_gvfs_catalog678/test_gvfs_schema
            /test_gvfs_fileset/', 'delimiter': '/'}
            []
        >> oss.ls('bucket-xiaoyu/test_gvfs_catalog678/test_gvfs_schema/test_gvfs_fileset/test_ls')
            DEBUG:ossfs:Get directory listing page for bucket-xiaoyu/test_gvfs_catalog678/test_gvfs_schema
            /test_gvfs_fileset/test_ls
            DEBUG:ossfs:CALL: ObjectIterator - () - {'prefix': 'test_gvfs_catalog678/test_gvfs_schema
            /test_gvfs_fileset/test_ls/', 'delimiter': '/'}
            [{'name': 'bucket-xiaoyu/test_gvfs_catalog678/test_gvfs_schema/test_gvfs_fileset/test_ls
            /test.file', 'type': 'file', 'size': 0, 'LastModified': 1729754793,
            'Size': 0, 'Key': 'bucket-xiaoyu/test_gvfs_catalog678/test_gvfs_schema/
            test_gvfs_fileset/test_ls/test.file'}]

        ```

        Please take a look at the above example: if we do not remove the protocol (starts with oss://),
        it will always return an empty array when we call `oss.ls`, however, if we remove the protocol,
        it will produce the correct result as expected.
        """
        if storage_type in (StorageType.HDFS, StorageType.GCS, StorageType.S3A):
            return path
        if storage_type == StorageType.LOCAL:
            return path[len(f"{StorageType.LOCAL.value}:") :]

        ## We need to remove the protocol and accout from the path, for instance,
        # the path can be converted from 'abfss://container@account/path' to
        # 'container/path'.
        if storage_type == StorageType.ABS:
            ops = infer_storage_options(path)
            return ops["username"] + ops["path"]

        # OSS has different behavior than S3 and GCS, if we do not remove the
        # protocol, it will always return an empty array.
        if storage_type == StorageType.OSS:
            if path.startswith(f"{StorageType.OSS.value}://"):
                return path[len(f"{StorageType.OSS.value}://") :]
            return path

        raise GravitinoRuntimeException(
            f"Storage type:{storage_type} doesn't support now."
        )

    @staticmethod
    def _check_auth_config(auth_type: str, config_key: str, config_value: str):
        """Check if the config value is null.
        :param auth_type: The auth type
        :param config_key: The config key
        :param config_value: The config value
        """
        if config_value is None:
            raise GravitinoRuntimeException(
                f"{config_key} should not be null"
                f" if {GVFSConfig.AUTH_TYPE} is set to {auth_type}."
            )

    def _get_fileset_catalog(self, catalog_ident: NameIdentifier):
        read_lock = self._catalog_cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[NameIdentifier, FilesetCatalog] = (
                self._catalog_cache.get(catalog_ident)
            )
            if cache_value is not None:
                return cache_value
        finally:
            read_lock.release()

        write_lock = self._catalog_cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: Tuple[NameIdentifier, FilesetCatalog] = (
                self._catalog_cache.get(catalog_ident)
            )
            if cache_value is not None:
                return cache_value
            catalog = self._client.load_catalog(catalog_ident.name())
            self._catalog_cache[catalog_ident] = catalog
            return catalog
        finally:
            write_lock.release()

    def _file_system_expired(self, expire_time: int):
        return expire_time <= time.time() * 1000

    # Disable Too many branches (13/12) (too-many-branches)
    # pylint: disable=R0912
    def _get_filesystem(
        self,
        actual_file_location: str,
        fileset_catalog: Catalog,
        name_identifier: NameIdentifier,
    ):
        storage_type = self._recognize_storage_type(actual_file_location)
        read_lock = self._cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[int, AbstractFileSystem] = self._cache.get(
                name_identifier
            )
            if cache_value is not None:
                if not self._file_system_expired(cache_value[0]):
                    return cache_value[1]
        finally:
            read_lock.release()

        write_lock = self._cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: Tuple[int, AbstractFileSystem] = self._cache.get(
                name_identifier
            )

            if cache_value is not None:
                if not self._file_system_expired(cache_value[0]):
                    return cache_value[1]

            new_cache_value: Tuple[int, AbstractFileSystem]
            if storage_type == StorageType.HDFS:
                fs_class = importlib.import_module("pyarrow.fs").HadoopFileSystem
                new_cache_value = (
                    TIME_WITHOUT_EXPIRATION,
                    ArrowFSWrapper(fs_class.from_uri(actual_file_location)),
                )
            elif storage_type == StorageType.LOCAL:
                new_cache_value = (TIME_WITHOUT_EXPIRATION, LocalFileSystem())
            elif storage_type == StorageType.GCS:
                new_cache_value = self._get_gcs_filesystem(
                    fileset_catalog, name_identifier
                )
            elif storage_type == StorageType.S3A:
                new_cache_value = self._get_s3_filesystem(
                    fileset_catalog, name_identifier
                )
            elif storage_type == StorageType.OSS:
                new_cache_value = self._get_oss_filesystem(
                    fileset_catalog, name_identifier
                )
            elif storage_type == StorageType.ABS:
                new_cache_value = self._get_abs_filesystem(
                    fileset_catalog, name_identifier
                )
            else:
                raise GravitinoRuntimeException(
                    f"Storage type: `{storage_type}` doesn't support now."
                )
            self._cache[name_identifier] = new_cache_value
            return new_cache_value[1]
        finally:
            write_lock.release()

    def _get_gcs_filesystem(self, fileset_catalog: Catalog, identifier: NameIdentifier):
        fileset: GenericFileset = fileset_catalog.as_fileset_catalog().load_fileset(
            NameIdentifier.of(identifier.namespace().level(2), identifier.name())
        )
        credentials = fileset.support_credentials().get_credentials()

        credential = self._get_most_suitable_gcs_credential(credentials)
        if credential is not None:
            expire_time = self._get_expire_time_by_ratio(credential.expire_time_in_ms())
            if isinstance(credential, GCSTokenCredential):
                fs = importlib.import_module("gcsfs").GCSFileSystem(
                    token=credential.token()
                )
                return (expire_time, fs)

        # get 'service-account-key' from gcs_options, if the key is not found, throw an exception
        service_account_key_path = self._options.get(
            GVFSConfig.GVFS_FILESYSTEM_GCS_SERVICE_KEY_FILE
        )
        if service_account_key_path is None:
            raise GravitinoRuntimeException(
                "Service account key is not found in the options."
            )
        return (
            TIME_WITHOUT_EXPIRATION,
            importlib.import_module("gcsfs").GCSFileSystem(
                token=service_account_key_path
            ),
        )

    def _get_s3_filesystem(self, fileset_catalog: Catalog, identifier: NameIdentifier):
        fileset: GenericFileset = fileset_catalog.as_fileset_catalog().load_fileset(
            NameIdentifier.of(identifier.namespace().level(2), identifier.name())
        )
        credentials = fileset.support_credentials().get_credentials()
        credential = self._get_most_suitable_s3_credential(credentials)

        # S3 endpoint from gravitino server, Note: the endpoint may not a real S3 endpoint
        # it can be a simulated S3 endpoint, such as minio, so though the endpoint is not a required field
        # for S3FileSystem, we still need to assign the endpoint to the S3FileSystem
        s3_endpoint = fileset_catalog.properties().get("s3-endpoint", None)
        # If the oss endpoint is not found in the fileset catalog, get it from the client options
        if s3_endpoint is None:
            s3_endpoint = self._options.get(GVFSConfig.GVFS_FILESYSTEM_S3_ENDPOINT)

        if credential is not None:
            expire_time = self._get_expire_time_by_ratio(credential.expire_time_in_ms())
            if isinstance(credential, S3TokenCredential):
                fs = importlib.import_module("s3fs").S3FileSystem(
                    key=credential.access_key_id(),
                    secret=credential.secret_access_key(),
                    token=credential.session_token(),
                    endpoint_url=s3_endpoint,
                )
                return (expire_time, fs)
            if isinstance(credential, S3SecretKeyCredential):
                fs = importlib.import_module("s3fs").S3FileSystem(
                    key=credential.access_key_id(),
                    secret=credential.secret_access_key(),
                    endpoint_url=s3_endpoint,
                )
                return (expire_time, fs)

        # this is the old way to get the s3 file system
        # get 'aws_access_key_id' from s3_options, if the key is not found, throw an exception
        aws_access_key_id = self._options.get(GVFSConfig.GVFS_FILESYSTEM_S3_ACCESS_KEY)
        if aws_access_key_id is None:
            raise GravitinoRuntimeException(
                "AWS access key id is not found in the options."
            )

        # get 'aws_secret_access_key' from s3_options, if the key is not found, throw an exception
        aws_secret_access_key = self._options.get(
            GVFSConfig.GVFS_FILESYSTEM_S3_SECRET_KEY
        )
        if aws_secret_access_key is None:
            raise GravitinoRuntimeException(
                "AWS secret access key is not found in the options."
            )

        return (
            TIME_WITHOUT_EXPIRATION,
            importlib.import_module("s3fs").S3FileSystem(
                key=aws_access_key_id,
                secret=aws_secret_access_key,
                endpoint_url=s3_endpoint,
            ),
        )

    def _get_oss_filesystem(self, fileset_catalog: Catalog, identifier: NameIdentifier):
        fileset: GenericFileset = fileset_catalog.as_fileset_catalog().load_fileset(
            NameIdentifier.of(identifier.namespace().level(2), identifier.name())
        )
        credentials = fileset.support_credentials().get_credentials()

        # OSS endpoint from gravitino server
        oss_endpoint = fileset_catalog.properties().get("oss-endpoint", None)
        # If the oss endpoint is not found in the fileset catalog, get it from the client options
        if oss_endpoint is None:
            oss_endpoint = self._options.get(GVFSConfig.GVFS_FILESYSTEM_OSS_ENDPOINT)

        credential = self._get_most_suitable_oss_credential(credentials)
        if credential is not None:
            expire_time = self._get_expire_time_by_ratio(credential.expire_time_in_ms())
            if isinstance(credential, OSSTokenCredential):
                fs = importlib.import_module("ossfs").OSSFileSystem(
                    key=credential.access_key_id(),
                    secret=credential.secret_access_key(),
                    token=credential.security_token(),
                    endpoint=oss_endpoint,
                )
                return (expire_time, fs)
            if isinstance(credential, OSSSecretKeyCredential):
                return (
                    expire_time,
                    importlib.import_module("ossfs").OSSFileSystem(
                        key=credential.access_key_id(),
                        secret=credential.secret_access_key(),
                        endpoint=oss_endpoint,
                    ),
                )

        # get 'oss_access_key_id' from oss options, if the key is not found, throw an exception
        oss_access_key_id = self._options.get(GVFSConfig.GVFS_FILESYSTEM_OSS_ACCESS_KEY)
        if oss_access_key_id is None:
            raise GravitinoRuntimeException(
                "OSS access key id is not found in the options."
            )

        # get 'oss_secret_access_key' from oss options, if the key is not found, throw an exception
        oss_secret_access_key = self._options.get(
            GVFSConfig.GVFS_FILESYSTEM_OSS_SECRET_KEY
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
            ),
        )

    def _get_abs_filesystem(self, fileset_catalog: Catalog, identifier: NameIdentifier):
        fileset: GenericFileset = fileset_catalog.as_fileset_catalog().load_fileset(
            NameIdentifier.of(identifier.namespace().level(2), identifier.name())
        )
        credentials = fileset.support_credentials().get_credentials()

        credential = self._get_most_suitable_abs_credential(credentials)
        if credential is not None:
            expire_time = self._get_expire_time_by_ratio(credential.expire_time_in_ms())
            if isinstance(credential, ADLSTokenCredential):
                fs = importlib.import_module("adlfs").AzureBlobFileSystem(
                    account_name=credential.account_name(),
                    sas_token=credential.sas_token(),
                )
                return (expire_time, fs)

            if isinstance(credential, AzureAccountKeyCredential):
                fs = importlib.import_module("adlfs").AzureBlobFileSystem(
                    account_name=credential.account_name(),
                    account_key=credential.account_key(),
                )
                return (expire_time, fs)

        # get 'abs_account_name' from abs options, if the key is not found, throw an exception
        abs_account_name = self._options.get(
            GVFSConfig.GVFS_FILESYSTEM_AZURE_ACCOUNT_NAME
        )
        if abs_account_name is None:
            raise GravitinoRuntimeException(
                "ABS account name is not found in the options."
            )

        # get 'abs_account_key' from abs options, if the key is not found, throw an exception
        abs_account_key = self._options.get(
            GVFSConfig.GVFS_FILESYSTEM_AZURE_ACCOUNT_KEY
        )
        if abs_account_key is None:
            raise GravitinoRuntimeException(
                "ABS account key is not found in the options."
            )

        return (
            TIME_WITHOUT_EXPIRATION,
            importlib.import_module("adlfs").AzureBlobFileSystem(
                account_name=abs_account_name,
                account_key=abs_account_key,
            ),
        )

    def _get_most_suitable_s3_credential(self, credentials: List[Credential]):
        for credential in credentials:
            # Prefer to use the token credential, if it does not exist, use the
            # secret key credential.
            if isinstance(credential, S3TokenCredential):
                return credential

        for credential in credentials:
            if isinstance(credential, S3SecretKeyCredential):
                return credential
        return None

    def _get_most_suitable_oss_credential(self, credentials: List[Credential]):
        for credential in credentials:
            # Prefer to use the token credential, if it does not exist, use the
            # secret key credential.
            if isinstance(credential, OSSTokenCredential):
                return credential

        for credential in credentials:
            if isinstance(credential, OSSSecretKeyCredential):
                return credential
        return None

    def _get_most_suitable_gcs_credential(self, credentials: List[Credential]):
        for credential in credentials:
            # Prefer to use the token credential, if it does not exist, return None.
            if isinstance(credential, GCSTokenCredential):
                return credential
        return None

    def _get_most_suitable_abs_credential(self, credentials: List[Credential]):
        for credential in credentials:
            # Prefer to use the token credential, if it does not exist, use the
            # account key credential
            if isinstance(credential, ADLSTokenCredential):
                return credential

        for credential in credentials:
            if isinstance(credential, AzureAccountKeyCredential):
                return credential
        return None

    def _get_expire_time_by_ratio(self, expire_time: int):
        if expire_time <= 0:
            return TIME_WITHOUT_EXPIRATION

        ratio = float(
            self._options.get(
                GVFSConfig.GVFS_FILESYSTEM_CREDENTIAL_EXPIRED_TIME_RATIO,
                GVFSConfig.DEFAULT_CREDENTIAL_EXPIRED_TIME_RATIO,
            )
        )
        return time.time() * 1000 + (expire_time - time.time() * 1000) * ratio


fsspec.register_implementation(PROTOCOL_NAME, GravitinoVirtualFileSystem)
