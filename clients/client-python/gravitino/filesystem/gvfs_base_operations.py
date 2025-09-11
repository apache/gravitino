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
import os
import sys
import time
from abc import ABC, abstractmethod
from pathlib import PurePosixPath
from typing import Dict, Tuple

from cachetools import TTLCache, LRUCache
from fsspec import AbstractFileSystem
from readerwriterlock import rwlock

from gravitino.api.catalog import Catalog
from gravitino.api.credential.credential import Credential
from gravitino.api.file.fileset import Fileset
from gravitino.audit.caller_context import CallerContextHolder, CallerContext
from gravitino.audit.fileset_audit_constants import FilesetAuditConstants
from gravitino.audit.fileset_data_operation import FilesetDataOperation
from gravitino.audit.internal_client_type import InternalClientType
from gravitino.client.fileset_catalog import FilesetCatalog
from gravitino.client.generic_fileset import GenericFileset
from gravitino.client.gravitino_client_config import GravitinoClientConfig
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
    NoSuchLocationNameException,
)
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.filesystem.gvfs_storage_handler import get_storage_handler_by_path
from gravitino.filesystem.gvfs_utils import (
    get_sub_path_from_virtual_path,
    extract_identifier,
    create_client,
)
from gravitino.name_identifier import NameIdentifier

logger = logging.getLogger(__name__)

PROTOCOL_NAME = "gvfs"

TIME_WITHOUT_EXPIRATION = sys.maxsize


class FilesetPathNotFoundError(FileNotFoundError):
    """Exception raised when the catalog, schema or fileset is not found in the GVFS path."""


class BaseGVFSOperations(ABC):
    """
    Abstract base class for Gravitino Virtual File System operations.

    This class provides the core functionality for interacting with the Gravitino Virtual File
    System, including operations for file manipulation, directory management, and credential
    handling. It handles the mapping between virtual paths in the Gravitino namespace and actual file
    paths in underlying storage systems.

    Implementations of this class should provide the specific behaviors for each file system
    operation.
    """

    # pylint: disable=too-many-instance-attributes

    SLASH = "/"

    ENV_CURRENT_LOCATION_NAME_ENV_VAR_DEFAULT = "CURRENT_LOCATION_NAME"
    ENABLE_CREDENTIAL_VENDING_DEFAULT = False
    ENABLE_FILESET_METADATA_CACHE_DEFAULT = False

    def __init__(
        self,
        server_uri: str = None,
        metalake_name: str = None,
        options: Dict = None,
        **kwargs,
    ):
        self._metalake = metalake_name
        self._options = options

        request_headers = (
            None
            if options is None
            else {
                key[
                    len(GVFSConfig.GVFS_FILESYSTEM_CLIENT_REQUEST_HEADER_PREFIX) :
                ]: value
                for key, value in options.items()
                if key.startswith(
                    GVFSConfig.GVFS_FILESYSTEM_CLIENT_REQUEST_HEADER_PREFIX
                )
            }
        )

        client_config = (
            None
            if options is None
            else {
                key.replace(
                    GVFSConfig.GVFS_FILESYSTEM_CLIENT_CONFIG_PREFIX,
                    GravitinoClientConfig.GRAVITINO_CLIENT_CONFIG_PREFIX,
                ): value
                for key, value in options.items()
                if key.startswith(GVFSConfig.GVFS_FILESYSTEM_CLIENT_CONFIG_PREFIX)
            }
        )

        self._client = create_client(
            options, server_uri, metalake_name, request_headers, client_config
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
        self._filesystem_cache = TTLCache(maxsize=cache_size, ttl=cache_expired_time)
        self._cache_lock = rwlock.RWLockFair()

        self._enable_fileset_metadata_cache = (
            self.ENABLE_FILESET_METADATA_CACHE_DEFAULT
            if options is None
            else options.get(
                GVFSConfig.GVFS_FILESYSTEM_ENABLE_FILESET_METADATA_CACHE,
                self.ENABLE_FILESET_METADATA_CACHE_DEFAULT,
            )
        )
        if self._enable_fileset_metadata_cache:
            self._catalog_cache = LRUCache(maxsize=100)
            self._catalog_cache_lock = rwlock.RWLockFair()

            self._fileset_cache = LRUCache(maxsize=10000)
            self._fileset_cache_lock = rwlock.RWLockFair()

        self._enable_credential_vending = (
            False
            if options is None
            else options.get(
                GVFSConfig.GVFS_FILESYSTEM_ENABLE_CREDENTIAL_VENDING,
                self.ENABLE_CREDENTIAL_VENDING_DEFAULT,
            )
        )
        self._current_location_name = self._init_current_location_name()
        self._kwargs = kwargs

    @property
    def current_location_name(self):
        return self._current_location_name

    @abstractmethod
    def ls(self, path, detail=True, **kwargs):
        """List the files and directories info of the path.
        :param path: Virtual fileset path
        :param detail: Whether to show the details for the files and directories info
        :param kwargs: Extra args
        :return If details is true, returns a list of file info dicts, else returns a list of file paths
        """
        pass

    @abstractmethod
    def info(self, path, **kwargs):
        """Get file info.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return A file info dict
        """
        pass

    @abstractmethod
    def exists(self, path, **kwargs):
        """Check if a file or a directory exists.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return If a file or directory exists, it returns True, otherwise False
        """
        pass

    @abstractmethod
    def cp_file(self, path1, path2, **kwargs):
        """Copy a file.
        :param path1: Virtual src fileset path
        :param path2: Virtual dst fileset path, should be consistent with the src path fileset identifier
        :param kwargs: Extra args
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def rm(self, path, recursive=False, maxdepth=None):
        """Remove a file or directory.
        :param path: Virtual fileset path
        :param recursive: Whether to remove the directory recursively.
                When removing a directory, this parameter should be True.
        :param maxdepth: The maximum depth to remove the directory recursively.
        """
        pass

    @abstractmethod
    def rm_file(self, path):
        """Remove a file.
        :param path: Virtual fileset path
        """
        pass

    @abstractmethod
    def rmdir(self, path):
        """Remove a directory.
        It will delete a directory and all its contents recursively for PyArrow.HadoopFileSystem.
        And it will throw an exception if delete a directory which is non-empty for LocalFileSystem.
        :param path: Virtual fileset path
        """
        pass

    @abstractmethod
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
        pass

    @abstractmethod
    def mkdir(self, path, create_parents=True, **kwargs):
        """Make a directory.
        if create_parents=True, this is equivalent to ``makedirs``.

        :param path: Virtual fileset path
        :param create_parents: Create parent directories if missing when set to True
        :param kwargs: Extra args
        """
        pass

    @abstractmethod
    def makedirs(self, path, exist_ok=True):
        """Make a directory recursively.
        :param path: Virtual fileset path
        :param exist_ok: Continue if a directory already exists
        """
        pass

    @abstractmethod
    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime
        Only supports for `fsspec.LocalFileSystem` now.
        :param path: Virtual fileset path
        :return Created time(datetime.datetime)
        """
        pass

    @abstractmethod
    def modified(self, path):
        """Returns the modified time of the path file if it exists.
        :param path: Virtual fileset path
        :return Modified time(datetime.datetime)
        """
        pass

    @abstractmethod
    def cat_file(self, path, start=None, end=None, **kwargs):
        """Get the content of a file.
        :param path: Virtual fileset path
        :param start: The offset in bytes to start reading from. It can be None.
        :param end: The offset in bytes to end reading at. It can be None.
        :param kwargs: Extra args
        :return File content
        """
        pass

    @abstractmethod
    def get_file(self, rpath, lpath, callback=None, outfile=None, **kwargs):
        """Copy single remote file to local.
        :param rpath: Remote file path
        :param lpath: Local file path
        :param callback: The callback class
        :param outfile: The output file path
        :param kwargs: Extra args
        """
        pass

    @staticmethod
    def _pre_process_path(virtual_path):
        """Pre-process the path.
         This method uniformly processes `gvfs://fileset/{catalog}/{schema}/{fileset_name}/xxx`
         into the format of `fileset/{catalog}/{schema}/{fileset_name}/xxx`.
         The conversion is necessary because some implementations of `PyArrow` and `fsspec`
         can only recognize this format.
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

    def _assert_same_fileset(self, gvfs_path1: str, gvfs_path2: str):
        """Assert that the two gvfs paths are the same fileset.
        :param gvfs_path1: The first gvfs path
        :param gvfs_path2: The second gvfs path
        """
        src_path = self._pre_process_path(gvfs_path1)
        dst_path = self._pre_process_path(gvfs_path2)
        src_identifier: NameIdentifier = extract_identifier(self._metalake, src_path)
        dst_identifier: NameIdentifier = extract_identifier(self._metalake, dst_path)
        if src_identifier != dst_identifier:
            raise GravitinoRuntimeException(
                f"Destination file path identifier: `{dst_identifier}` should be same with src file path "
                f"identifier: `{src_identifier}`."
            )

    def _init_current_location_name(self):
        """Initialize the current location name.
         get from configuration first, otherwise use the env variable
         if both are not set, return null which means use the default location
        :return: The current location name
        """
        current_location_name_env_var = (
            self._options.get(GVFSConfig.GVFS_FILESYSTEM_CURRENT_LOCATION_NAME_ENV_VAR)
            if self._options
            else None
        ) or self.ENV_CURRENT_LOCATION_NAME_ENV_VAR_DEFAULT

        return (
            self._options.get(GVFSConfig.GVFS_FILESYSTEM_CURRENT_LOCATION_NAME)
            if self._options
            else None
        ) or os.environ.get(current_location_name_env_var)

    def _get_actual_filesystem(
        self, gvfs_path: str, location_name: str
    ) -> AbstractFileSystem:
        """Get the actual filesystem based on the gvfs path.
        :param gvfs_path: The gvfs path
        :param location_name: The location name, None means the default location
        :return: The actual filesystem
        """
        fileset_ident: NameIdentifier = extract_identifier(self._metalake, gvfs_path)
        return self._get_actual_filesystem_by_location_name(
            fileset_ident, location_name
        )

    def _get_actual_filesystem_by_location_name(
        self, fileset_ident: NameIdentifier, location_name: str
    ) -> AbstractFileSystem:
        """Get the actual filesystem based on the fileset identifier and location name.
        :param fileset_ident: The fileset identifier
        :param location_name: The location name, None means the default location
        :return: The actual filesystem
        """
        catalog_ident: NameIdentifier = NameIdentifier.of(
            self._metalake, fileset_ident.namespace().level(1)
        )
        catalog = self._get_fileset_catalog(catalog_ident)
        fileset = self._get_fileset(fileset_ident)
        target_location_name = (
            location_name
            or fileset.properties().get(fileset.PROPERTY_DEFAULT_LOCATION_NAME)
            or fileset.LOCATION_NAME_UNKNOWN
        )
        actual_location = fileset.storage_locations().get(target_location_name)
        if actual_location is None:
            raise NoSuchLocationNameException(
                f"Cannot find the location: {target_location_name} in fileset: {fileset_ident}"
            )
        actual_fs = self._get_filesystem(
            actual_location, catalog, fileset_ident, target_location_name
        )
        self._create_fileset_location_if_needed(
            catalog.properties(), actual_fs, actual_location
        )
        return actual_fs

    def _create_fileset_location_if_needed(
        self,
        catalog_props: Dict[str, str],
        actual_fs: AbstractFileSystem,
        fileset_path: str,
    ):
        # If the server-side filesystem ops are disabled, the fileset directory may not exist. In
        # such case the operations like create, open, list files under this directory will fail.
        # So we need to check the existence of the fileset directory beforehand.
        fs_ops_disabled = catalog_props.get("disable-filesystem-ops", "false")
        if fs_ops_disabled.lower() == "true":
            if not actual_fs.exists(fileset_path):
                actual_fs.makedir(fileset_path, create_parents=True)
                logger.info(
                    "Automatically created a directory for fileset path: %s when "
                    "disable-filesystem-ops sets to true in the server side",
                    fileset_path,
                )

    def _get_actual_file_path(
        self, gvfs_path: str, location_name: str, operation: FilesetDataOperation
    ) -> str:
        """Get the actual file path by the given virtual path and location name.
        :param gvfs_path: The gvfs path
        :param location_name: The location name
        :param operation: The data operation
        :return: The actual file path
        """
        processed_virtual_path: str = self._pre_process_path(gvfs_path)
        identifier: NameIdentifier = extract_identifier(
            self._metalake, processed_virtual_path
        )
        catalog_ident: NameIdentifier = NameIdentifier.of(
            self._metalake, identifier.namespace().level(1)
        )
        fileset_catalog = self._get_fileset_catalog(catalog_ident).as_fileset_catalog()

        sub_path: str = get_sub_path_from_virtual_path(
            identifier, processed_virtual_path
        )
        context = {
            FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION: operation.name,
            FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE: InternalClientType.PYTHON_GVFS.name,
        }
        caller_context: CallerContext = CallerContext(context)
        CallerContextHolder.set(caller_context)

        return fileset_catalog.get_file_location(
            NameIdentifier.of(identifier.namespace().level(2), identifier.name()),
            sub_path,
            location_name,
        )

    def _file_system_expired(self, expire_time: int):
        return expire_time <= time.time() * 1000

    def _get_filesystem(
        self,
        actual_file_location: str,
        fileset_catalog: Catalog,
        name_identifier: NameIdentifier,
        location_name: str,
    ):
        read_lock = self._cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[int, AbstractFileSystem] = self._filesystem_cache.get(
                (name_identifier, location_name)
            )
            if cache_value is not None:
                if not self._file_system_expired(cache_value[0]):
                    return cache_value[1]
        finally:
            read_lock.release()

        write_lock = self._cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: Tuple[int, AbstractFileSystem] = self._filesystem_cache.get(
                (name_identifier, location_name)
            )

            if cache_value is not None:
                if not self._file_system_expired(cache_value[0]):
                    return cache_value[1]

            fileset: GenericFileset = fileset_catalog.as_fileset_catalog().load_fileset(
                NameIdentifier.of(
                    name_identifier.namespace().level(2), name_identifier.name()
                )
            )
            if location_name:
                context = {
                    Credential.HTTP_HEADER_CURRENT_LOCATION_NAME: location_name,
                }
                caller_context: CallerContext = CallerContext(context)
                CallerContextHolder.set(caller_context)
            credentials = (
                fileset.support_credentials().get_credentials()
                if self._enable_credential_vending
                else None
            )
            new_cache_value = get_storage_handler_by_path(
                actual_file_location
            ).get_filesystem_with_expiration(
                credentials,
                fileset_catalog.properties(),
                self._options,
                actual_file_location,
                **self._kwargs,
            )
            self._filesystem_cache[(name_identifier, location_name)] = new_cache_value
            return new_cache_value[1]
        finally:
            write_lock.release()
            CallerContextHolder.remove()

    def _get_fileset_catalog(self, catalog_ident: NameIdentifier):
        if not self._enable_fileset_metadata_cache:
            return self._client.load_catalog(catalog_ident.name())

        read_lock = self._catalog_cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: FilesetCatalog = self._catalog_cache.get(catalog_ident)
            if cache_value is not None:
                return cache_value
        finally:
            read_lock.release()

        write_lock = self._catalog_cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: FilesetCatalog = self._catalog_cache.get(catalog_ident)
            if cache_value is not None:
                return cache_value
            catalog = self._client.load_catalog(catalog_ident.name())
            self._catalog_cache[catalog_ident] = catalog
            return catalog
        finally:
            write_lock.release()

    def _get_fileset(self, fileset_ident: NameIdentifier):
        if not self._enable_fileset_metadata_cache:
            catalog_ident: NameIdentifier = NameIdentifier.of(
                fileset_ident.namespace().level(0), fileset_ident.namespace().level(1)
            )
            catalog: FilesetCatalog = self._get_fileset_catalog(catalog_ident)
            return catalog.as_fileset_catalog().load_fileset(
                NameIdentifier.of(
                    fileset_ident.namespace().level(2), fileset_ident.name()
                )
            )

        read_lock = self._fileset_cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Fileset = self._fileset_cache.get(fileset_ident)
            if cache_value is not None:
                return cache_value
        finally:
            read_lock.release()

        write_lock = self._fileset_cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: Fileset = self._fileset_cache.get(fileset_ident)
            if cache_value is not None:
                return cache_value

            catalog_ident: NameIdentifier = NameIdentifier.of(
                fileset_ident.namespace().level(0), fileset_ident.namespace().level(1)
            )
            catalog: FilesetCatalog = self._get_fileset_catalog(catalog_ident)
            fileset = catalog.as_fileset_catalog().load_fileset(
                NameIdentifier.of(
                    fileset_ident.namespace().level(2), fileset_ident.name()
                )
            )
            self._fileset_cache[fileset_ident] = fileset
            return fileset
        finally:
            write_lock.release()
