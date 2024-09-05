"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from enum import Enum
from pathlib import PurePosixPath
from typing import Dict, Tuple
import re
import fsspec

from cachetools import TTLCache
from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.utils import infer_storage_options
from pyarrow.fs import HadoopFileSystem
from readerwriterlock import rwlock
from gravitino.api.catalog import Catalog
from gravitino.api.fileset import Fileset
from gravitino.auth.simple_auth_provider import SimpleAuthProvider
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.exceptions.base import GravitinoRuntimeException
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.name_identifier import NameIdentifier

PROTOCOL_NAME = "gvfs"


class StorageType(Enum):
    HDFS = "hdfs"
    LOCAL = "file"


class FilesetContext:
    """A context object that holds the information about the fileset and the file system which used in
    the GravitinoVirtualFileSystem's operations.
    """

    def __init__(
        self,
        name_identifier: NameIdentifier,
        fileset: Fileset,
        fs: AbstractFileSystem,
        storage_type: StorageType,
        actual_path: str,
    ):
        self._name_identifier = name_identifier
        self._fileset = fileset
        self._fs = fs
        self._storage_type = storage_type
        self._actual_path = actual_path

    def get_name_identifier(self):
        return self._name_identifier

    def get_fileset(self):
        return self._fileset

    def get_fs(self):
        return self._fs

    def get_actual_path(self):
        return self._actual_path

    def get_storage_type(self):
        return self._storage_type


class GravitinoVirtualFileSystem(fsspec.AbstractFileSystem):
    """This is a virtual file system which users can access `fileset` and
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
            GVFSConfig.DEFAULT_AUTH_TYPE
            if options is None
            else options.get(GVFSConfig.AUTH_TYPE, GVFSConfig.DEFAULT_AUTH_TYPE)
        )
        if auth_type == GVFSConfig.DEFAULT_AUTH_TYPE:
            self._client = GravitinoClient(
                uri=server_uri,
                metalake_name=metalake_name,
                auth_data_provider=SimpleAuthProvider(),
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
        context: FilesetContext = self._get_fileset_context(path)
        if detail:
            entries = [
                self._convert_actual_info(entry, context)
                for entry in context.get_fs().ls(
                    self._strip_storage_protocol(
                        context.get_storage_type(), context.get_actual_path()
                    ),
                    detail=True,
                )
            ]
            return entries
        entries = [
            self._convert_actual_path(entry_path, context)
            for entry_path in context.get_fs().ls(
                self._strip_storage_protocol(
                    context.get_storage_type(), context.get_actual_path()
                ),
                detail=False,
            )
        ]
        return entries

    def info(self, path, **kwargs):
        """Get file info.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return A file info dict
        """
        context: FilesetContext = self._get_fileset_context(path)
        actual_info: Dict = context.get_fs().info(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            )
        )
        return self._convert_actual_info(actual_info, context)

    def exists(self, path, **kwargs):
        """Check if a file or a directory exists.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return If a file or directory exists, it returns True, otherwise False
        """
        context: FilesetContext = self._get_fileset_context(path)
        return context.get_fs().exists(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            )
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
        src_context: FilesetContext = self._get_fileset_context(src_path)
        if self._check_mount_single_file(
            src_context.get_fileset(),
            src_context.get_fs(),
            src_context.get_storage_type(),
        ):
            raise GravitinoRuntimeException(
                f"Cannot cp file of the fileset: {src_identifier} which only mounts to a single file."
            )
        dst_context: FilesetContext = self._get_fileset_context(dst_path)

        src_context.get_fs().cp_file(
            self._strip_storage_protocol(
                src_context.get_storage_type(), src_context.get_actual_path()
            ),
            self._strip_storage_protocol(
                dst_context.get_storage_type(), dst_context.get_actual_path()
            ),
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
        src_context: FilesetContext = self._get_fileset_context(src_path)
        if self._check_mount_single_file(
            src_context.get_fileset(),
            src_context.get_fs(),
            src_context.get_storage_type(),
        ):
            raise GravitinoRuntimeException(
                f"Cannot cp file of the fileset: {src_identifier} which only mounts to a single file."
            )
        dst_context: FilesetContext = self._get_fileset_context(dst_path)
        if src_context.get_storage_type() == StorageType.HDFS:
            src_context.get_fs().mv(
                self._strip_storage_protocol(
                    src_context.get_storage_type(), src_context.get_actual_path()
                ),
                self._strip_storage_protocol(
                    dst_context.get_storage_type(), dst_context.get_actual_path()
                ),
            )
        elif src_context.get_storage_type() == StorageType.LOCAL:
            src_context.get_fs().mv(
                self._strip_storage_protocol(
                    src_context.get_storage_type(), src_context.get_actual_path()
                ),
                self._strip_storage_protocol(
                    dst_context.get_storage_type(), dst_context.get_actual_path()
                ),
                recursive,
                maxdepth,
            )
        else:
            raise GravitinoRuntimeException(
                f"Storage type:{src_context.get_storage_type()} doesn't support now."
            )

    def _rm(self, path):
        raise GravitinoRuntimeException(
            "Deprecated method, use `rm_file` method instead."
        )

    def rm(self, path, recursive=False, maxdepth=None):
        """Remove a file or directory.
        :param path: Virtual fileset path
        :param recursive: Whether to remove the directory recursively.
                When removing a directory, this parameter should be True.
        :param maxdepth: The maximum depth to remove the directory recursively.
        """
        context: FilesetContext = self._get_fileset_context(path)
        context.get_fs().rm(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            ),
            recursive,
            maxdepth,
        )

    def rm_file(self, path):
        """Remove a file.
        :param path: Virtual fileset path
        """
        context: FilesetContext = self._get_fileset_context(path)
        context.get_fs().rm_file(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            )
        )

    def rmdir(self, path):
        """Remove a directory.
        It will delete a directory and all its contents recursively for PyArrow.HadoopFileSystem.
        And it will throw an exception if delete a directory which is non-empty for LocalFileSystem.
        :param path: Virtual fileset path
        """
        context: FilesetContext = self._get_fileset_context(path)
        context.get_fs().rmdir(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            )
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
        context: FilesetContext = self._get_fileset_context(path)
        return context.get_fs().open(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            ),
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
        context: FilesetContext = self._get_fileset_context(path)
        context.get_fs().mkdir(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            ),
            create_parents,
            **kwargs,
        )

    def makedirs(self, path, exist_ok=True):
        """Make a directory recursively.
        :param path: Virtual fileset path
        :param exist_ok: Continue if a directory already exists
        """
        context: FilesetContext = self._get_fileset_context(path)
        context.get_fs().makedirs(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            ),
            exist_ok,
        )

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime
        Only supports for `fsspec.LocalFileSystem` now.
        :param path: Virtual fileset path
        :return Created time(datetime.datetime)
        """
        context: FilesetContext = self._get_fileset_context(path)
        if context.get_storage_type() == StorageType.LOCAL:
            return context.get_fs().created(
                self._strip_storage_protocol(
                    context.get_storage_type(), context.get_actual_path()
                )
            )
        raise GravitinoRuntimeException(
            f"Storage type:{context.get_storage_type()} doesn't support now."
        )

    def modified(self, path):
        """Returns the modified time of the path file if it exists.
        :param path: Virtual fileset path
        :return Modified time(datetime.datetime)
        """
        context: FilesetContext = self._get_fileset_context(path)
        return context.get_fs().modified(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            )
        )

    def cat_file(self, path, start=None, end=None, **kwargs):
        """Get the content of a file.
        :param path: Virtual fileset path
        :param start: The offset in bytes to start reading from. It can be None.
        :param end: The offset in bytes to end reading at. It can be None.
        :param kwargs: Extra args
        :return File content
        """
        context: FilesetContext = self._get_fileset_context(path)
        return context.get_fs().cat_file(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            ),
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
        context: FilesetContext = self._get_fileset_context(rpath)
        context.get_fs().get_file(
            self._strip_storage_protocol(
                context.get_storage_type(), context.get_actual_path()
            ),
            lpath,
            **kwargs,
        )

    def _convert_actual_path(self, path, context: FilesetContext):
        """Convert an actual path to a virtual path.
          The virtual path is like `fileset/{catalog}/{schema}/{fileset}/xxx`.
        :param path: Actual path
        :param context: Fileset context
        :return A virtual path
        """
        if context.get_storage_type() == StorageType.HDFS:
            actual_prefix = infer_storage_options(
                context.get_fileset().storage_location()
            )["path"]
        elif context.get_storage_type() == StorageType.LOCAL:
            actual_prefix = context.get_fileset().storage_location()[
                len(f"{StorageType.LOCAL.value}:") :
            ]
        else:
            raise GravitinoRuntimeException(
                f"Storage type:{context.get_storage_type()} doesn't support now."
            )

        if not path.startswith(actual_prefix):
            raise GravitinoRuntimeException(
                f"Path {path} does not start with valid prefix {actual_prefix}."
            )
        virtual_location = self._get_virtual_location(context.get_name_identifier())
        # if the storage location is end with "/",
        # we should truncate this to avoid replace issues.
        if actual_prefix.endswith(self.SLASH) and not virtual_location.endswith(
            self.SLASH
        ):
            return f"{path.replace(actual_prefix[:-1], virtual_location)}"
        return f"{path.replace(actual_prefix, virtual_location)}"

    def _convert_actual_info(self, entry: Dict, context: FilesetContext):
        """Convert a file info from an actual entry to a virtual entry.
        :param entry: A dict of the actual file info
        :param context: Fileset context
        :return A dict of the virtual file info
        """
        path = self._convert_actual_path(entry["name"], context)
        return {
            "name": path,
            "size": entry["size"],
            "type": entry["type"],
            "mtime": entry["mtime"],
        }

    def _get_fileset_context(self, virtual_path: str):
        """Get a fileset context from the cache or the Gravitino server
        :param virtual_path: The virtual path
        :return A fileset context
        """
        virtual_path: str = self._pre_process_path(virtual_path)
        identifier: NameIdentifier = self._extract_identifier(virtual_path)
        read_lock = self._cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[Fileset, AbstractFileSystem, StorageType] = (
                self._cache.get(identifier)
            )
            if cache_value is not None:
                actual_path = self._get_actual_path_by_ident(
                    identifier,
                    cache_value[0],
                    cache_value[1],
                    cache_value[2],
                    virtual_path,
                )
                return FilesetContext(
                    identifier,
                    cache_value[0],
                    cache_value[1],
                    cache_value[2],
                    actual_path,
                )
        finally:
            read_lock.release()

        write_lock = self._cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: Tuple[Fileset, AbstractFileSystem] = self._cache.get(
                identifier
            )
            if cache_value is not None:
                actual_path = self._get_actual_path_by_ident(
                    identifier,
                    cache_value[0],
                    cache_value[1],
                    cache_value[2],
                    virtual_path,
                )
                return FilesetContext(
                    identifier,
                    cache_value[0],
                    cache_value[1],
                    cache_value[2],
                    actual_path,
                )
            fileset: Fileset = self._load_fileset_from_server(identifier)
            storage_location = fileset.storage_location()
            if storage_location.startswith(f"{StorageType.HDFS.value}://"):
                fs = ArrowFSWrapper(HadoopFileSystem.from_uri(storage_location))
                storage_type = StorageType.HDFS
            elif storage_location.startswith(f"{StorageType.LOCAL.value}:/"):
                fs = LocalFileSystem()
                storage_type = StorageType.LOCAL
            else:
                raise GravitinoRuntimeException(
                    f"Storage under the fileset: `{identifier}` doesn't support now."
                )
            actual_path = self._get_actual_path_by_ident(
                identifier, fileset, fs, storage_type, virtual_path
            )
            self._cache[identifier] = (fileset, fs, storage_type)
            context = FilesetContext(identifier, fileset, fs, storage_type, actual_path)
            return context
        finally:
            write_lock.release()

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

    def _load_fileset_from_server(self, identifier: NameIdentifier) -> Fileset:
        """Load the fileset from the server.
        If the fileset is not found on the server, an `NoSuchFilesetException` exception will be raised.
        :param identifier: The fileset identifier
        :return The fileset
        """
        catalog: Catalog = self._client.load_catalog(identifier.namespace().level(1))

        return catalog.as_fileset_catalog().load_fileset(
            NameIdentifier.of(identifier.namespace().level(2), identifier.name())
        )

    def _get_actual_path_by_ident(
        self,
        identifier: NameIdentifier,
        fileset: Fileset,
        fs: AbstractFileSystem,
        storage_type: StorageType,
        virtual_path: str,
    ):
        """Get the actual path by the virtual path and the fileset.
        :param identifier: The fileset identifier
        :param fileset: The fileset
        :param fs: The file system corresponding to the fileset storage location
        :param storage_type: The storage type of the fileset storage location
        :param virtual_path: The virtual fileset path
        :return The actual path.
        """
        virtual_location = self._get_virtual_location(identifier)
        storage_location = fileset.storage_location()
        if self._check_mount_single_file(fileset, fs, storage_type):
            if virtual_path != virtual_location:
                raise GravitinoRuntimeException(
                    f"Path: {virtual_path} should be same with the virtual location: {virtual_location}"
                    " when the fileset only mounts a single file."
                )
            return storage_location
        # if the storage location ends with "/",
        # we should handle the conversion specially
        if storage_location.endswith(self.SLASH):
            sub_path = virtual_path[len(virtual_location) :]
            # For example, if the virtual path is `gvfs://fileset/catalog/schema/test_fileset/ttt`,
            # and the storage location is `hdfs://cluster:8020/user/`,
            # we should replace `gvfs://fileset/catalog/schema/test_fileset`
            # with `hdfs://localhost:8020/user` which truncates the tailing slash.
            # If the storage location is `hdfs://cluster:8020/user`,
            # we can replace `gvfs://fileset/catalog/schema/test_fileset`
            # with `hdfs://localhost:8020/user` directly.
            if sub_path.startswith(self.SLASH):
                new_storage_location = storage_location[:-1]
            else:
                new_storage_location = storage_location

            # Replace virtual_location with the adjusted storage_location
            return virtual_path.replace(virtual_location, new_storage_location, 1)
        return virtual_path.replace(virtual_location, storage_location, 1)

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

    def _check_mount_single_file(
        self, fileset: Fileset, fs: AbstractFileSystem, storage_type: StorageType
    ):
        """Check if the fileset is mounted a single file.
        :param fileset: The fileset
        :param fs: The file system corresponding to the fileset storage location
        :param storage_type: The storage type of the fileset storage location
        :return True the fileset is mounted a single file.
        """
        result: Dict = fs.info(
            self._strip_storage_protocol(storage_type, fileset.storage_location())
        )
        return result["type"] == "file"

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
        """
        if storage_type == StorageType.HDFS:
            return path
        if storage_type == StorageType.LOCAL:
            return path[len(f"{StorageType.LOCAL.value}:") :]
        raise GravitinoRuntimeException(
            f"Storage type:{storage_type} doesn't support now."
        )


fsspec.register_implementation(PROTOCOL_NAME, GravitinoVirtualFileSystem)
