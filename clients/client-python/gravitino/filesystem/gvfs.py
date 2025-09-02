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
import functools
import importlib
import logging
import re
from typing import Dict, Optional, Callable

import fsspec

from gravitino.audit.fileset_data_operation import FilesetDataOperation
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
    NoSuchCatalogException,
    CatalogNotInUseException,
    NoSuchFilesetException,
    NoSuchLocationNameException,
)
from gravitino.filesystem.gvfs_base_operations import (
    BaseGVFSOperations,
    FilesetPathNotFoundError,
)
from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.filesystem.gvfs_default_operations import DefaultGVFSOperations
from gravitino.filesystem.gvfs_hook import DEFAULT_HOOK, GravitinoVirtualFileSystemHook

logger = logging.getLogger(__name__)

PROTOCOL_NAME = "gvfs"


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
        self._kwargs = dict(kwargs)
        self._hook = self._get_hook_class(options)
        self._hook.initialize(options)
        self._operations = self._get_gvfs_operations_class(
            server_uri, metalake_name, options
        )

        super().__init__(**kwargs)

    @property
    def fsid(self):
        return PROTOCOL_NAME

    @property
    def hook(self):
        return self._hook

    @property
    def operations(self):
        return self._operations

    @staticmethod
    def _with_exception_translation(operation: FilesetDataOperation):
        """
        Decorator to translate fileset metadata not found exceptions into FilesetPathNotFoundError.
        :param operation: The operation being performed.
        """

        def decorator(func: Callable):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except (NoSuchCatalogException, CatalogNotInUseException) as e:
                    message = f"Cannot get fileset catalog during {operation}"
                    logger.warning("%s, %s", message, str(e))
                    raise FilesetPathNotFoundError(message) from e
                except NoSuchFilesetException as e:
                    message = f"Cannot get fileset during {operation}"
                    logger.warning("%s, %s", message, str(e))
                    raise FilesetPathNotFoundError(message) from e
                except NoSuchLocationNameException as e:
                    message = f"Cannot find location name during {operation}"
                    logger.warning("%s, %s", message, str(e))
                    raise FilesetPathNotFoundError(message) from e

            return wrapper

        return decorator

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
        new_path = self._hook.pre_ls(path, detail, **kwargs)
        decorated_ls = self._with_exception_translation(
            FilesetDataOperation.LIST_STATUS
        )(self._operations.ls)
        result = decorated_ls(new_path, detail, **kwargs)
        return self._hook.post_ls(detail, result, **kwargs)

    def info(self, path, **kwargs):
        """Get file info.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return A file info dict
        """
        new_path = self._hook.pre_info(path, **kwargs)
        decorated_info = self._with_exception_translation(
            FilesetDataOperation.GET_FILE_STATUS
        )(self._operations.info)
        result = decorated_info(new_path, **kwargs)
        return self._hook.post_info(
            result,
            **kwargs,
        )

    def exists(self, path, **kwargs):
        """Check if a file or a directory exists.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return If a file or directory exists, it returns True, otherwise False
        """
        new_path = self._hook.pre_exists(path, **kwargs)
        decorated_exists = self._with_exception_translation(
            FilesetDataOperation.EXISTS
        )(self._operations.exists)
        try:
            result = decorated_exists(new_path, **kwargs)
        except FilesetPathNotFoundError:
            return False

        return self._hook.post_exists(
            new_path,
            result,
            **kwargs,
        )

    def cp_file(self, path1, path2, **kwargs):
        """Copy a file.
        :param path1: Virtual src fileset path
        :param path2: Virtual dst fileset path, should be consistent with the src path fileset identifier
        :param kwargs: Extra args
        """
        new_path1, new_path2 = self._hook.pre_cp_file(path1, path2, **kwargs)
        decorated_cp_file = self._with_exception_translation(
            FilesetDataOperation.COPY_FILE
        )(self._operations.cp_file)
        decorated_cp_file(new_path1, new_path2, **kwargs)
        self._hook.post_cp_file(new_path1, new_path2, **kwargs)

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
        new_path1, new_path2 = self._hook.pre_mv(
            path1, path2, recursive, maxdepth, **kwargs
        )
        decorated_mv = self._with_exception_translation(FilesetDataOperation.RENAME)(
            self._operations.mv
        )
        decorated_mv(new_path1, new_path2, recursive, maxdepth, **kwargs)
        self._hook.post_mv(new_path1, new_path2, recursive, maxdepth, **kwargs)

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
        new_path = self._hook.pre_rm(path, recursive, maxdepth)
        decorated_rm = self._with_exception_translation(FilesetDataOperation.DELETE)(
            self._operations.rm
        )
        decorated_rm(new_path, recursive, maxdepth)
        self._hook.post_rm(new_path, recursive, maxdepth)

    def rm_file(self, path):
        """Remove a file.
        :param path: Virtual fileset path
        """
        new_path = self._hook.pre_rm_file(path)
        decorated_rm_file = self._with_exception_translation(
            FilesetDataOperation.DELETE
        )(self._operations.rm_file)
        decorated_rm_file(new_path)
        self._hook.post_rm_file(new_path)

    def rmdir(self, path):
        """Remove a directory.
        It will delete a directory and all its contents recursively for PyArrow.HadoopFileSystem.
        And it will throw an exception if delete a directory which is non-empty for LocalFileSystem.
        :param path: Virtual fileset path
        """
        new_path = self._hook.pre_rmdir(path)
        decorated_rmdir = self._with_exception_translation(FilesetDataOperation.DELETE)(
            self._operations.rmdir
        )
        decorated_rmdir(new_path)
        self._hook.post_rmdir(new_path)

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
        new_path = self._hook.pre_open(
            path, mode, block_size, cache_options, compression, **kwargs
        )
        if mode in ("w", "wb"):
            data_operation = FilesetDataOperation.OPEN_AND_WRITE
        elif mode in ("a", "ab"):
            data_operation = FilesetDataOperation.OPEN_AND_APPEND
        else:
            data_operation = FilesetDataOperation.OPEN

        decorated_open = self._with_exception_translation(data_operation)(
            self._operations.open
        )
        try:
            result = decorated_open(
                new_path,
                mode,
                block_size,
                cache_options,
                compression,
                **kwargs,
            )
        except FilesetPathNotFoundError as e:
            if mode in ("w", "wb", "x", "xb", "a", "ab"):
                raise OSError(
                    f"Fileset is not found for path: {new_path} for operation OPEN. This "
                    f"may be caused by fileset related metadata not found or not in use "
                    f"in Gravitino,"
                ) from e
            raise
        return self._hook.post_open(
            new_path,
            mode,
            block_size,
            cache_options,
            compression,
            result,
            **kwargs,
        )

    def mkdir(self, path, create_parents=True, **kwargs):
        """Make a directory.
        if create_parents=True, this is equivalent to ``makedirs``.

        :param path: Virtual fileset path
        :param create_parents: Create parent directories if missing when set to True
        :param kwargs: Extra args
        """
        new_path = self._hook.pre_mkdir(path, create_parents, **kwargs)
        decorated_mkdir = self._with_exception_translation(FilesetDataOperation.MKDIRS)(
            self._operations.mkdir
        )
        try:
            decorated_mkdir(new_path, create_parents, **kwargs)
        except FilesetPathNotFoundError as e:
            raise OSError(
                f"Fileset is not found for path: {new_path} for operation MKDIRS. This "
                f"may be caused by fileset related metadata not found or not in use "
                f"in Gravitino,"
            ) from e
        self._hook.post_mkdir(new_path, create_parents, **kwargs)

    def makedirs(self, path, exist_ok=True):
        """Make a directory recursively.
        :param path: Virtual fileset path
        :param exist_ok: Continue if a directory already exists
        """
        new_path = self._hook.pre_makedirs(path, exist_ok)
        decorated_makedirs = self._with_exception_translation(
            FilesetDataOperation.MKDIRS
        )(self._operations.makedirs)
        try:
            decorated_makedirs(new_path, exist_ok)
        except FilesetPathNotFoundError as e:
            raise OSError(
                f"Fileset is not found for path: {new_path} for operation MKDIRS. This "
                f"may be caused by fileset related metadata not found or not in use "
                f"in Gravitino,"
            ) from e
        self._hook.post_makedirs(new_path, exist_ok)

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime
        Only supports for `fsspec.LocalFileSystem` now.
        :param path: Virtual fileset path
        :return Created time(datetime.datetime)
        """
        new_path = self._hook.pre_created(path)
        decorated_created = self._with_exception_translation(
            FilesetDataOperation.CREATED_TIME
        )(self._operations.created)
        result = decorated_created(new_path)
        return self._hook.post_created(
            new_path,
            result,
        )

    def modified(self, path):
        """Returns the modified time of the path file if it exists.
        :param path: Virtual fileset path
        :return Modified time(datetime.datetime)
        """
        new_path = self._hook.pre_modified(path)
        decorated_modified = self._with_exception_translation(
            FilesetDataOperation.MODIFIED_TIME
        )(self._operations.modified)
        result = decorated_modified(new_path)
        return self._hook.post_modified(
            new_path,
            result,
        )

    def cat_file(self, path, start=None, end=None, **kwargs):
        """Get the content of a file.
        :param path: Virtual fileset path
        :param start: The offset in bytes to start reading from. It can be None.
        :param end: The offset in bytes to end reading at. It can be None.
        :param kwargs: Extra args
        :return File content
        """
        new_path = self._hook.pre_cat_file(path, start, end, **kwargs)
        decorated_cat_file = self._with_exception_translation(
            FilesetDataOperation.CAT_FILE
        )(self._operations.cat_file)
        result = decorated_cat_file(new_path, start, end, **kwargs)
        return self._hook.post_cat_file(
            new_path,
            start,
            end,
            result,
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
        new_rpath = self._hook.pre_get_file(rpath, lpath, callback, outfile, **kwargs)
        decorated_get_file = self._with_exception_translation(
            FilesetDataOperation.GET_FILE_STATUS
        )(self._operations.get_file)
        decorated_get_file(
            new_rpath,
            lpath,
            callback,
            outfile,
            **kwargs,
        )
        self._hook.post_get_file(new_rpath, lpath, outfile, **kwargs)

    def _get_hook_class(
        self, options: Optional[Dict[str, str]]
    ) -> GravitinoVirtualFileSystemHook:
        hook_class = (
            None if options is None else options.get(GVFSConfig.GVFS_FILESYSTEM_HOOK)
        )

        if hook_class is not None:
            module_name, class_name = hook_class.rsplit(".", 1)
            module = importlib.import_module(module_name)
            loaded_class = getattr(module, class_name)
            return loaded_class()

        return DEFAULT_HOOK

    def _get_gvfs_operations_class(
        self,
        server_uri: str = None,
        metalake_name: str = None,
        options: Dict = None,
    ) -> BaseGVFSOperations:
        operations_class = (
            None
            if options is None
            else options.get(GVFSConfig.GVFS_FILESYSTEM_OPERATIONS)
        )
        if operations_class is not None:
            module_name, class_name = operations_class.rsplit(".", 1)
            module = importlib.import_module(module_name)
            loaded_class = getattr(module, class_name)
            return loaded_class(server_uri, metalake_name, options)

        return DefaultGVFSOperations(
            server_uri=server_uri,
            metalake_name=metalake_name,
            options=options,
            **self._kwargs,
        )


fsspec.register_implementation(PROTOCOL_NAME, GravitinoVirtualFileSystem)
