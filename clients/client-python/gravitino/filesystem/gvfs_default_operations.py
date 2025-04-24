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

from gravitino.name_identifier import NameIdentifier
from gravitino.audit.fileset_data_operation import FilesetDataOperation
from gravitino.exceptions.base import GravitinoRuntimeException
from gravitino.filesystem.gvfs_base_operations import BaseGVFSOperations
from gravitino.filesystem.gvfs_storage_handler import (
    StorageType,
    get_storage_handler_by_path,
)
from gravitino.filesystem.gvfs_utils import (
    extract_identifier,
    get_sub_path_from_virtual_path,
    to_gvfs_path_prefix,
)


class DefaultGVFSOperations(BaseGVFSOperations):
    """
    Default implementation of the Gravitino Virtual File System operations.
    """

    def ls(self, path, detail=True, **kwargs):
        """List the files and directories info of the path.
        :param path: Virtual fileset path
        :param detail: Whether to show the details for the files and directories info
        :param kwargs: Extra args
        :return If details is true, returns a list of file info dicts, else returns a list of file paths
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.LIST_STATUS
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        pre_process_path: str = self._pre_process_path(path)
        identifier: NameIdentifier = extract_identifier(
            self._metalake, pre_process_path
        )
        sub_path: str = get_sub_path_from_virtual_path(identifier, pre_process_path)
        fileset_location: str = actual_file_path[
            : len(actual_file_path) - len(sub_path)
        ]

        paths_or_entries = actual_fs.ls(
            storage_handler.strip_storage_protocol(actual_file_path),
            detail=detail,
        )
        # return entries with details
        if detail:
            virtual_entries = [
                storage_handler.actual_info_to_gvfs_info(
                    entry, fileset_location, to_gvfs_path_prefix(identifier)
                )
                for entry in paths_or_entries
            ]
            return virtual_entries

        # only returns paths
        virtual_entry_paths = [
            storage_handler.actual_path_to_gvfs_path(
                entry_path, fileset_location, to_gvfs_path_prefix(identifier)
            )
            for entry_path in paths_or_entries
        ]
        return virtual_entry_paths

    def info(self, path, **kwargs):
        """Get file info.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return A file info dict
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.GET_FILE_STATUS
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        pre_process_path: str = self._pre_process_path(path)
        identifier: NameIdentifier = extract_identifier(
            self._metalake, pre_process_path
        )
        sub_path: str = get_sub_path_from_virtual_path(identifier, pre_process_path)
        fileset_location: str = actual_file_path[
            : len(actual_file_path) - len(sub_path)
        ]

        entry_info = actual_fs.info(
            storage_handler.strip_storage_protocol(actual_file_path)
        )
        return storage_handler.actual_info_to_gvfs_info(
            entry_info, fileset_location, to_gvfs_path_prefix(identifier)
        )

    def exists(self, path, **kwargs):
        """Check if a file or a directory exists.
        :param path: Virtual fileset path
        :param kwargs: Extra args
        :return If a file or directory exists, it returns True, otherwise False
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.EXISTS
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        return actual_fs.exists(
            storage_handler.strip_storage_protocol(actual_file_path)
        )

    def cp_file(self, path1, path2, **kwargs):
        """Copy a file.
        :param path1: Virtual src fileset path
        :param path2: Virtual dst fileset path, should be consistent with the src path fileset identifier
        :param kwargs: Extra args
        """
        self._assert_same_fileset(path1, path2)
        src_path = self._pre_process_path(path1)
        dst_path = self._pre_process_path(path2)

        actual_fs = self._get_actual_filesystem(src_path, self.current_location_name)
        actual_src_file_path = self._get_actual_file_path(
            src_path, self.current_location_name, FilesetDataOperation.COPY_FILE
        )
        actual_dst_file_path = self._get_actual_file_path(
            dst_path, self.current_location_name, FilesetDataOperation.COPY_FILE
        )
        storage_handler = get_storage_handler_by_path(actual_src_file_path)

        actual_fs.cp_file(
            storage_handler.strip_storage_protocol(actual_src_file_path),
            storage_handler.strip_storage_protocol(actual_dst_file_path),
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
        self._assert_same_fileset(path1, path2)
        src_path = self._pre_process_path(path1)
        dst_path = self._pre_process_path(path2)

        actual_fs = self._get_actual_filesystem(src_path, self.current_location_name)
        src_actual_path = self._get_actual_file_path(
            src_path, self.current_location_name, FilesetDataOperation.RENAME
        )
        dst_actual_path = self._get_actual_file_path(
            dst_path, self.current_location_name, FilesetDataOperation.RENAME
        )
        storage_handler = get_storage_handler_by_path(src_actual_path)

        if storage_handler.storage_type() == StorageType.LOCAL:
            actual_fs.mv(
                storage_handler.strip_storage_protocol(src_actual_path),
                storage_handler.strip_storage_protocol(dst_actual_path),
                recursive,
                maxdepth,
            )
        else:
            actual_fs.mv(
                storage_handler.strip_storage_protocol(src_actual_path),
                storage_handler.strip_storage_protocol(dst_actual_path),
            )

    def rm(self, path, recursive=False, maxdepth=None):
        """Remove a file or directory.
        :param path: Virtual fileset path
        :param recursive: Whether to remove the directory recursively.
                When removing a directory, this parameter should be True.
        :param maxdepth: The maximum depth to remove the directory recursively.
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.DELETE
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        # S3FileSystem and ocifs don't support maxdepth
        if storage_handler.storage_type() in [StorageType.S3A]:
            actual_fs.rm(
                storage_handler.strip_storage_protocol(actual_file_path),
                recursive,
            )
        else:
            actual_fs.rm(
                storage_handler.strip_storage_protocol(actual_file_path),
                recursive,
                maxdepth,
            )

    def rm_file(self, path):
        """Remove a file.
        :param path: Virtual fileset path
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.DELETE
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        actual_fs.rm_file(storage_handler.strip_storage_protocol(actual_file_path))

    def rmdir(self, path):
        """Remove a directory.
        It will delete a directory and all its contents recursively for PyArrow.HadoopFileSystem.
        And it will throw an exception if delete a directory which is non-empty for LocalFileSystem.
        :param path: Virtual fileset path
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.DELETE
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        actual_fs.rmdir(storage_handler.strip_storage_protocol(actual_file_path))

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

        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, data_operation
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        return actual_fs.open(
            storage_handler.strip_storage_protocol(actual_file_path),
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
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.MKDIRS
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        actual_fs.mkdir(
            storage_handler.strip_storage_protocol(actual_file_path),
            create_parents,
            **kwargs,
        )

    def makedirs(self, path, exist_ok=True):
        """Make a directory recursively.
        :param path: Virtual fileset path
        :param exist_ok: Continue if a directory already exists
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.MKDIRS
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        actual_fs.makedirs(
            storage_handler.strip_storage_protocol(actual_file_path),
            exist_ok,
        )

    def created(self, path):
        """Return the created timestamp of a file as a datetime.datetime
        Only supports for `fsspec.LocalFileSystem` now.
        :param path: Virtual fileset path
        :return Created time(datetime.datetime)
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.CREATED_TIME
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)
        if storage_handler.storage_type() != StorageType.LOCAL:
            raise GravitinoRuntimeException(
                f"Storage type:{storage_handler.storage_type()} doesn't support now."
            )

        created_time = actual_fs.created(
            storage_handler.strip_storage_protocol(actual_file_path)
        )
        return created_time

    def modified(self, path):
        """Returns the modified time of the path file if it exists.
        :param path: Virtual fileset path
        :return Modified time(datetime.datetime)
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.MODIFIED_TIME
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        return actual_fs.modified(
            storage_handler.strip_storage_protocol(actual_file_path)
        )

    def cat_file(self, path, start=None, end=None, **kwargs):
        """Get the content of a file.
        :param path: Virtual fileset path
        :param start: The offset in bytes to start reading from. It can be None.
        :param end: The offset in bytes to end reading at. It can be None.
        :param kwargs: Extra args
        :return File content
        """
        actual_fs = self._get_actual_filesystem(path, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            path, self.current_location_name, FilesetDataOperation.GET_FILE_STATUS
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        return actual_fs.cat_file(
            storage_handler.strip_storage_protocol(actual_file_path),
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

        actual_fs = self._get_actual_filesystem(rpath, self.current_location_name)
        actual_file_path = self._get_actual_file_path(
            rpath, self.current_location_name, FilesetDataOperation.GET_FILE
        )
        storage_handler = get_storage_handler_by_path(actual_file_path)

        actual_fs.get_file(
            storage_handler.strip_storage_protocol(actual_file_path),
            lpath,
            **kwargs,
        )
