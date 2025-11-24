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
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple, List, Any, Union

from fsspec import Callback


class GravitinoVirtualFileSystemHook(ABC):
    """
    Represents a hook that can be used to intercept file system operations. The implementor
    should handle the exception, if any, in the pre-hook method, otherwise the exception will be
    thrown to the caller and fail the operation. Besides, the implemented pre-hook method should
    be lightweight and fast, otherwise it will slow down the operation. The pre-hook method may
    be called more than once and in parallel, so the implementor should handle the concurrent and
    idempotent issues if required.
    """

    def set_operations_context(self, operations):
        """
        Set the operations context for this hook. This method will be called during GVFS initialization
        to provide the hook with access to the BaseGVFSOperations instance.

        Args:
            operations: The BaseGVFSOperations instance.
        """
        # Default implementation does nothing - hooks can override if they need operations access
        pass

    @abstractmethod
    def initialize(self, config: Optional[Dict[str, str]]):
        """
        Initialize the hook with the configuration. This method will be called in the GVFS initialize
        method, and the configuration will be passed from the GVFS configuration. The implementor
        can initialize the hook with the configuration. The exception will be thrown to the caller
        and fail the GVFS initialization.

        Args:
            config: The configuration.
        """
        pass

    @abstractmethod
    def pre_ls(self, path: str, detail: bool, **kwargs) -> str:
        """
        Called before a directory is listed. The returned path will be used for the ls operation.
        The implementor can modify the path for customization. The exception will be thrown to the
        caller and fail the ls operation.

        Args:
            path: The path of the directory.
            detail: Whether to list the directory in detail.
            **kwargs: Additional arguments.

        Returns:
            The path to list.
        """

    @abstractmethod
    def post_ls(
        self, detail: bool, entries: List[Union[str, Dict[str, Any]]], **kwargs
    ) -> List[Union[str, Dict[str, Any]]]:
        """
        Called after a directory is listed. The implementor can modify the entries for
        customization. The exception will be thrown to the caller and fail the ls operation.

        Args:
            detail: Whether to list the directory in detail.
            entries: The entries of the directory. It can be a list of str if the detail is False,
                or a list of dict if the detail is True.
            **kwargs: Additional arguments.

        Returns:
            The entries to list.
        """

    @abstractmethod
    def pre_info(self, path: str, **kwargs) -> str:
        """
        Called before the information of a file is retrieved. The returned path will be used for
        the info operation. The implementor can modify the path for customization. The exception
        will be thrown to the caller and fail the info operation.

        Args:
            path: The path of the file.
            **kwargs: Additional arguments.

        Returns:
            The path to get the information.
        """

    @abstractmethod
    def post_info(self, info: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        """
        Called after the information of a file is retrieved. The implementor can modify the info for
        customization. The exception will be thrown to the caller and fail the info operation.

        Args:
            info: The information of the file.
            **kwargs: Additional arguments.

        Returns:
            The information to get.
        """

    @abstractmethod
    def pre_exists(self, path: str, **kwargs) -> str:
        """
        Called before the existence of a file or a directory is checked. The returned path will be
        used for the exists operation. The implementor can modify the path for customization. The
        exception will be thrown to the caller and fail the exists operation.

        Args:
            path: The path of the file or the directory.
            **kwargs: Additional arguments.

        Returns:
            The path to check the existence.
        """

    @abstractmethod
    def post_exists(self, gvfs_path: str, exists: bool, **kwargs) -> bool:
        """
        Called after the existence of a file or a directory is checked. The implementor can modify the
        value for customization. The exception will be thrown to the caller and fail the exists
        operation.

        Args:
            gvfs_path: The GVFS path of the file or the directory
            exists: The existence of the file or the directory.
            **kwargs: Additional arguments.

        Returns:
            The existence to check.
        """

    @abstractmethod
    def pre_cp_file(self, src: str, dst: str, **kwargs) -> Tuple[str, str]:
        """
        Called before a file is copied. The returned source and destination will be used for the cp
        operation. The implementor can modify the source and destination for customization. The
        exception will be thrown to the caller and fail the cp operation.

        Args:
            src: The source file.
            dst: The destination file.
            **kwargs: Additional arguments.

        Returns:
            The source and destination to copy.
        """

    @abstractmethod
    def post_cp_file(self, src_gvfs_path: str, dst_gvfs_path: str, **kwargs):
        """
        Called after a file is copied. if this method is invoked, it means that the cp_file is
        succeeded. The exception in the method will be thrown to the caller and fail the operation.

        Args:
            src_gvfs_path: The source GVFS path.
            dst_gvfs_path: The destination GVFS path.
            **kwargs: Additional arguments
        """

    @abstractmethod
    def pre_mv(
        self, src: str, dst: str, recursive: bool, maxdepth: int, **kwargs
    ) -> Tuple[str, str]:
        """
        Called before a file or a directory is moved. The returned source and destination will be
        used for the mv operation. The implementor can modify the source and destination for
        customization. The exception will be thrown to the caller and fail the mv operation.

        Args:
            src: The source file or directory.
            dst: The destination file or directory.
            recursive: Whether to move the file or directory recursively.
            maxdepth: The maximum depth to move.
            **kwargs: Additional arguments.

        Returns:
            The source and destination to move.
        """

    @abstractmethod
    def post_mv(
        self,
        src_gvfs_path: str,
        dst_gvfs_path: str,
        recursive: bool,
        maxdepth: int,
        **kwargs
    ):
        """
        Called after a file or a directory is moved. If this method is invoked, it means that the mv
        is succeeded. The exception in the method will be thrown to the caller and fail the operation.

        Args:
            src_gvfs_path: The source GVFS path.
            dst_gvfs_path: The destination GVFS path.
            recursive: Whether to move the file or directory recursively.
            maxdepth: The maximum depth to move.
            **kwargs: Additional arguments
        """

    @abstractmethod
    def pre_rm(self, path: str, recursive: bool, maxdepth: int) -> str:
        """
        Called before a file or a directory is removed. The returned path will be used for the rm
        operation. The implementor can modify the path for customization. The exception will be
        thrown to the caller and fail the rm operation.

        Args:
            path: The path of the file or the directory.
            recursive: Whether to remove the file or directory recursively.
            maxdepth: The maximum depth to remove.

        Returns:
            The path to remove.
        """

    @abstractmethod
    def post_rm(self, gvfs_path: str, recursive: bool, maxdepth: int):
        """
        Called after a file or a directory is removed. If this method is invoked, it means that the
        rm is succeeded. The exception will be thrown to the caller and fail the rm operation.

        Args:
            gvfs_path: The GVFS path of the file or the directory.
            recursive: Whether to remove the file or directory recursively.
            maxdepth: The maximum depth to remove.
        """

    @abstractmethod
    def pre_rm_file(self, path: str) -> str:
        """
        Called before a file is removed. The returned path will be used for the rm_file operation.
        The implementor can modify the path for customization. The exception will be thrown to the
        caller and fail the rm_file operation.

        Args:
            path: The path of the file.

        Returns:
            The path to remove.
        """

    @abstractmethod
    def post_rm_file(self, gvfs_path: str):
        """
        Called after a file is removed. If this method is invoked, it means that the rm_file is
        succeeded. The exception will be thrown to the caller and fail the rm_file operation.

        Args:
            gvfs_path: The GVFS path of the file.
        """

    @abstractmethod
    def pre_rmdir(self, path: str) -> str:
        """
        Called before a directory is removed. The returned path will be used for the rmdir operation.
        The implementor can modify the path for customization. The exception will be thrown to the
        caller and fail the rmdir operation.

        Args:
            path: The path of the directory.

        Returns:
            The path to remove.
        """

    def post_rmdir(self, gvfs_path: str):
        """
        Called after a directory is removed. If this method is invoked, it means that the rmdir
        is succeeded. The exception will be thrown to the caller and fail the rmdir operation.

        Args:
            gvfs_path: The GVFS path of the directory.
        """

    @abstractmethod
    def pre_open(
        self,
        path: str,
        mode: str,
        block_size: int,
        cache_options: dict,
        compression: str,
        **kwargs
    ) -> str:
        """
        Called before a file is opened. The returned path will be used for the open operation. The
        implementor can modify the path for customization. The exception will be thrown to the caller
        and fail the open operation.

        Args:
            path: The path of the file.
            mode: The mode to open the file.
            block_size: The block size of the file.
            cache_options: The cache options of the file.
            compression: The compression of the file.
            **kwargs: Additional arguments.

        Returns:
            The path to open.
        """

    @abstractmethod
    def post_open(
        self,
        gvfs_path: str,
        mode: str,
        block_size: int,
        cache_options: dict,
        compression: str,
        file: Any,
        **kwargs
    ) -> Any:
        """
        Called after a file is opened. The implementor can modify the file object for
        customization. The exception will be thrown to the caller and fail the open operation.

        Args:
            gvfs_path: The GVFS path of the file.
            mode: The mode to open the file.
            block_size: The block size of the file.
            cache_options: The cache options of the file.
            compression: The compression of the file.
            file: The file object to open.
            **kwargs: Additional arguments.

        Returns:
            The file to open.
        """

    @abstractmethod
    def pre_mkdir(self, path: str, create_parents: bool, **kwargs) -> str:
        """
        Called before a directory is created. The returned path will be used for the mkdir operation.
        The implementor can modify the path for customization. The exception will be thrown to the
        caller and fail the mkdir operation.

        Args:
            path: The path of the directory.
            create_parents: Whether to create the parent directories.
            **kwargs: Additional arguments.

        Returns:
            The path to mkdir.
        """

    @abstractmethod
    def post_mkdir(self, gvfs_path: str, create_parents: bool, **kwargs):
        """
        Called after a directory is created. If this method is invoked, it means that the mkdir
        is succeeded. The exception will be  thrown to the caller and fail the mkdir operation.

        Args:
            gvfs_path: The GVFS path of the directory.
            create_parents: Whether to create the parent directories.
            **kwargs: Additional arguments.
        """

    @abstractmethod
    def pre_makedirs(self, path: str, exist_ok: bool) -> str:
        """
        Called before a directory is created. The returned path will be used for the makedirs
        operation. The implementor can modify the path for customization. The exception will be
        thrown to the caller and fail the makedirs operation.

        Args:
            path: The path of the directory.
            exist_ok: Whether to exist the directory.

        Returns:
            The path to makedirs.
        """

    def post_makedirs(self, gvfs_path: str, exist_ok: bool):
        """
        Called after a directory is created. If this method is invoked, it means that the
        makedirs is succeeded. The exception will be thrown to the caller and fail the
        makedirs operation.

        Args:
            gvfs_path: The GVFS path of the directory.
            exist_ok: Whether to exist the directory.
        """

    @abstractmethod
    def pre_cat_file(self, path: str, start: int, end: int, **kwargs) -> str:
        """
        Called before a file is read. The returned path will be used for the cat_file operation. The
        implementor can modify the path for customization. The exception will be thrown to the caller
        and fail the cat_file operation.

        Args:
            path: The path of the file.
            start: The start position to read.
            end: The end position to read.
            **kwargs: Additional arguments.

        Returns:
            The path to cat_file.
        """

    @abstractmethod
    def post_cat_file(
        self, gvfs_path: str, start: int, end: int, content: Any, **kwargs
    ) -> Any:
        """
        Called after a file is read. The implementor can modify the content for customization. The
        exception will be thrown to the caller and fail the cat_file operation.

        Args:
            gvfs_path: The GVFS path of the file.
            start: The start position to read.
            end: The end position to read.
            content: The content of the file.
            **kwargs: Additional arguments.

        Returns:
            The content to cat_file.
        """

    @abstractmethod
    def pre_get_file(
        self, rpath: str, lpath: str, callback: Callback, outfile: str, **kwargs
    ) -> str:
        """
        Called before a file is downloaded. The returned path will be used for the get_file operation.
        The implementor can modify the path for customization. The exception will be thrown to the caller
        and fail the get_file operation.

        Args:
            rpath: The remote path of the file.
            lpath: The local path of the file.
            callback: The callback to call.
            outfile: The output file.
            **kwargs: Additional arguments.

        Returns:
            The path to get_file.
        """

    def post_get_file(self, gvfs_path: str, local_path: str, outfile: str, **kwargs):
        """
        Called after a file is downloaded. If this method is invoked, it means that the get_file
        is succeeded. The exception will be thrown to  the caller and fail the get_file operation.

        Args:
            gvfs_path: The GVFS path of the file.
            local_path: The local path of the file.
            outfile: The output file.
            **kwargs: Additional arguments
        """

    @abstractmethod
    def pre_created(self, path: str) -> str:
        """
        Called before the creation time of a file is retrieved. The returned path will be used for
        the created operation. The implementor can modify the path for customization. The exception
        will be thrown to the caller and fail the created operation.

        Args:
            path: The path of the file.

        Returns:
            The path to get the creation time.
        """

    @abstractmethod
    def post_created(self, gvfs_path: str, created: Any) -> Any:
        """
        Called after the creation time of a file is retrieved. The implementor can modify the created
        time for customization. The exception will be thrown to the caller and fail the created operation.

        Args:
            gvfs_path: The GVFS path of the file.
            created: The creation time of the file.

        Returns:
            The creation time to get.
        """

    @abstractmethod
    def pre_modified(self, path: str) -> str:
        """
        Called before the modification time of a file is retrieved. The returned path will be used for
        the modified operation. The implementor can modify the path for customization. The exception
        will be thrown to the caller and fail the modified operation.

        Args:
            path: The path of the file.

        Returns:
            The path to get the modification time.
        """

    @abstractmethod
    def post_modified(self, gvfs_path: str, modified: Any) -> Any:
        """
        Called after the modification time of a file is retrieved. The implementor can modify the modified
        time for customization. The exception will be thrown to the caller and fail the modified operation.

        Args:
            gvfs_path: The GVFS path of the file.
            modified: The modification time of the file.

        Returns:
            The modification time to get.
        """

    def on_ls_failure(self, path: str, exception: Exception, **kwargs):
        """
        Called when listing directory contents fails.

        Args:
            path: The path that failed to list contents for.
            exception: The exception that caused the failure.
            **kwargs: Additional arguments.

        Returns:
            The fallback entries list if available.

        Raises:
            Exception: Re-raises the exception if no fallback is available.
        """
        raise exception

    def on_info_failure(self, path: str, exception: Exception, **kwargs):
        """
        Called when getting file info fails.

        Args:
            path: The path that failed to get info for.
            exception: The exception that caused the failure.
            **kwargs: Additional arguments.

        Returns:
            The fallback file info dict if available.

        Raises:
            Exception: Re-raises the exception if no fallback is available.
        """
        raise exception

    def on_exists_failure(self, path: str, exception: Exception, **kwargs):
        """
        Called when checking file existence fails.

        Args:
            path: The path that failed to check existence for.
            exception: The exception that caused the failure.
            **kwargs: Additional arguments.

        Returns:
            The fallback boolean value if available.

        Raises:
            Exception: Re-raises the exception if no fallback is available.
        """
        raise exception

    def on_open_failure(
        self,
        path: str,
        mode: str,
        block_size: int,
        cache_options: dict,
        compression: str,
        exception: Exception,
        **kwargs
    ):
        """
        Called when opening a file fails.

        Args:
            path: The path of the file that failed to open.
            mode: The mode to open the file.
            block_size: The block size of the file.
            cache_options: The cache options of the file.
            compression: The compression of the file.
            exception: The exception that caused the failure.
            **kwargs: Additional arguments.

        Returns:
            The fallback file object if available.

        Raises:
            Exception: Re-raises the exception if no fallback is available.
        """
        raise exception

    def on_cp_file_failure(self, src: str, dst: str, exception: Exception, **kwargs):
        """
        Called when copying a file fails.

        Args:
            src: The source path that failed to copy.
            dst: The destination path for the copy operation.
            exception: The exception that caused the failure.
            **kwargs: Additional arguments.

        Raises:
            Exception: Can re-raise the exception if no fallback is available.
        """
        raise exception

    def on_mv_failure(
        self,
        src: str,
        dst: str,
        recursive: bool,
        maxdepth: int,
        exception: Exception,
        **kwargs
    ):
        """
        Called when moving a file or directory fails.

        Args:
            src: The source path that failed to be moved.
            dst: The destination path for the move operation.
            recursive: Whether the move was requested to be recursive.
            maxdepth: The maximum depth to move.
            exception: The exception that caused the failure.
            **kwargs: Additional arguments.

        Raises:
            Exception: Can re-raise the exception if no fallback is available.
        """
        raise exception

    def on_rm_failure(
        self, path: str, recursive: bool, maxdepth: int, exception: Exception
    ):
        """
        Called when deleting a file or directory fails.

        Args:
            path: The path that failed to be deleted.
            recursive: Whether the deletion was requested to be recursive.
            maxdepth: The maximum depth to delete.
            exception: The exception that caused the failure.

        Raises:
            Exception: Re-raises the exception if no fallback is available.
        """
        raise exception

    def on_rm_file_failure(self, path: str, exception: Exception):
        """
        Called when deleting a file fails.

        Args:
            path: The path that failed to be deleted.
            exception: The exception that caused the failure.

        Raises:
            Exception: Can re-raise the exception if no fallback is available.
        """
        raise exception

    def on_rmdir_failure(self, path: str, exception: Exception):
        """
        Called when deleting a directory fails.

        Args:
            path: The path that failed to be deleted.
            exception: The exception that caused the failure.

        Raises:
            Exception: Can re-raise the exception if no fallback is available.
        """
        raise exception

    def on_mkdir_failure(
        self, path: str, create_parents: bool, exception: Exception, **kwargs
    ):
        """
        Called when creating a directory fails.

        Args:
            path: The path that failed to create directory for.
            create_parents: Whether to create parent directories.
            exception: The exception that caused the failure.
            **kwargs: Additional arguments.

        Raises:
            Exception: Re-raises the exception if no fallback is available.
        """
        raise exception

    def on_makedirs_failure(self, path: str, exist_ok: bool, exception: Exception):
        """
        Called when creating directories fails.

        Args:
            path: The path that failed to create directories for.
            exist_ok: Whether it's okay if the directory already exists.
            exception: The exception that caused the failure.

        Raises:
            Exception: Can re-raise the exception if no fallback is available.
        """
        raise exception

    def on_cat_file_failure(
        self, path: str, start: int, end: int, exception: Exception, **kwargs
    ):
        """
        Called when reading file contents fails.

        Args:
            path: The path of the file that failed to read.
            start: The start position to read.
            end: The end position to read.
            exception: The exception that caused the failure.
            **kwargs: Additional arguments.

        Returns:
            The fallback content if available.

        Raises:
            Exception: Re-raises the exception if no fallback is available.
        """
        raise exception

    def on_get_file_failure(
        self,
        rpath: str,
        lpath: str,
        callback: Callback,
        outfile: str,
        exception: Exception,
        **kwargs
    ):
        """
        Called when downloading a file fails.

        Args:
            rpath: The remote path that failed to download.
            lpath: The local path for the file.
            callback: The callback to call.
            outfile: The output file.
            exception: The exception that caused the failure.
            **kwargs: Additional arguments.

        Raises:
            Exception: Can re-raise the exception if no fallback is available.
        """
        raise exception

    def on_created_failure(self, path: str, exception: Exception):
        """
        Called when getting file creation time fails.

        Args:
            path: The path that failed to get creation time for.
            exception: The exception that caused the failure.

        Returns:
            The fallback creation time if available.

        Raises:
            Exception: Re-raises the exception if no fallback is available.
        """
        raise exception

    def on_modified_failure(self, path: str, exception: Exception):
        """
        Called when getting file modification time fails.

        Args:
            path: The path that failed to get modification time for.
            exception: The exception that caused the failure.

        Returns:
            The fallback modification time if available.

        Raises:
            Exception: Re-raises the exception if no fallback is available.
        """
        raise exception


class NoOpHook(GravitinoVirtualFileSystemHook):
    """
    A no-op hook that does nothing.
    """

    def initialize(self, config: Optional[Dict[str, str]]):
        pass

    def pre_ls(self, path: str, detail: bool, **kwargs) -> str:
        return path

    def post_ls(
        self, detail: bool, entries: List[Union[str, Dict[str, Any]]], **kwargs
    ) -> List[Union[str, Dict[str, Any]]]:
        return entries

    def pre_info(self, path: str, **kwargs) -> str:
        return path

    def post_info(self, info: Dict[str, Any], **kwargs) -> Dict[str, Any]:
        return info

    def pre_exists(self, path: str, **kwargs) -> str:
        return path

    def post_exists(self, gvfs_path: str, exists: bool, **kwargs) -> bool:
        return exists

    def pre_cp_file(self, src: str, dst: str, **kwargs) -> Tuple[str, str]:
        return src, dst

    def post_cp_file(self, src_gvfs_path: str, dst_gvfs_path: str, **kwargs):
        pass

    def pre_mv(
        self, src: str, dst: str, recursive: bool, maxdepth: int, **kwargs
    ) -> Tuple[str, str]:
        return src, dst

    def post_mv(
        self,
        src_gvfs_path: str,
        dst_gvfs_path: str,
        recursive: bool,
        maxdepth: int,
        **kwargs
    ):
        pass

    def pre_rm(self, path: str, recursive: bool, maxdepth: int) -> str:
        return path

    def post_rm(self, gvfs_path: str, recursive: bool, maxdepth: int):
        pass

    def pre_rm_file(self, path: str) -> str:
        return path

    def post_rm_file(self, gvfs_path: str):
        pass

    def pre_rmdir(self, path: str) -> str:
        return path

    def post_rmdir(self, gvfs_path: str):
        pass

    def pre_open(
        self,
        path: str,
        mode: str,
        block_size: int,
        cache_options: dict,
        compression: str,
        **kwargs
    ) -> str:
        return path

    def post_open(
        self,
        gvfs_path: str,
        mode: str,
        block_size: int,
        cache_options: dict,
        compression: str,
        file: Any,
        **kwargs
    ) -> Any:
        return file

    def pre_mkdir(self, path: str, create_parents: bool, **kwargs) -> str:
        return path

    def post_mkdir(self, gvfs_path: str, create_parents: bool, **kwargs):
        pass

    def pre_makedirs(self, path: str, exist_ok: bool) -> str:
        return path

    def post_makedirs(self, gvfs_path: str, exist_ok: bool):
        pass

    def pre_cat_file(self, path: str, start: int, end: int, **kwargs) -> str:
        return path

    def post_cat_file(
        self, gvfs_path: str, start: int, end: int, content: Any, **kwargs
    ) -> Any:
        return content

    def pre_get_file(
        self, rpath: str, lpath: str, callback: Callback, outfile: str, **kwargs
    ) -> str:
        return rpath

    def post_get_file(self, gvfs_path: str, local_path: str, outfile: str, **kwargs):
        pass

    def pre_modified(self, path: str) -> str:
        return path

    def post_modified(self, gvfs_path: str, modified: Any) -> Any:
        return modified

    def pre_created(self, path: str) -> str:
        return path

    def post_created(self, gvfs_path: str, created: Any) -> Any:
        return created


DEFAULT_HOOK = NoOpHook()
