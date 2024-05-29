"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from enum import Enum
from pathlib import PurePosixPath
from typing import Dict, Tuple
import re
import fsspec

from fsspec import AbstractFileSystem
from fsspec.implementations.local import LocalFileSystem
from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.utils import infer_storage_options
from pyarrow.fs import HadoopFileSystem
from readerwriterlock import rwlock
from gravitino.api.catalog import Catalog
from gravitino.api.fileset import Fileset
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.name_identifier import NameIdentifier

PROTOCOL_NAME = "gvfs"


class StorageType(Enum):
    HDFS = "hdfs"
    FILE = "file"


class FilesetContext:
    def __init__(
        self,
        name_identifier: NameIdentifier,
        fileset: Fileset,
        fs: AbstractFileSystem,
        storage_type: StorageType,
        actual_path: str,
    ):
        self.name_identifier = name_identifier
        self.fileset = fileset
        self.fs = fs
        self.storage_type = storage_type
        self.actual_path = actual_path

    def get_name_identifier(self):
        return self.name_identifier

    def get_fileset(self):
        return self.fileset

    def get_fs(self):
        return self.fs

    def get_actual_path(self):
        return self.actual_path

    def get_storage_type(self):
        return self.storage_type


class GravitinoVirtualFileSystem(fsspec.AbstractFileSystem):
    protocol = PROTOCOL_NAME
    _identifier_pattern = re.compile("^fileset/([^/]+)/([^/]+)/([^/]+)(?:/[^/]+)*/?$")

    def __init__(self, server_uri=None, metalake_name=None, **kwargs):
        self.metalake = metalake_name
        self.client = GravitinoClient(uri=server_uri, metalake_name=metalake_name)
        self.cache: Dict[NameIdentifier, Tuple] = {}
        self.cache_lock = rwlock.RWLockFair()

        super().__init__(**kwargs)

    @property
    def fsid(self):
        raise RuntimeError("Unsupported method now.")

    def sign(self, path, expiration=None, **kwargs):
        raise RuntimeError("Unsupported method now.")

    def ls(self, path, detail=True, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        if detail:
            entries = [
                self._convert_info(entry, context)
                for entry in context.fs.ls(
                    self._strip_storage_protocol(
                        context.storage_type, context.actual_path
                    ),
                    detail=True,
                )
            ]
            return entries
        entries = [
            self._actual_path_to_virtual_path(entry_path, context)
            for entry_path in context.fs.ls(
                self._strip_storage_protocol(context.storage_type, context.actual_path),
                detail=True,
            )
        ]
        return entries

    def info(self, path, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        actual_info: Dict = context.fs.info(
            self._strip_storage_protocol(context.storage_type, context.actual_path)
        )
        return self._convert_info(actual_info, context)

    def exists(self, path, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        try:
            context.fs.info(
                self._strip_storage_protocol(context.storage_type, context.actual_path)
            )
        except FileNotFoundError:
            return False
        return True

    def cp_file(self, path1, path2, **kwargs):
        path1 = self._pre_process_path(path1)
        path2 = self._pre_process_path(path2)
        src_identifier: NameIdentifier = self._extract_identifier(path1)
        dst_identifier: NameIdentifier = self._extract_identifier(path2)
        if src_identifier != dst_identifier:
            raise RuntimeError(
                f"Destination file path identifier: `{dst_identifier}` should be same with src file path "
                f"identifier: `{src_identifier}`."
            )
        src_context: FilesetContext = self._get_fileset_context(path1)
        if self._check_mount_single_file(
            src_context.fileset, src_context.fs, src_context.storage_type
        ):
            raise RuntimeError(
                f"Cannot cp file of the fileset: {src_identifier} which only mounts to a single file."
            )
        dst_context: FilesetContext = self._get_fileset_context(path2)

        src_context.fs.cp_file(
            self._strip_storage_protocol(
                src_context.storage_type, src_context.actual_path
            ),
            self._strip_storage_protocol(
                dst_context.storage_type, dst_context.actual_path
            ),
        )

    # pylint: disable=W0221
    def mv(self, path1, path2, **kwargs):
        """
        1. Supports move a file which src and dst under the same directory.
            fs.mv(path1='gvfs://fileset/fileset_catalog/tmp/tmp_fileset/ttt/test2.txt',
                  path2='gvfs://fileset/fileset_catalog/tmp/tmp_fileset/ttt/test3.txt')
        2. Supports move a directory which src and dst under the same directory.
            fs.mv(path1='gvfs://fileset/fileset_catalog/tmp/tmp_fileset/ttt/qqq',
                  path2='gvfs://fileset/fileset_catalog/tmp/tmp_fileset/ttt/zzz')
        3. Supports move a file which destination's parent directory is existed.
           The directory `gvfs://fileset/fileset_catalog/tmp/tmp_fileset/xyz/qqq` is existed.
           fs.mv(path1='gvfs://fileset/fileset_catalog/tmp/tmp_fileset/ttt/test2.txt',
                  path2='gvfs://fileset/fileset_catalog/tmp/tmp_fileset/xyz/qqq/test3.txt')
        4. Supports move a directory which destination's parent directory is existed.
           The directory `gvfs://fileset/fileset_catalog/tmp/tmp_fileset/xyz/qqq` is existed.
           fs.mv(path1='gvfs://fileset/fileset_catalog/tmp/tmp_fileset/ttt/zzz',
                  path2='gvfs://fileset/fileset_catalog/tmp/tmp_fileset/xyz/qqq/ppp')
        """
        path1 = self._pre_process_path(path1)
        path2 = self._pre_process_path(path2)
        src_identifier: NameIdentifier = self._extract_identifier(path1)
        dst_identifier: NameIdentifier = self._extract_identifier(path2)
        if src_identifier != dst_identifier:
            raise RuntimeError(
                f"Destination file path identifier: `{dst_identifier}`"
                f" should be same with src file path identifier: `{src_identifier}`."
            )
        src_context: FilesetContext = self._get_fileset_context(path1)
        if self._check_mount_single_file(
            src_context.fileset, src_context.fs, src_context.storage_type
        ):
            raise RuntimeError(
                f"Cannot cp file of the fileset: {src_identifier} which only mounts to a single file."
            )
        dst_context: FilesetContext = self._get_fileset_context(path2)
        src_context.fs.move(
            self._strip_storage_protocol(
                src_context.storage_type, src_context.actual_path
            ),
            self._strip_storage_protocol(
                dst_context.storage_type, dst_context.actual_path
            ),
        )

    def _rm(self, path):
        raise RuntimeError("Deprecated method, use rm_file method instead.")

    def rm(self, path, recursive=False, maxdepth=None):
        context: FilesetContext = self._get_fileset_context(path)
        context.fs.rm(
            self._strip_storage_protocol(context.storage_type, context.actual_path),
            recursive,
            maxdepth,
        )

    def rm_file(self, path):
        context: FilesetContext = self._get_fileset_context(path)
        context.fs.rm_file(
            self._strip_storage_protocol(context.storage_type, context.actual_path)
        )

    def rmdir(self, path):
        context: FilesetContext = self._get_fileset_context(path)
        context.fs.rmdir(
            self._strip_storage_protocol(context.storage_type, context.actual_path)
        )

    # pylint: disable=W0221
    def _open(self, path, mode="rb", block_size=None, seekable=True, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        if context.storage_type == StorageType.HDFS:
            return context.fs._open(
                self._strip_storage_protocol(context.storage_type, context.actual_path),
                mode,
                block_size,
                seekable,
                **kwargs,
            )
        if context.storage_type == StorageType.FILE:
            return context.fs._open(
                self._strip_storage_protocol(context.storage_type, context.actual_path),
                mode,
                block_size,
                **kwargs,
            )
        raise RuntimeError(f"Storage type:{context.storage_type} doesn't support now.")

    def mkdir(self, path, create_parents=True, **kwargs):
        if create_parents:
            self.makedirs(path, exist_ok=True)
        else:
            context: FilesetContext = self._get_fileset_context(path)
            context.fs.mkdir(
                self._strip_storage_protocol(context.storage_type, context.actual_path),
                create_parents,
            )

    def makedirs(self, path, exist_ok=True):
        context: FilesetContext = self._get_fileset_context(path)
        context.fs.makedirs(
            self._strip_storage_protocol(context.storage_type, context.actual_path),
            exist_ok,
        )

    def created(self, path):
        raise RuntimeError("Unsupported method now.")

    def modified(self, path):
        context: FilesetContext = self._get_fileset_context(path)
        return context.fs.modified(
            self._strip_storage_protocol(context.storage_type, context.actual_path)
        ).mtime

    def cat_file(self, path, start=None, end=None, **kwargs):
        kwargs["seekable"] = start not in [None, 0]
        context: FilesetContext = self._get_fileset_context(path)
        return context.fs.cat_file(
            self._strip_storage_protocol(context.storage_type, context.actual_path),
            start,
            end,
            **kwargs,
        )

    def get_file(self, rpath, lpath, **kwargs):
        kwargs["seekable"] = False
        context: FilesetContext = self._get_fileset_context(rpath)
        return context.fs.get_file(
            self._strip_storage_protocol(context.storage_type, context.actual_path),
            lpath,
            **kwargs,
        )

    def _actual_path_to_virtual_path(self, path, context: FilesetContext):
        if context.storage_type == StorageType.HDFS:
            actual_prefix = infer_storage_options(context.fileset.storage_location())[
                "path"
            ]
        elif context.storage_type == StorageType.FILE:
            actual_prefix = context.fileset.storage_location()[
                len(f"{StorageType.FILE.value}:") :
            ]
        else:
            raise RuntimeError(
                f"Storage type:{context.storage_type} doesn't support now."
            )

        if not path.startswith(actual_prefix):
            raise RuntimeError(
                f"Path {path} does not start with valid prefix {actual_prefix}."
            )
        virtual_location = self._get_virtual_location(context.name_identifier)
        return f"{path.replace(actual_prefix, virtual_location)}"

    def _convert_info(self, entry: Dict, context: FilesetContext):
        path = self._actual_path_to_virtual_path(entry["name"], context)
        return {
            "name": path,
            "size": entry["size"],
            "type": entry["type"],
            "mtime": entry["mtime"],
        }

    def _get_fileset_context(self, virtual_path: str) -> FilesetContext:
        virtual_path: str = self._pre_process_path(virtual_path)
        identifier: NameIdentifier = self._extract_identifier(virtual_path)
        read_lock = self.cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[Fileset, AbstractFileSystem, StorageType] = (
                self.cache.get(identifier)
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

        write_lock = self.cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: Tuple[Fileset, AbstractFileSystem] = self.cache.get(identifier)
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
            elif storage_location.startswith(f"{StorageType.FILE.value}:/"):
                fs = LocalFileSystem()
                storage_type = StorageType.FILE
            else:
                raise ValueError(
                    f"Storage under the fileset: `{identifier}` doesn't support now."
                )
            actual_path = self._get_actual_path_by_ident(
                identifier, fileset, fs, storage_type, virtual_path
            )
            self.cache[identifier] = (fileset, fs, storage_type)
            context = FilesetContext(identifier, fileset, fs, storage_type, actual_path)
            return context
        finally:
            write_lock.release()

    def _extract_identifier(self, path) -> NameIdentifier:
        if path is None:
            raise RuntimeError("path which need be extracted cannot be null or empty.")

        match = self._identifier_pattern.match(path)
        if match and len(match.groups()) == 3:
            return NameIdentifier.of_fileset(
                self.metalake, match.group(1), match.group(2), match.group(3)
            )
        raise RuntimeError(f"path: `{path}` doesn't contains valid identifier.")

    def _load_fileset_from_server(self, identifier: NameIdentifier) -> Fileset:
        catalog: Catalog = self.client.load_catalog(
            NameIdentifier.of_catalog(
                identifier.namespace().level(0), identifier.namespace().level(1)
            )
        )
        return catalog.as_fileset_catalog().load_fileset(identifier)

    def _get_actual_path_by_ident(
        self,
        identifier: NameIdentifier,
        fileset: Fileset,
        fs: AbstractFileSystem,
        storage_type: StorageType,
        virtual_path: str,
    ) -> str:
        virtual_location = self._get_virtual_location(identifier)
        storage_location = fileset.storage_location()
        if self._check_mount_single_file(fileset, fs, storage_type):
            if virtual_path != virtual_location:
                raise RuntimeError(
                    f"Path: {virtual_path} should be same with the virtual location: {virtual_location}"
                    " when the fileset only mounts a single file."
                )
            return storage_location
        return virtual_path.replace(virtual_location, storage_location, 1)

    @staticmethod
    def _get_virtual_location(identifier: NameIdentifier) -> str:
        return (
            f"fileset/{identifier.namespace().level(1)}"
            f"/{identifier.namespace().level(2)}"
            f"/{identifier.name()}"
        )

    def _check_mount_single_file(
        self, fileset: Fileset, fs: AbstractFileSystem, storage_type: StorageType
    ) -> bool:
        result: Dict = fs.info(
            self._strip_storage_protocol(storage_type, fileset.storage_location())
        )
        return result["type"] == "file"

    @staticmethod
    def _pre_process_path(path):
        if isinstance(path, PurePosixPath):
            path = path.as_posix()
        gvfs_prefix = f"{PROTOCOL_NAME}://"
        if path.startswith(gvfs_prefix):
            path = path[len(gvfs_prefix) :]
        if not path.startswith("fileset/"):
            raise RuntimeError(
                f"Invalid path:`{path}`. Expected path to start with `fileset/`."
                " Example: fileset/{fileset_catalog}/{schema}/{fileset_name}/{sub_path}."
            )
        return path

    @staticmethod
    def _strip_storage_protocol(storage_type: StorageType, path: str):
        if storage_type == StorageType.HDFS:
            return path
        if storage_type == StorageType.FILE:
            return path[len(f"{StorageType.FILE.value}:") :]
        raise RuntimeError(f"Storage type:{storage_type} doesn't support now.")


fsspec.register_implementation(PROTOCOL_NAME, GravitinoVirtualFileSystem)
