"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import errno
import os
import secrets
import shutil
import io
from contextlib import suppress
from enum import Enum
from typing import Dict, Tuple
import fsspec
import regex

from fsspec.utils import infer_storage_options, mirror_from
from pyarrow.fs import FileType, FileSelector, FileSystem, HadoopFileSystem
from readerwriterlock import rwlock
from gravitino.api.catalog import Catalog
from gravitino.api.fileset import Fileset
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.name_identifier import NameIdentifier

PROTOCOL_NAME = "gvfs"


class StorageType(Enum):
    HDFS = "hdfs://"


class FilesetContext:
    def __init__(
        self,
        name_identifier: NameIdentifier,
        fileset: Fileset,
        fs: FileSystem,
        actual_path,
    ):
        self.name_identifier = name_identifier
        self.fileset = fileset
        self.fs = fs
        self.actual_path = actual_path

    def get_name_identifier(self):
        return self.name_identifier

    def get_fileset(self):
        return self.fileset

    def get_fs(self):
        return self.fs

    def get_actual_path(self):
        return self.actual_path


class GravitinoVirtualFileSystem(fsspec.AbstractFileSystem):
    protocol = PROTOCOL_NAME
    _gvfs_prefix = "gvfs://fileset"
    _identifier_pattern = regex.compile(
        "^(?:gvfs://fileset)?/([^/]+)/([^/]+)/([^/]+)(?>/[^/]+)*/?$"
    )

    def __init__(self, server_uri, metalake_name, **kwargs):
        self.metalake = metalake_name
        self.client = GravitinoClient(uri=server_uri, metalake_name=metalake_name)
        self.cache: Dict[NameIdentifier, Tuple] = {}
        self.cache_lock = rwlock.RWLockFair()

        super().__init__(**kwargs)

    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        path = ops["path"]
        return path

    @property
    def fsid(self):
        raise RuntimeError("Unsupported method now.")

    def sign(self, path, expiration=None, **kwargs):
        raise RuntimeError("Unsupported method now.")

    def ls(self, path, detail=True, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            entries = []
            for entry in context.fs.get_file_info(
                FileSelector(self._strip_protocol(context.actual_path))
            ):
                entries.append(
                    self._convert_path_prefix(
                        entry,
                        context.fileset.storage_location(),
                        self._get_virtual_location(context.name_identifier, True),
                    )
                )
            return entries
        raise RuntimeError(
            f"Storage under the fileset: `{context.name_identifier}` doesn't support now."
        )

    def info(self, path, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            [info] = context.fs.get_file_info([actual_path])

            return self._convert_path_prefix(
                info,
                context.fileset.storage_location(),
                self._get_virtual_location(context.name_identifier, True),
            )
        raise RuntimeError(
            f"Storage under the fileset: `{context.name_identifier}` doesn't support now."
        )

    def exists(self, path, **kwargs):
        try:
            self.info(path)
        except FileNotFoundError:
            return False
        return True

    def cp_file(self, path1, path2, **kwargs):
        src_identifier: NameIdentifier = self._extract_identifier(path1)
        dst_identifier: NameIdentifier = self._extract_identifier(path2)
        if src_identifier != dst_identifier:
            raise RuntimeError(
                f"Destination file path identifier: `{dst_identifier}` should be same with src file path "
                f"identifier: `{src_identifier}`."
            )
        src_context: FilesetContext = self._get_fileset_context(path1)
        if src_context.actual_path.startswith(StorageType.HDFS.value):
            if self._check_mount_single_file(src_context.fileset, src_context.fs):
                raise RuntimeError(
                    f"Cannot cp file of the fileset: {src_identifier} which only mounts to a single file."
                )
            dst_context: FilesetContext = self._get_fileset_context(path2)

            src_actual_path = self._strip_protocol(src_context.actual_path).rstrip("/")
            dst_actual_path = self._strip_protocol(dst_context.actual_path).rstrip("/")

            with src_context.fs.open_input_stream(src_actual_path) as lstream:
                tmp_dst_name = f"{dst_actual_path}.tmp.{secrets.token_hex(6)}"
                try:
                    with dst_context.fs.open_output_stream(tmp_dst_name) as rstream:
                        shutil.copyfileobj(lstream, rstream)
                    dst_context.fs.move(tmp_dst_name, dst_actual_path)
                except BaseException:  # noqa
                    with suppress(FileNotFoundError):
                        dst_context.fs.delete_file(tmp_dst_name)
                    raise
        else:
            raise RuntimeError(
                f"Storage under the fileset: `{src_context.name_identifier}` doesn't support now."
            )

    def mv(self, path1, path2, recursive=False, maxdepth=None, **kwargs):
        src_identifier: NameIdentifier = self._extract_identifier(path1)
        dst_identifier: NameIdentifier = self._extract_identifier(path2)
        if src_identifier != dst_identifier:
            raise RuntimeError(
                f"Destination file path identifier: `{dst_identifier}`"
                f" should be same with src file path identifier: `{src_identifier}`."
            )
        src_context: FilesetContext = self._get_fileset_context(path1)
        if src_context.actual_path.startswith(StorageType.HDFS.value):
            if self._check_mount_single_file(src_context.fileset, src_context.fs):
                raise RuntimeError(
                    f"Cannot cp file of the fileset: {src_identifier} which only mounts to a single file."
                )
            dst_context: FilesetContext = self._get_fileset_context(path2)

            src_actual_path = self._strip_protocol(src_context.actual_path).rstrip("/")
            dst_actual_path = self._strip_protocol(dst_context.actual_path).rstrip("/")

            src_context.fs.move(src_actual_path, dst_actual_path)
        else:
            raise RuntimeError(
                f"Storage under the fileset: `{src_context.name_identifier}` doesn't support now."
            )

    def rm_file(self, path):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            context.fs.delete_file(actual_path)
        else:
            raise RuntimeError(
                f"Storage under the fileset: `{context.name_identifier}` doesn't support now."
            )

    def _rm(self, path):
        raise RuntimeError("Deprecated method, use rm_file method instead.")

    def rm(self, path, recursive=False, maxdepth=None, **kwargs):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path).rstrip("/")
            [info] = context.fs.get_file_info([actual_path])
            if info.type is FileType.Directory:
                if recursive:
                    context.fs.delete_dir(actual_path)
                else:
                    raise RuntimeError(
                        "Cannot delete directories that recursive is `False`."
                    )
            else:
                context.fs.delete_file(actual_path)
        else:
            raise RuntimeError(
                f"Storage under the fileset: `{context.name_identifier}` doesn't support now."
            )

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kwargs,
    ):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            if mode == "rb":
                method = context.fs.open_input_stream
            elif mode == "wb":
                method = context.fs.open_output_stream
            elif mode == "ab":
                method = context.fs.open_append_stream
            else:
                raise RuntimeError(f"Unsupported mode for Arrow FileSystem: {mode!r}.")

            _kwargs = {}
            stream = method(context.actual_path, **_kwargs)

            return HDFSFile(
                self, stream, context.actual_path, mode, block_size, **kwargs
            )
        raise RuntimeError(
            f"Storage under the fileset: `{context.name_identifier}` doesn't support now."
        )

    def mkdir(self, path, create_parents=True, **kwargs):
        if create_parents:
            self.makedirs(path, exist_ok=True)
        else:
            context: FilesetContext = self._get_fileset_context(path)
            if context.actual_path.startswith(StorageType.HDFS.value):
                actual_path = self._strip_protocol(context.actual_path)
                context.fs.create_dir(actual_path, recursive=False)
            else:
                raise RuntimeError(
                    f"Storage under the fileset: `{context.name_identifier}` doesn't support now."
                )

    def makedirs(self, path, exist_ok=True):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            context.fs.create_dir(actual_path)
        else:
            raise RuntimeError(
                f"Storage under the fileset: `{context.name_identifier}` doesn't support now."
            )

    def rmdir(self, path):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            context.fs.delete_dir(actual_path)
        else:
            raise RuntimeError(
                f"Storage under the fileset: `{context.name_identifier}` doesn't support now."
            )

    def created(self, path):
        raise RuntimeError("Unsupported method now.")

    def modified(self, path):
        context: FilesetContext = self._get_fileset_context(path)
        if context.actual_path.startswith(StorageType.HDFS.value):
            actual_path = self._strip_protocol(context.actual_path)
            return context.fs.get_file_info(actual_path).mtime
        raise RuntimeError(
            f"Storage under the fileset: `{context.name_identifier}` doesn't support now."
        )

    def cat_file(self, path, start=None, end=None, **kwargs):
        raise RuntimeError("Unsupported method now.")

    def get_file(self, rpath, lpath, callback=None, outfile=None, **kwargs):
        raise RuntimeError("Unsupported method now.")

    def _convert_path_prefix(self, info, storage_location, virtual_location):
        actual_prefix = self._strip_protocol(storage_location)
        virtual_prefix = self._strip_protocol(virtual_location)
        if not info.path.startswith(actual_prefix):
            raise ValueError(
                f"Path {info.path} does not start with valid prefix {actual_prefix}."
            )

        if info.type is FileType.Directory:
            kind = "directory"
        elif info.type is FileType.File:
            kind = "file"
        elif info.type is FileType.NotFound:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), info.path)
        else:
            kind = "other"

        return {
            "name": f"{self._gvfs_prefix}{info.path.replace(actual_prefix, virtual_prefix)}",
            "size": info.size,
            "type": kind,
            "mtime": info.mtime,
        }

    def _get_fileset_context(self, virtual_path: str) -> FilesetContext:
        identifier: NameIdentifier = self._extract_identifier(virtual_path)
        read_lock = self.cache_lock.gen_rlock()
        try:
            read_lock.acquire()
            cache_value: Tuple[Fileset, FileSystem] = self.cache.get(identifier)
            if cache_value is not None:
                actual_path = self._get_actual_path_by_ident(
                    identifier, cache_value[0], cache_value[1], virtual_path
                )
                return FilesetContext(
                    identifier, cache_value[0], cache_value[1], actual_path
                )
        finally:
            read_lock.release()

        write_lock = self.cache_lock.gen_wlock()
        try:
            write_lock.acquire()
            cache_value: Tuple[Fileset, FileSystem] = self.cache.get(identifier)
            if cache_value is not None:
                actual_path = self._get_actual_path_by_ident(
                    identifier, cache_value[0], cache_value[1], virtual_path
                )
                return FilesetContext(
                    identifier, cache_value[0], cache_value[1], actual_path
                )
            fileset: Fileset = self._load_fileset_from_server(identifier)
            storage_location = fileset.storage_location()
            if storage_location.startswith(StorageType.HDFS.value):
                fs = HadoopFileSystem.from_uri(storage_location)
                actual_path = self._get_actual_path_by_ident(
                    identifier, fileset, fs, virtual_path
                )
                self.cache[identifier] = (fileset, fs)
                context = FilesetContext(identifier, fileset, fs, actual_path)
                return context
            raise ValueError(
                f"Storage under the fileset: `{identifier}` doesn't support now."
            )
        finally:
            write_lock.release()

    def _extract_identifier(self, path) -> NameIdentifier:
        if path is None or len(path) == 0:
            raise ValueError("path which need be extracted cannot be null or empty.")
        match = self._identifier_pattern.match(path)
        if match and len(match.groups()) == 3:
            return NameIdentifier.of_fileset(
                self.metalake,
                match.group(1),
                match.group(2),
                match.group(3),
            )
        raise ValueError(f"path: `{path}` doesn't contains valid identifier.")

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
        fs: FileSystem,
        virtual_path: str,
    ) -> str:
        with_scheme = virtual_path.startswith(self._gvfs_prefix)
        virtual_location = self._get_virtual_location(identifier, with_scheme)
        storage_location = fileset.storage_location()
        if self._check_mount_single_file(fileset, fs):
            if virtual_path != virtual_location:
                raise ValueError(
                    f"Path: {virtual_path} should be same with the virtual location: {virtual_location}"
                    " when the fileset only mounts a single file."
                )
            return storage_location
        return virtual_path.replace(virtual_location, storage_location, 1)

    def _get_virtual_location(
        self, identifier: NameIdentifier, with_scheme: bool
    ) -> str:
        prefix = self._gvfs_prefix if with_scheme is True else ""
        return (
            f"{prefix}"
            f"/{identifier.namespace().level(1)}"
            f"/{identifier.namespace().level(2)}"
            f"/{identifier.name()}"
        )

    def _check_mount_single_file(self, fileset: Fileset, fs: FileSystem) -> bool:
        [info] = fs.get_file_info([(self._strip_protocol(fileset.storage_location()))])
        return info.type is FileType.File


@mirror_from(
    "stream",
    [
        "read",
        "seek",
        "tell",
        "write",
        "readable",
        "writable",
        "close",
        "size",
        "seekable",
    ],
)
class HDFSFile(io.IOBase):
    def __init__(self, fs, stream, path, mode, block_size=None, **kwargs):
        self.path = path
        self.mode = mode

        self.fs = fs
        self.stream = stream

        self.blocksize = self.block_size = block_size
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self.close()


fsspec.register_implementation(PROTOCOL_NAME, GravitinoVirtualFileSystem)
