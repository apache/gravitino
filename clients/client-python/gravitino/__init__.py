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

from gravitino.api.catalog import Catalog
from gravitino.api.schema import Schema
from gravitino.api.file.fileset import Fileset
from gravitino.api.file.fileset_change import FilesetChange
from gravitino.api.metalake_change import MetalakeChange
from gravitino.api.schema_change import SchemaChange
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.client.gravitino_admin_client import GravitinoAdminClient
from gravitino.client.gravitino_metalake import GravitinoMetalake
from gravitino.name_identifier import NameIdentifier


def __getattr__(name):
    """Load the optional GVFS module only when a caller requests it."""
    if name != "gvfs":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    try:
        gvfs = importlib.import_module("gravitino.filesystem.gvfs")
    except ModuleNotFoundError as error:
        if error.name not in {"fsspec", "cachetools", "readerwriterlock"}:
            raise
        raise ImportError(
            "GVFS support requires optional filesystem dependencies. "
            "Install `apache-gravitino[gvfs]` for local files, a backend extra "
            "such as `apache-gravitino[hdfs]`, or `apache-gravitino[storage]` "
            "for all backends."
        ) from error

    globals()[name] = gvfs
    return gvfs


__all__ = [
    "Catalog",
    "Schema",
    "Fileset",
    "FilesetChange",
    "MetalakeChange",
    "SchemaChange",
    "GravitinoClient",
    "GravitinoAdminClient",
    "GravitinoMetalake",
    "NameIdentifier",
    "gvfs",
]
