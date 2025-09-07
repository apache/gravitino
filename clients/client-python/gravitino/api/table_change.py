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

from abc import ABC
from dataclasses import dataclass, field
from typing import final

from dataclasses_json import config


class TableChange(ABC):
    """Defines the public APIs for managing tables in a schema.

    The `TableChange` interface defines the public API for managing tables in a schema.
    If the catalog implementation supports tables, it must implement this interface.
    """

    @staticmethod
    def rename(new_name: str) -> "RenameTable":
        """Create a `TableChange` for renaming a table.

        Args:
            new_name: The new table name.

        Returns:
            RenameTable: A `TableChange` for the rename.
        """
        return TableChange.RenameTable(new_name)

    @final
    @dataclass(frozen=True)
    class RenameTable:
        """A `TableChange` to rename a table."""

        _new_name: str = field(metadata=config(field_name="new_name"))

        def get_new_name(self) -> str:
            """Retrieves the new name for the table.

            Returns:
                str: The new name of the table.
            """
            return self._new_name

        def __str__(self):
            return f"RENAMETABLE {self._new_name}"
