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

from typing import TYPE_CHECKING, List

from gravitino.utils.precondition import Precondition

if TYPE_CHECKING:
    from gravitino.dto.rel.column_dto import ColumnDTO


class PartitionUtils:
    """Validates the existence of the partition field in the table."""

    @staticmethod
    def validate_field_existence(
        columns: List["ColumnDTO"], field_name: List[str]
    ) -> None:
        """Validates the existence of the partition field in the table.

        Args:
            columns (List[ColumnDTO]): The columns of the table.
            field_name (List[str]): The name of the field to validate.

        Raises:
            IllegalArgumentException:
                If the field does not exist in the table, this exception is thrown.
        """
        Precondition.check_argument(
            columns is not None and len(columns) > 0, "columns cannot be null or empty"
        )
        # TODO: Need to consider the case sensitivity issues. To be optimized.
        partition_column = [
            c for c in columns if c.name().lower() == field_name[0].lower()
        ]

        Precondition.check_argument(
            len(partition_column) == 1, f"Field '{field_name[0]}' not found in table"
        )
        # TODO: should validate nested fieldName after column type support namedStruct
