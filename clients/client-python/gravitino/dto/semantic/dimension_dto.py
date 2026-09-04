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

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.semantic.dimension import Dimension
from gravitino.dto.semantic.semantic_dto_utils import is_none


@dataclass
class DimensionDTO(DataClassJsonMixin):
    """Represents a Semantic Model dimension marker DTO."""

    _is_time: Optional[bool] = field(
        default=None,
        metadata=config(field_name="is_time", exclude=is_none),
    )

    def is_time(self) -> Optional[bool]:
        """Returns whether the dimension is a time dimension."""
        return self._is_time

    @staticmethod
    def from_dimension(dimension: Dimension) -> "DimensionDTO":
        """Convert a dimension to its DTO."""
        return DimensionDTO(_is_time=dimension.is_time())

    def to_dimension(self) -> Dimension:
        """Convert this DTO to a dimension."""
        return Dimension(self._is_time)
