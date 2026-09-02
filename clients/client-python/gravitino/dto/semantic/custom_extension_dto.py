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

from gravitino.api.semantic.custom_extension import CustomExtension
from gravitino.dto.semantic.semantic_dto_utils import is_none


@dataclass
class CustomExtensionDTO(DataClassJsonMixin):
    """Represents a vendor-specific custom extension DTO."""

    _vendor_name: Optional[str] = field(
        default=None, metadata=config(field_name="vendor_name", exclude=is_none)
    )
    _data: Optional[str] = field(
        default=None, metadata=config(field_name="data", exclude=is_none)
    )

    def vendor_name(self) -> Optional[str]:
        """Returns the vendor name that owns this extension."""
        return self._vendor_name

    def data(self) -> Optional[str]:
        """Returns the opaque extension data."""
        return self._data

    @staticmethod
    def from_custom_extension(extension: CustomExtension) -> "CustomExtensionDTO":
        """Convert a custom extension to its DTO."""
        return CustomExtensionDTO(
            _vendor_name=extension.vendor_name(), _data=extension.data()
        )

    def to_custom_extension(self) -> CustomExtension:
        """Convert this DTO to a custom extension."""
        return CustomExtension(self._vendor_name, self._data)
