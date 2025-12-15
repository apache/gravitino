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


from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config

from gravitino.api.metadata_object import MetadataObject
from gravitino.utils.precondition import Precondition


@dataclass
class MetadataObjectDTO(MetadataObject):
    """Represents a Metadata Object DTO (Data Transfer Object)."""

    _type: MetadataObject.Type = field(metadata=config(field_name="type"))
    _full_name: str = field(metadata=config(field_name="fullName"))

    _name: str = field(init=False, default="")
    _parent: Optional[str] = field(init=False, default=None)

    def __post_init__(self) -> None:
        self.set_full_name(self._full_name)

    @staticmethod
    def builder() -> MetadataObjectDTO.Builder:
        return MetadataObjectDTO.Builder()

    def type(self) -> MetadataObject.Type:
        return self._type

    def parent(self) -> Optional[str]:
        return self._parent

    def name(self) -> str:
        return self._name

    def set_full_name(self, full_name: str) -> None:
        """
        Sets the full name of the metadata object.

        Args:
            full_name (str): The full name of the metadata object.
        """
        index = full_name.rfind(".")
        if index == -1:
            self._parent = None
            self._name = full_name
        else:
            self._parent = full_name[:index]
            self._name = full_name[index + 1 :]

    class Builder:
        def __init__(self) -> None:
            self._full_name: str = ""
            self._type: MetadataObject.Type = None

        def full_name(self, full_name: str) -> MetadataObjectDTO.Builder:
            self._full_name = full_name
            return self

        def type(self, _type: MetadataObject.Type) -> MetadataObjectDTO.Builder:
            self._type = _type
            return self

        def build(self) -> MetadataObjectDTO:
            Precondition.check_string_not_empty(
                self._full_name, "Full name is required and cannot be empty."
            )
            Precondition.check_argument(
                self._type is not None, "Type is required and cannot be None."
            )

            return MetadataObjectDTO(
                self._type,
                self._full_name,
            )
