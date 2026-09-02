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

from gravitino.api.semantic.relationship import Relationship
from gravitino.dto.semantic.ai_context_dto import AIContextDTO
from gravitino.dto.semantic.custom_extension_dto import CustomExtensionDTO
from gravitino.dto.semantic.json_serdes.ai_context_serdes import AIContextSerdes
from gravitino.dto.semantic.semantic_dto_utils import convert_list, is_none


@dataclass
class RelationshipDTO(
    DataClassJsonMixin
):  # pylint: disable=too-many-instance-attributes
    """Represents a Semantic Model relationship DTO."""

    _name: Optional[str] = field(
        default=None, metadata=config(field_name="name", exclude=is_none)
    )
    _from_dataset: Optional[str] = field(
        default=None, metadata=config(field_name="from", exclude=is_none)
    )
    _to_dataset: Optional[str] = field(
        default=None, metadata=config(field_name="to", exclude=is_none)
    )
    _from_columns: Optional[list[str]] = field(
        default=None, metadata=config(field_name="from_columns", exclude=is_none)
    )
    _to_columns: Optional[list[str]] = field(
        default=None, metadata=config(field_name="to_columns", exclude=is_none)
    )
    _ai_context: Optional[AIContextDTO] = field(
        default=None,
        metadata=config(
            field_name="ai_context",
            encoder=AIContextSerdes.serialize,
            decoder=AIContextSerdes.deserialize,
            exclude=is_none,
        ),
    )
    _custom_extensions: Optional[list[CustomExtensionDTO]] = field(
        default=None,
        metadata=config(field_name="custom_extensions", exclude=is_none),
    )

    def name(self) -> Optional[str]:
        """Returns the relationship name."""
        return self._name

    def from_dataset(self) -> Optional[str]:
        """Returns the name of the dataset the relationship joins from."""
        return self._from_dataset

    def to_dataset(self) -> Optional[str]:
        """Returns the name of the dataset the relationship joins to."""
        return self._to_dataset

    def from_columns(self) -> Optional[list[str]]:
        """Returns the joined columns exposed by the source dataset."""
        return self._from_columns

    def to_columns(self) -> Optional[list[str]]:
        """Returns the joined columns exposed by the target dataset."""
        return self._to_columns

    def ai_context(self) -> Optional[AIContextDTO]:
        """Returns the AI context, or `None` if it is not set."""
        return self._ai_context

    def custom_extensions(self) -> Optional[list[CustomExtensionDTO]]:
        """Returns the custom extensions, or `None` if they are not set."""
        return self._custom_extensions

    @staticmethod
    def from_relationship(relationship: Relationship) -> "RelationshipDTO":
        """Convert a relationship to its DTO."""
        ai_context = relationship.ai_context()
        return RelationshipDTO(
            _name=relationship.name(),
            _from_dataset=relationship.from_dataset(),
            _to_dataset=relationship.to_dataset(),
            _from_columns=relationship.from_columns(),
            _to_columns=relationship.to_columns(),
            _ai_context=(
                None if ai_context is None else AIContextDTO.from_ai_context(ai_context)
            ),
            _custom_extensions=convert_list(
                relationship.custom_extensions(),
                CustomExtensionDTO.from_custom_extension,
            ),
        )

    def to_relationship(self) -> Relationship:
        """Convert this DTO to a relationship."""
        return Relationship(
            name=self._name,
            from_dataset=self._from_dataset,
            to_dataset=self._to_dataset,
            from_columns=self._from_columns,
            to_columns=self._to_columns,
            ai_context=(
                None if self._ai_context is None else self._ai_context.to_ai_context()
            ),
            custom_extensions=convert_list(
                self._custom_extensions, CustomExtensionDTO.to_custom_extension
            ),
        )
