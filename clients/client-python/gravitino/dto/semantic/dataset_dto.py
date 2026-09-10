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

from gravitino.api.semantic.dataset import Dataset
from gravitino.dto.semantic.ai_context_dto import AIContextDTO
from gravitino.dto.semantic.custom_extension_dto import CustomExtensionDTO
from gravitino.dto.semantic.field_dto import FieldDTO
from gravitino.dto.semantic.json_serdes.ai_context_serdes import AIContextSerdes
from gravitino.dto.semantic.semantic_dto_utils import convert_list, is_none
from gravitino.name_identifier import NameIdentifier


@dataclass
class DatasetDTO(DataClassJsonMixin):  # pylint: disable=too-many-instance-attributes
    """Represents a Semantic Model dataset DTO."""

    _name: Optional[str] = field(
        default=None, metadata=config(field_name="name", exclude=is_none)
    )
    _source: Optional[NameIdentifier] = field(
        default=None, metadata=config(field_name="source", exclude=is_none)
    )
    _primary_key: Optional[list[str]] = field(
        default=None,
        metadata=config(field_name="primary_key", exclude=is_none),
    )
    _unique_keys: Optional[list[list[str]]] = field(
        default=None,
        metadata=config(field_name="unique_keys", exclude=is_none),
    )
    _description: Optional[str] = field(
        default=None,
        metadata=config(field_name="description", exclude=is_none),
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
    _fields: Optional[list[FieldDTO]] = field(
        default=None,
        metadata=config(field_name="fields", exclude=is_none),
    )
    _custom_extensions: Optional[list[CustomExtensionDTO]] = field(
        default=None,
        metadata=config(field_name="custom_extensions", exclude=is_none),
    )

    def name(self) -> Optional[str]:
        """Returns the dataset name."""
        return self._name

    def source(self) -> Optional[NameIdentifier]:
        """Returns the identifier of the table or view backing the dataset."""
        return self._source

    def primary_key(self) -> Optional[list[str]]:
        """Returns the primary key columns, or `None` if they are not set."""
        return self._primary_key

    def unique_keys(self) -> Optional[list[list[str]]]:
        """Returns the unique key column groups, or `None` if they are not set."""
        return self._unique_keys

    def description(self) -> Optional[str]:
        """Returns the dataset description, or `None` if it is not set."""
        return self._description

    def ai_context(self) -> Optional[AIContextDTO]:
        """Returns the AI context, or `None` if it is not set."""
        return self._ai_context

    def fields(self) -> Optional[list[FieldDTO]]:
        """Returns the dataset fields, or `None` if they are not set."""
        return self._fields

    def custom_extensions(self) -> Optional[list[CustomExtensionDTO]]:
        """Returns the custom extensions, or `None` if they are not set."""
        return self._custom_extensions

    @staticmethod
    def from_dataset(dataset: Dataset) -> "DatasetDTO":
        """Convert a dataset to its DTO."""
        ai_context = dataset.ai_context()
        return DatasetDTO(
            _name=dataset.name(),
            _source=dataset.source(),
            _primary_key=dataset.primary_key(),
            _unique_keys=dataset.unique_keys(),
            _description=dataset.description(),
            _ai_context=(
                None if ai_context is None else AIContextDTO.from_ai_context(ai_context)
            ),
            _fields=convert_list(dataset.fields(), FieldDTO.from_field),
            _custom_extensions=convert_list(
                dataset.custom_extensions(), CustomExtensionDTO.from_custom_extension
            ),
        )

    def to_dataset(self) -> Dataset:
        """Convert this DTO to a dataset."""
        return Dataset(
            name=self._name,
            source=self._source,
            primary_key=self._primary_key,
            unique_keys=self._unique_keys,
            description=self._description,
            ai_context=(
                None if self._ai_context is None else self._ai_context.to_ai_context()
            ),
            fields=convert_list(self._fields, FieldDTO.to_field),
            custom_extensions=convert_list(
                self._custom_extensions, CustomExtensionDTO.to_custom_extension
            ),
        )
