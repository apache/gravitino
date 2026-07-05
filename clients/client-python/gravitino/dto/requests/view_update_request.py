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

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config, dataclass_json

from gravitino.api.rel.view_change import ViewChange
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.json_serdes.representation_serdes import RepresentationSerdes
from gravitino.dto.rel.representation_dto import RepresentationDTO
from gravitino.dto.requests.view_create_request import ViewCreateRequest
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class ViewUpdateRequestBase(RESTRequest, ABC):
    """Base class for all view update requests."""

    _type: str = field(init=False, metadata=config(field_name="@type"))

    @abstractmethod
    def view_change(self) -> ViewChange:
        """Convert to view change operation."""


class ViewUpdateRequest:
    """Namespace for all view update request types."""

    @dataclass_json
    @dataclass
    class RenameViewRequest(ViewUpdateRequestBase):
        """Update request to rename a view."""

        _new_name: str = field(metadata=config(field_name="newName"))

        def __post_init__(self):
            self._type = "rename"

        def validate(self):
            Precondition.check_string_not_empty(
                self._new_name, '"newName" field is required and cannot be empty'
            )

        def view_change(self) -> ViewChange:
            return ViewChange.rename(self._new_name)

    @dataclass_json
    @dataclass
    class SetViewPropertyRequest(ViewUpdateRequestBase):
        """Update request to set a view property."""

        _property: str = field(metadata=config(field_name="property"))
        _value: str = field(metadata=config(field_name="value"))

        def __post_init__(self):
            self._type = "setProperty"

        def validate(self):
            Precondition.check_string_not_empty(
                self._property, '"property" field is required and cannot be empty'
            )
            Precondition.check_argument(
                self._value is not None, '"value" field is required and cannot be null'
            )

        def view_change(self) -> ViewChange:
            return ViewChange.set_property(self._property, self._value)

    @dataclass_json
    @dataclass
    class RemoveViewPropertyRequest(ViewUpdateRequestBase):
        """Update request to remove a view property."""

        _property: str = field(metadata=config(field_name="property"))

        def __post_init__(self):
            self._type = "removeProperty"

        def validate(self):
            Precondition.check_string_not_empty(
                self._property, '"property" field is required and cannot be empty'
            )

        def view_change(self) -> ViewChange:
            return ViewChange.remove_property(self._property)

    @dataclass_json
    @dataclass
    class ReplaceViewRequest(ViewUpdateRequestBase):
        """Update request to replace the view body."""

        _columns: Optional[list[ColumnDTO]] = field(
            default=None, metadata=config(field_name="columns")
        )
        _representations: list[RepresentationDTO] = field(
            default=None,
            metadata=config(
                field_name="representations",
                encoder=lambda items: [
                    RepresentationSerdes.serialize(item) for item in items
                ],
            ),
        )
        _default_catalog: Optional[str] = field(
            default=None, metadata=config(field_name="defaultCatalog")
        )
        _default_schema: Optional[str] = field(
            default=None, metadata=config(field_name="defaultSchema")
        )
        _comment: Optional[str] = field(
            default=None, metadata=config(field_name="comment")
        )

        def __post_init__(self):
            self._type = "replaceView"

        def validate(self):
            Precondition.check_argument(
                self._representations is not None and len(self._representations) > 0,
                '"representations" field is required and cannot be empty',
            )
            for representation in self._representations:
                Precondition.check_argument(
                    representation is not None, "representation must not be null"
                )
                representation.validate()
            if self._columns:
                for column in self._columns:
                    Precondition.check_argument(
                        column is not None, "column must not be null"
                    )
                    column.validate()
            ViewCreateRequest.validate_no_duplicate_dialects(self._representations)

        def view_change(self) -> ViewChange:
            return ViewChange.replace_view(
                self._columns or [],
                self._representations,
                self._default_catalog,
                self._default_schema,
                self._comment,
            )
