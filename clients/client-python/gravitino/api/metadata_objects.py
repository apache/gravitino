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

from typing import ClassVar, Optional, Union, overload

from gravitino.api.metadata_object import MetadataObject
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


class MetadataObjects:
    """The helper class for `MetadataObject`."""

    METADATA_OBJECT_RESERVED_NAME: ClassVar[str] = "*"
    """The reserved name for the metadata object."""

    DOT_SPLITTER: ClassVar[str] = "."
    DOT_JOINER: ClassVar[str] = "."

    _NAMES_LEN_CONDS: ClassVar[dict[int, set[MetadataObject.Type]]] = {
        1: {
            MetadataObject.Type.METALAKE,
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.ROLE,
        },
        2: {MetadataObject.Type.SCHEMA},
        3: {
            MetadataObject.Type.FILESET,
            MetadataObject.Type.TABLE,
            MetadataObject.Type.TOPIC,
            MetadataObject.Type.MODEL,
        },
        4: {MetadataObject.Type.COLUMN},
    }

    @staticmethod
    @overload
    def of(name_or_names: list[str], type_: MetadataObject.Type) -> MetadataObject: ...

    @staticmethod
    @overload
    def of(
        name_or_names: str, type_: MetadataObject.Type, parent: Optional[str] = None
    ) -> MetadataObject: ...

    @staticmethod
    def of(
        name_or_names: Union[str, list[str]],
        type_: MetadataObject.Type,
        parent: Optional[str] = None,
    ) -> MetadataObject:
        if isinstance(name_or_names, str):
            name = name_or_names
            full_name = (
                MetadataObjects.DOT_JOINER.join([parent, name]) if parent else name
            )
            return MetadataObjects.parse(full_name, type_)
        names_len = len(name_or_names)
        Precondition.check_argument(
            names_len > 0,
            "Cannot create a metadata object with no names",
        )
        Precondition.check_argument(
            names_len <= 4,
            "Cannot create a metadata object with the name length which is greater than 4",
        )
        Precondition.check_argument(
            names_len != 1 or type_ in MetadataObjects._NAMES_LEN_CONDS[1],
            "If the length of names is 1, it must be the CATALOG, METALAKE, or ROLE type",
        )
        Precondition.check_argument(
            names_len != 2 or type_ in MetadataObjects._NAMES_LEN_CONDS[2],
            "If the length of names is 2, it must be the SCHEMA type",
        )
        Precondition.check_argument(
            names_len != 3 or type_ in MetadataObjects._NAMES_LEN_CONDS[3],
            "If the length of names is 3, it must be FILESET, TABLE, TOPIC or MODEL",
        )
        Precondition.check_argument(
            names_len != 4 or type_ in MetadataObjects._NAMES_LEN_CONDS[4],
            "If the length of names is 4, it must be COLUMN",
        )
        names = name_or_names
        for name in names:
            MetadataObjects.check_name(name)
        return MetadataObjects.MetadataObjectImpl(
            MetadataObjects.get_last_name(names),
            type_,
            MetadataObjects.get_parent_full_name(names),
        )

    @staticmethod
    def parent(object_: MetadataObject) -> Optional[MetadataObject]:
        """Get the parent metadata object of the given metadata object.

        Args:
            object_ (MetadataObject): The metadata object

        Returns:
            Optional[MetadataObject]:
                The parent metadata object if it exists, otherwise `None`
        """
        if object_ is None:
            return None

        object_type = object_.type()
        # Return None if the object is the root object
        if object_type in {
            MetadataObject.Type.METALAKE,
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.ROLE,
        }:
            return None

        parent_type = None
        if object_type is MetadataObject.Type.COLUMN:
            parent_type = MetadataObject.Type.TABLE
        elif object_type in {
            MetadataObject.Type.TABLE,
            MetadataObject.Type.FILESET,
            MetadataObject.Type.TOPIC,
            MetadataObject.Type.MODEL,
        }:
            parent_type = MetadataObject.Type.SCHEMA
        elif object_type is MetadataObject.Type.SCHEMA:
            parent_type = MetadataObject.Type.CATALOG
        else:
            raise IllegalArgumentException(
                f"Unexpected to reach here for metadata object type: {object_type.value}"
            )

        return MetadataObjects.parse(object_.parent(), parent_type)

    @staticmethod
    def get_parent_full_name(names: list[str]) -> Optional[str]:
        """Get the parent full name of the given full name.

        Args:
            names (list[str]): The names of the metadata object

        Returns:
            Optional[str]: The parent full name if it exists, otherwise `None`.
        """
        names_len = len(names)
        if names_len <= 1:
            return None
        return MetadataObjects.DOT_JOINER.join(names[:-1])

    @staticmethod
    def get_last_name(names: list[str]) -> str:
        return names[-1]

    @staticmethod
    def check_name(name: str) -> None:
        Precondition.check_argument(
            name is not None,
            "Cannot create a metadata object with null name",
        )
        Precondition.check_argument(
            name != MetadataObjects.METADATA_OBJECT_RESERVED_NAME,
            "Cannot create a metadata object with `*` name.",
        )

    @staticmethod
    def parse(full_name: str, type_: MetadataObject.Type) -> MetadataObject:
        Precondition.check_argument(
            full_name is not None and len(full_name.strip()) > 0,
            "Metadata object full name cannot be blank",
        )

        parts = full_name.split(MetadataObjects.DOT_SPLITTER)
        if type_ is MetadataObject.Type.ROLE:
            return MetadataObjects.of([full_name], MetadataObject.Type.ROLE)

        return MetadataObjects.of(parts, type_)

    class MetadataObjectImpl(MetadataObject):
        def __init__(
            self, name: str, type_: MetadataObject.Type, parent: Optional[str] = None
        ) -> None:
            self._name = name
            self._type = type_
            self._parent = parent

        def name(self) -> str:
            return self._name

        def type(self) -> MetadataObject.Type:
            return self._type

        def parent(self) -> Optional[str]:
            return self._parent

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, MetadataObjects.MetadataObjectImpl):
                return False
            return (
                self._name == value.name()
                and self._type == value.type()
                and self._parent == value.parent()
            )

        def __hash__(self) -> int:
            return hash((self._name, self._parent, self._type))

        def __str__(self) -> str:
            return f"MetadataObject: [fullName={self.full_name()}], [type={self.type().value}]"
