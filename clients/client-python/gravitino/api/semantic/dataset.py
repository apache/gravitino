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

from typing import Optional

from gravitino.api.semantic.ai_context import AIContext
from gravitino.api.semantic.custom_extension import CustomExtension
from gravitino.api.semantic.field import Field
from gravitino.api.semantic.semantic_utils import (
    check_no_none_elements,
    check_non_empty_string_elements,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.utils.precondition import Precondition


class Dataset:  # pylint: disable=too-many-instance-attributes
    """A source-backed dataset exposed by a Semantic Model.

    Dataset names are unique within a Semantic Model. The source is a three-part
    `NameIdentifier` that must resolve to a table or a logical view in the same
    metalake, inline query sources are not supported.
    """

    def __init__(
        self,
        name: str,
        source: NameIdentifier,
        primary_key: Optional[list[str]] = None,
        unique_keys: Optional[list[list[str]]] = None,
        description: Optional[str] = None,
        ai_context: Optional[AIContext] = None,
        fields: Optional[list[Field]] = None,
        custom_extensions: Optional[list[CustomExtension]] = None,
    ):
        Precondition.check_argument(
            name is not None and name != "", "name must not be null or empty"
        )
        Precondition.check_argument(source is not None, "source must not be null")
        check_non_empty_string_elements("primaryKey", primary_key)
        _check_unique_keys(unique_keys)
        check_no_none_elements("fields", fields)
        check_no_none_elements("customExtensions", custom_extensions)

        self._name = name
        self._source = source
        self._primary_key = None if primary_key is None else list(primary_key)
        self._unique_keys = _copy_unique_keys(unique_keys)
        self._description = description
        self._ai_context = ai_context
        self._fields = None if fields is None else list(fields)
        self._custom_extensions = (
            None if custom_extensions is None else list(custom_extensions)
        )

    def name(self) -> str:
        """Returns the dataset name."""
        return self._name

    def source(self) -> NameIdentifier:
        """Returns the identifier of the table or view backing the dataset."""
        return self._source

    def primary_key(self) -> Optional[list[str]]:
        """Returns the primary key columns, or `None` if they are not set."""
        return None if self._primary_key is None else list(self._primary_key)

    def unique_keys(self) -> Optional[list[list[str]]]:
        """Returns the unique key column groups, or `None` if they are not set."""
        return _copy_unique_keys(self._unique_keys)

    def description(self) -> Optional[str]:
        """Returns the dataset description, or `None` if it is not set."""
        return self._description

    def ai_context(self) -> Optional[AIContext]:
        """Returns the AI context, or `None` if it is not set."""
        return self._ai_context

    def fields(self) -> Optional[list[Field]]:
        """Returns the dataset fields, or `None` if they are not set."""
        return None if self._fields is None else list(self._fields)

    def custom_extensions(self) -> Optional[list[CustomExtension]]:
        """Returns the custom extensions, or `None` if they are not set."""
        return (
            None if self._custom_extensions is None else list(self._custom_extensions)
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Dataset):
            return False
        return (
            self._name == other.name()
            and self._source == other.source()
            and self._primary_key == other.primary_key()
            and self._unique_keys == other.unique_keys()
            and self._description == other.description()
            and self._ai_context == other.ai_context()
            and self._fields == other.fields()
            and self._custom_extensions == other.custom_extensions()
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                self._source,
                None if self._primary_key is None else tuple(self._primary_key),
                (
                    None
                    if self._unique_keys is None
                    else tuple(tuple(key) for key in self._unique_keys)
                ),
                self._description,
                self._ai_context,
                None if self._fields is None else tuple(self._fields),
                (
                    None
                    if self._custom_extensions is None
                    else tuple(self._custom_extensions)
                ),
            )
        )

    def __repr__(self) -> str:
        return (
            f"Dataset(name={self._name!r}, source={self._source!r}, "
            f"primaryKey={self._primary_key!r}, uniqueKeys={self._unique_keys!r}, "
            f"description={self._description!r}, aiContext={self._ai_context!r}, "
            f"fields={self._fields!r}, customExtensions={self._custom_extensions!r})"
        )


def _check_unique_keys(unique_keys: Optional[list[list[str]]]) -> None:
    if unique_keys is None:
        return
    for index, unique_key in enumerate(unique_keys):
        Precondition.check_argument(
            unique_key is not None and len(unique_key) > 0,
            f"uniqueKeys[{index}] must not be null or empty",
        )
        check_non_empty_string_elements(f"uniqueKeys[{index}]", unique_key)


def _copy_unique_keys(
    unique_keys: Optional[list[list[str]]],
) -> Optional[list[list[str]]]:
    if unique_keys is None:
        return None
    return [list(unique_key) for unique_key in unique_keys]
