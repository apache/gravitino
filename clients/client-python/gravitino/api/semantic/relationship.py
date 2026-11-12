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
from gravitino.api.semantic.semantic_utils import (
    check_no_none_elements,
    check_non_empty_string_elements,
)
from gravitino.utils.precondition import Precondition


class Relationship:  # pylint: disable=too-many-instance-attributes
    """A join between two datasets in the same Semantic Model.

    Relationship names are unique within a Semantic Model. Both endpoints name a
    dataset in the same Semantic Model, and the joined column lists are non-empty
    and of equal length.

    ``from_dataset`` and ``to_dataset`` map to the Ossie ``from`` and ``to``
    fields, which cannot be used as Python identifiers.
    """

    def __init__(
        self,
        name: str,
        from_dataset: str,
        to_dataset: str,
        from_columns: list[str],
        to_columns: list[str],
        ai_context: Optional[AIContext] = None,
        custom_extensions: Optional[list[CustomExtension]] = None,
    ):
        Precondition.check_argument(
            name is not None and name != "", "name must not be null or empty"
        )
        Precondition.check_argument(
            from_dataset is not None and from_dataset != "",
            "from must not be null or empty",
        )
        Precondition.check_argument(
            to_dataset is not None and to_dataset != "", "to must not be null or empty"
        )
        Precondition.check_argument(
            from_columns is not None and len(from_columns) > 0,
            "fromColumns must not be null or empty",
        )
        Precondition.check_argument(
            to_columns is not None and len(to_columns) > 0,
            "toColumns must not be null or empty",
        )
        check_non_empty_string_elements("fromColumns", from_columns)
        check_non_empty_string_elements("toColumns", to_columns)
        Precondition.check_argument(
            len(from_columns) == len(to_columns),
            "fromColumns and toColumns must have the same length",
        )
        check_no_none_elements("customExtensions", custom_extensions)

        self._name = name
        self._from_dataset = from_dataset
        self._to_dataset = to_dataset
        self._from_columns = list(from_columns)
        self._to_columns = list(to_columns)
        self._ai_context = ai_context
        self._custom_extensions = (
            None if custom_extensions is None else list(custom_extensions)
        )

    def name(self) -> str:
        """Returns the relationship name."""
        return self._name

    def from_dataset(self) -> str:
        """Returns the name of the dataset the relationship joins from."""
        return self._from_dataset

    def to_dataset(self) -> str:
        """Returns the name of the dataset the relationship joins to."""
        return self._to_dataset

    def from_columns(self) -> list[str]:
        """Returns the joined columns exposed by the source dataset."""
        return list(self._from_columns)

    def to_columns(self) -> list[str]:
        """Returns the joined columns exposed by the target dataset."""
        return list(self._to_columns)

    def ai_context(self) -> Optional[AIContext]:
        """Returns the AI context, or `None` if it is not set."""
        return self._ai_context

    def custom_extensions(self) -> Optional[list[CustomExtension]]:
        """Returns the custom extensions, or `None` if they are not set."""
        return (
            None if self._custom_extensions is None else list(self._custom_extensions)
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Relationship):
            return False
        return (
            self._name == other.name()
            and self._from_dataset == other.from_dataset()
            and self._to_dataset == other.to_dataset()
            and self._from_columns == other.from_columns()
            and self._to_columns == other.to_columns()
            and self._ai_context == other.ai_context()
            and self._custom_extensions == other.custom_extensions()
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                self._from_dataset,
                self._to_dataset,
                tuple(self._from_columns),
                tuple(self._to_columns),
                self._ai_context,
                (
                    None
                    if self._custom_extensions is None
                    else tuple(self._custom_extensions)
                ),
            )
        )

    def __repr__(self) -> str:
        return (
            f"Relationship(name={self._name!r}, from={self._from_dataset!r}, "
            f"to={self._to_dataset!r}, fromColumns={self._from_columns!r}, "
            f"toColumns={self._to_columns!r}, aiContext={self._ai_context!r}, "
            f"customExtensions={self._custom_extensions!r})"
        )
