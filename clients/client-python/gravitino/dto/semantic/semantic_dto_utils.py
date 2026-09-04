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

"""Shared conversion helpers for the Semantic Model DTOs."""

from typing import Any, Callable, Optional


def is_none(value: Any) -> bool:
    """Predicate that omits unset optional fields during serialization.

    Args:
        value (Any): The field value.

    Returns:
        bool: `True` if the field is unset and must not be serialized.
    """
    return value is None


def convert_list(values: Optional[list], converter: Callable) -> Optional[list]:
    """Convert an optional list, preserving `None` for the list and its elements.

    Args:
        values (list, optional): The list to convert, `None` is allowed.
        converter (Callable): The element converter.

    Returns:
        list, optional: The converted list, or `None` if the input is `None`.
    """
    if values is None:
        return None
    return [None if value is None else converter(value) for value in values]
