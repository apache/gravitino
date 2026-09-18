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

"""Shared validation helpers for the Semantic Model value types."""

from typing import Optional, Sequence

from gravitino.utils.precondition import Precondition


def check_no_none_elements(name: str, values: Optional[Sequence]) -> None:
    """Check that an optional sequence does not contain `None` elements.

    Args:
        name (str): The name reported in the error message.
        values (Sequence, optional): The sequence to check, `None` is allowed.

    Raises:
        IllegalArgumentException: If any element is `None`.
    """
    if values is None:
        return
    for index, value in enumerate(values):
        Precondition.check_argument(
            value is not None, f"{name}[{index}] must not be null"
        )


def check_non_empty_string_elements(name: str, values: Optional[Sequence[str]]) -> None:
    """Check that an optional sequence only contains non-empty strings.

    Args:
        name (str): The name reported in the error message.
        values (Sequence[str], optional): The sequence to check, `None` is allowed.

    Raises:
        IllegalArgumentException: If any element is `None` or empty.
    """
    if values is None:
        return
    for index, value in enumerate(values):
        Precondition.check_argument(
            value is not None and value != "",
            f"{name}[{index}] must not be null or empty",
        )
