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
from typing import Generic, Optional, TypeVar

from gravitino.exceptions.base import IllegalArgumentException

T = TypeVar("T")

PAGE_SIZE = "pageSize"
PAGE_TOKEN = "pageToken"


@dataclass(frozen=True)
class Page(Generic[T]):
    """One page of a list.

    Attributes:
        items: The entries in this page, in the list's stable order.
        next_page_token: The token for the next page, or None when no further entry remains.
    """

    items: list[T] = field(default_factory=list)
    next_page_token: Optional[str] = None


def page_params(page_size: int, page_token: Optional[str] = None) -> dict[str, str]:
    """Builds the query parameters of a paged list request.

    Args:
        page_size: The maximum number of entries in the page; must be at least 1.
        page_token: The ``next_page_token`` of the previous page, or None for the first page.

    Returns:
        dict[str, str]: The query parameters.

    Raises:
        IllegalArgumentException: If ``page_size`` is below 1 or ``page_token`` is empty.
    """
    if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size < 1:
        raise IllegalArgumentException(f"pageSize must be at least 1, got: {page_size}")
    params = {PAGE_SIZE: str(page_size)}
    if page_token is not None:
        if page_token == "":
            raise IllegalArgumentException("pageToken must not be empty")
        params[PAGE_TOKEN] = page_token
    return params
