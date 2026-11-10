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

from dataclasses_json import config

from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class BaseListResponse(BaseResponse):
    """Base class of the list responses. A list response can carry one page of a list.

    ``nextPageToken`` is serialized only when more entries remain, so an unpaged response is
    unchanged on the wire.
    """

    _next_page_token: Optional[str] = field(
        default=None,
        kw_only=True,
        metadata=config(
            field_name="nextPageToken", exclude=lambda value: value is None
        ),
    )

    def next_page_token(self) -> Optional[str]:
        """Returns the token for the next page, or None when no further entry remains."""
        return self._next_page_token
