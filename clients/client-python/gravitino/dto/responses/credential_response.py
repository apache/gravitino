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

from typing import List
from dataclasses import dataclass, field
from dataclasses_json import config

from gravitino.dto.credential_dto import CredentialDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class CredentialResponse(BaseResponse):
    """Response for credential response."""

    _credentials: List[CredentialDTO] = field(metadata=config(field_name="credentials"))

    def credentials(self) -> List[CredentialDTO]:
        return self._credentials

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException if credentials are None.
        """
        if self._credentials is None:
            raise IllegalArgumentException("credentials should be set")
        super().validate()
