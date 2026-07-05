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

from gravitino.api.audit import Audit
from gravitino.api.rel.column import Column
from gravitino.api.rel.representation import Representation
from gravitino.api.rel.view import View
from gravitino.dto.rel.view_dto import ViewDTO


class GenericView(View):
    """A generic implementation of the View interface."""

    def __init__(
        self,
        view_dto: ViewDTO,
    ):
        """Create a GenericView from a ViewDTO.

        Args:
            view_dto: The view DTO.
        """
        self._view_dto = view_dto

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, GenericView):
            return False

        return self._view_dto == value._view_dto

    def __hash__(self) -> int:
        return hash(self._view_dto)

    def name(self) -> str:
        return self._view_dto.name()

    def comment(self) -> Optional[str]:
        return self._view_dto.comment()

    def columns(self) -> list[Column]:
        return self._view_dto.columns()

    def representations(self) -> list[Representation]:
        return self._view_dto.representations()

    def default_catalog(self) -> Optional[str]:
        return self._view_dto.default_catalog()

    def default_schema(self) -> Optional[str]:
        return self._view_dto.default_schema()

    def properties(self) -> dict[str, str]:
        return self._view_dto.properties()

    def audit_info(self) -> Audit:
        return self._view_dto.audit_info()
