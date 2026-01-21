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

from typing import List, Optional

from gravitino.api.function.function import Function
from gravitino.api.function.function_definition import FunctionDefinition
from gravitino.api.function.function_type import FunctionType
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.function.function_dto import FunctionDTO


class GenericFunction(Function):
    """A generic implementation of the Function interface."""

    def __init__(self, function_dto: FunctionDTO):
        """Create a GenericFunction from a FunctionDTO.

        Args:
            function_dto: The function DTO.
        """
        self._function_dto = function_dto

    def name(self) -> str:
        """Returns the function name."""
        return self._function_dto.name()

    def function_type(self) -> FunctionType:
        """Returns the function type."""
        return self._function_dto.function_type()

    def deterministic(self) -> bool:
        """Returns whether the function is deterministic."""
        return self._function_dto.deterministic()

    def comment(self) -> Optional[str]:
        """Returns the optional comment of the function."""
        return self._function_dto.comment()

    def definitions(self) -> List[FunctionDefinition]:
        """Returns the definitions of the function."""
        return self._function_dto.definitions()

    def audit_info(self) -> Optional[AuditDTO]:
        """Returns the audit information."""
        return self._function_dto.audit_info()
