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
from typing import Optional, Dict, List

from gravitino.api.model.model_version import ModelVersion
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.model_version_dto import ModelVersionDTO


class GenericModelVersion(ModelVersion):

    _model_version_dto: ModelVersionDTO
    """The model version DTO object."""

    def __init__(self, model_version_dto: ModelVersionDTO):
        self._model_version_dto = model_version_dto

    def version(self) -> int:
        return self._model_version_dto.version()

    def comment(self) -> Optional[str]:
        return self._model_version_dto.comment()

    def aliases(self) -> List[str]:
        return self._model_version_dto.aliases()

    def uri(self) -> str:
        return self._model_version_dto.uri()

    def properties(self) -> Dict[str, str]:
        return self._model_version_dto.properties()

    def audit_info(self) -> AuditDTO:
        return self._model_version_dto.audit_info()
