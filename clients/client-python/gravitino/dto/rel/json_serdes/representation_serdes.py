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

from gravitino.api.rel.representation import Representation
from gravitino.dto.rel.representation_dto import RepresentationDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.exceptions.base import IllegalArgumentException


class RepresentationSerdes:
    """Serdes for view representation DTOs."""

    @staticmethod
    def deserialize(value: dict) -> RepresentationDTO:
        """Decode a representation DTO from a dictionary."""
        if value is None:
            return None
        if value.get("type") == Representation.TYPE_SQL:
            return SQLRepresentationDTO.from_dict(value)
        raise IllegalArgumentException(
            f"Unsupported representation type: {value.get('type')}"
        )

    @staticmethod
    def serialize(value: RepresentationDTO) -> dict:
        """Encode a representation DTO to a dictionary."""
        if value is None:
            return None
        return value.to_dict()
