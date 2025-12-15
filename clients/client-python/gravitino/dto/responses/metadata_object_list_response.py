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

from __future__ import annotations

from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.dto.metadata_object_dto import MetadataObjectDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.utils.precondition import Precondition


@dataclass
class MetadataObjectListResponse(BaseResponse):
    """Represents a response containing a list of metadata objects."""

    _metadata_objects: list[MetadataObjectDTO] = field(
        metadata=config(field_name="metadataObjects")
    )

    def metadata_objects(self) -> list[MetadataObjectDTO]:
        """
        Retrieve the list of metadata objects.

        Returns:
            list[MetadataObjectDTO]: The list of metadata objects.
        """
        return self._metadata_objects

    def validate(self) -> None:
        """
        Validates the response data.
        """
        super().validate()

        for metadata_object in self._metadata_objects:
            Precondition.check_argument(
                metadata_object is not None
                and metadata_object.type() is not None
                and (name := metadata_object.name()) is not None
                and name.strip() != "",
                "metadataObject must not be null and it's field cannot null or empty",
            )
