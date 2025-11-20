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

from dataclasses_json import config

from gravitino.dto.rel.partitions.json_serdes.partition_dto_serdes import (
    PartitionDTOSerdes,
)
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO
from gravitino.dto.responses.base_response import BaseResponse


@dataclass
class PartitionResponse(BaseResponse):
    """Represents a response for a partition."""

    _partition: PartitionDTO = field(
        metadata=config(
            field_name="partition",
            decoder=PartitionDTOSerdes.deserialize,
            encoder=PartitionDTOSerdes.serialize,
        )
    )
