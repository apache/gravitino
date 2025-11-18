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

from gravitino.dto.rel.partitions.partition_dto import PartitionDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class AddPartitionsRequest(RESTRequest):
    """Request to add partitions to a table."""

    _partitions: list[PartitionDTO] = field(metadata=config(field_name="partitions"))

    def validate(self):
        Precondition.check_argument(
            self._partitions is not None, "partitions must not be null"
        )
        Precondition.check_argument(
            len(self._partitions) == 1, "Haven't yet implemented multiple partitions"
        )
