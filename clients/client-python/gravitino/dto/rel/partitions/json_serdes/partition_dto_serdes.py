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

from typing import Any, Dict

from gravitino.api.types.json_serdes.base import JsonSerializable
from gravitino.dto.rel.partitions.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO


class PartitionDTOSerdes(JsonSerializable[PartitionDTO]):
    @classmethod
    def serialize(cls, data_type: PartitionDTO) -> Dict[str, Any]:
        return SerdesUtils.write_partition(data_type)

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> PartitionDTO:
        return SerdesUtils.read_partition(data)
