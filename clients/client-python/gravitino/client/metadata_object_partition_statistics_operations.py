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

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.stats.partition_range import PartitionRange
from gravitino.api.stats.partition_statistics import PartitionStatistics
from gravitino.api.stats.partition_statistics_drop import PartitionStatisticsDrop
from gravitino.api.stats.partition_statistics_update import PartitionStatisticsUpdate
from gravitino.api.stats.supports_partition_statistics import (
    SupportsPartitionStatistics,
)
from gravitino.dto.requests.partition_statistics_drop_request import (
    PartitionStatisticsDropRequest,
)
from gravitino.dto.requests.partition_statistics_update_request import (
    PartitionStatisticsUpdateRequest,
)
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.partition_statistics_list_response import (
    PartitionStatisticsListResponse,
)
from gravitino.exceptions.handlers.statistics_error_handler import (
    STATISTICS_ERROR_HANDLER,
)
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient
from gravitino.utils.precondition import Precondition


class MetadataObjectPartitionStatisticsOperations(SupportsPartitionStatistics):
    """Bind the existing partition statistics REST contract to the Python API."""

    def __init__(
        self,
        metalake_name: str,
        metadata_object: MetadataObject,
        rest_client: HTTPClient,
    ):
        self._rest_client = rest_client
        self.request_path = (
            f"api/metalakes/{encode_string(metalake_name)}/"
            f"objects/{metadata_object.type().value}/"
            f"{encode_string(metadata_object.full_name())}/statistics/partitions"
        )

    def list_partition_statistics(
        self, partition_range: PartitionRange
    ) -> list[PartitionStatistics]:
        Precondition.check_argument(
            isinstance(partition_range, PartitionRange),
            "A PartitionRange is required",
        )
        params = {}
        for name, bound, key, inclusive in (
            (
                partition_range.lower_partition_name,
                partition_range.lower_bound_type,
                "from",
                "fromInclusive",
            ),
            (
                partition_range.upper_partition_name,
                partition_range.upper_bound_type,
                "to",
                "toInclusive",
            ),
        ):
            if name is not None:
                params[key] = name
                params[inclusive] = (
                    "true" if bound == PartitionRange.BoundType.CLOSED else "false"
                )
        response = self._rest_client.get(
            endpoint=self.request_path,
            params=params,
            error_handler=STATISTICS_ERROR_HANDLER,
        )
        result = PartitionStatisticsListResponse.from_json(
            response.body, infer_missing=True
        )
        result.validate()
        return list(result.partition_statistics)

    def update_partition_statistics(
        self, updates: list[PartitionStatisticsUpdate]
    ) -> None:
        request = PartitionStatisticsUpdateRequest(updates)
        request.validate()
        response = self._rest_client.put(
            endpoint=self.request_path,
            json=request,
            error_handler=STATISTICS_ERROR_HANDLER,
        )
        BaseResponse.from_json(response.body, infer_missing=True).validate()

    def drop_partition_statistics(self, drops: list[PartitionStatisticsDrop]) -> bool:
        request = PartitionStatisticsDropRequest(drops)
        request.validate()
        response = self._rest_client.post(
            endpoint=self.request_path,
            json=request,
            error_handler=STATISTICS_ERROR_HANDLER,
        )
        result = DropResponse.from_json(response.body, infer_missing=True)
        result.validate()
        Precondition.check_argument(
            isinstance(result.dropped(), bool), '"dropped" must be a boolean'
        )
        return result.dropped()
