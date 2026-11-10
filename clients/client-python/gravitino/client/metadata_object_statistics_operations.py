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

import logging
from typing import Any

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.stats.statistic import Statistic
from gravitino.api.stats.statistic_value import StatisticValue
from gravitino.api.stats.supports_statistics import SupportsStatistics
from gravitino.dto.requests.statistics_drop_request import StatisticsDropRequest
from gravitino.dto.requests.statistics_update_request import StatisticsUpdateRequest
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.statistic_list_response import StatisticListResponse
from gravitino.exceptions.handlers.statistics_error_handler import (
    STATISTICS_ERROR_HANDLER,
)
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient
from gravitino.utils.precondition import Precondition

logger = logging.getLogger(__name__)


class MetadataObjectStatisticsOperations(SupportsStatistics):
    """The implementation of SupportsStatistics.

    This interface will be composited into table to provide statistics operations
    for metadata objects.
    """

    def __init__(
        self,
        metalake_name: str,
        metadata_object: MetadataObject,
        rest_client: HTTPClient,
    ):
        metadata_object_type = metadata_object.type().value
        metadata_object_fullname = metadata_object.full_name()
        self._rest_client = rest_client
        self.request_path = (
            f"api/metalakes/{encode_string(metalake_name)}/"
            f"objects/{metadata_object_type}/{encode_string(metadata_object_fullname)}/"
            "statistics"
        )

    def list_statistics(self) -> list[Statistic]:
        resp = self._rest_client.get(
            endpoint=self.request_path,
            error_handler=STATISTICS_ERROR_HANDLER,
        )

        statistic_resp = StatisticListResponse.from_json(resp.body, infer_missing=True)
        statistic_resp.validate()

        return statistic_resp.statistics

    def update_statistics(self, statistics: dict[str, StatisticValue[Any]]) -> None:
        Precondition.check_argument(
            statistics is not None and len(statistics) > 0,
            "Statistics map must not be null or empty",
        )
        req = StatisticsUpdateRequest(_updates=statistics)
        req.validate()
        resp = self._rest_client.put(
            endpoint=self.request_path,
            json=req,
            error_handler=STATISTICS_ERROR_HANDLER,
        )
        BaseResponse.from_json(resp.body, infer_missing=True).validate()

    def drop_statistics(self, statistics: list[str]) -> bool:
        Precondition.check_argument(
            statistics is not None and len(statistics) > 0,
            "Statistics list must not be null or empty",
        )
        req = StatisticsDropRequest(_names=statistics)
        req.validate()
        resp = self._rest_client.post(
            endpoint=self.request_path,
            json=req,
            error_handler=STATISTICS_ERROR_HANDLER,
        )
        drop_response = DropResponse.from_json(resp.body, infer_missing=True)
        drop_response.validate()

        return drop_response.dropped()
