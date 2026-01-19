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

"""Client implementation for metadata object statistics operations."""

from typing import List, Dict

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.statistics.statistic import Statistic
from gravitino.api.statistics.statistic_value import StatisticValue
from gravitino.api.statistics.supports_statistics import SupportsStatistics
from gravitino.audit.caller_context import CallerContextHolder
from gravitino.dto.requests.statistics_update_request import StatisticsUpdateRequest
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.statistic_list_response import StatisticListResponse
from gravitino.dto.stats.statistic_value_dto import (
    StatisticValueDTO,
    BooleanValueDTO,
    LongValueDTO,
    DoubleValueDTO,
    StringValueDTO,
    ListValueDTO,
    ObjectValueDTO,
)
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.exceptions.handlers.statistics_error_handler import (
    STATISTICS_ERROR_HANDLER,
)
from gravitino.utils import HTTPClient
from gravitino.rest.rest_utils import encode_string


class MetadataObjectStatisticsOperations(SupportsStatistics):
    """
    The implementation of SupportsStatistics. This interface will be composited into table to
    provide statistics operations for table metadata objects.
    """

    _rest_client: HTTPClient
    _statistics_request_path: str

    def __init__(
        self,
        metalake_name: str,
        catalog_name: str,
        metadata_object: MetadataObject,
        rest_client: HTTPClient,
    ):
        self._rest_client = rest_client

        # Build the REST API path based on the metadata object type
        full_name = metadata_object.full_name()

        # Currently only TABLE type is supported
        if metadata_object.type() != MetadataObject.Type.TABLE:
            raise IllegalArgumentException(
                f"Statistics are only supported for TABLE object type, but got: {metadata_object.type()}"
            )

        # For table:
        # api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}/statistics
        parts = full_name.split(".")
        if len(parts) != 3:
            raise IllegalArgumentException(f"Invalid table full name: {full_name}")

        # fullName format is catalog.schema.table, we need schema and table
        self._statistics_request_path = (
            f"api/metalakes/{encode_string(metalake_name)}/catalogs/{encode_string(catalog_name)}/"
            f"schemas/{encode_string(parts[1])}/tables/{encode_string(parts[2])}/statistics"
        )

    def list_statistics(self) -> List[Statistic]:
        """Lists all statistics."""
        with CallerContextHolder.with_context(self._rest_client.get_context_map()):
            resp = self._rest_client.get(
                self._statistics_request_path,
                error_handler=STATISTICS_ERROR_HANDLER,
            )

            statistic_list_resp = StatisticListResponse.from_json(
                resp.body, infer_missing=True
            )
            statistic_list_resp.validate()
            return statistic_list_resp.statistics()

    def update_statistics(
        self, statistics: Dict[str, StatisticValue]
    ) -> List[Statistic]:
        """Updates statistics with the provided values."""
        if not statistics:
            raise IllegalArgumentException("Statistics must not be None or empty")

        # Convert StatisticValue to StatisticValueDTO
        statistic_dtos = {}
        for name, value in statistics.items():
            statistic_dtos[name] = self._to_statistic_value_dto(value)

        request = StatisticsUpdateRequest(statistics=statistic_dtos)
        request.validate()

        with CallerContextHolder.with_context(self._rest_client.get_context_map()):
            resp = self._rest_client.post(
                self._statistics_request_path,
                json=request.to_json(),
                error_handler=STATISTICS_ERROR_HANDLER,
            )

            statistic_list_resp = StatisticListResponse.from_json(
                resp.body, infer_missing=True
            )
            statistic_list_resp.validate()
            return statistic_list_resp.statistics()

    def drop_statistics(self, statistics: List[str]) -> bool:
        """Drop statistics by their names."""
        if not statistics:
            raise IllegalArgumentException("Statistics list must not be None or empty")

        # Convert the list of statistics to a query parameter
        query_params = {"statistics": ",".join(statistics)}

        with CallerContextHolder.with_context(self._rest_client.get_context_map()):
            resp = self._rest_client.delete(
                self._statistics_request_path,
                params=query_params,
                error_handler=STATISTICS_ERROR_HANDLER,
            )

            drop_resp = DropResponse.from_json(resp.body, infer_missing=True)
            drop_resp.validate()
            return drop_resp.dropped()

    def _to_statistic_value_dto(self, value: StatisticValue) -> StatisticValueDTO:
        """Convert StatisticValue to StatisticValueDTO."""
        data_type = value.data_type()

        if data_type == StatisticValue.Type.BOOLEAN:
            return BooleanValueDTO(value.value())
        if data_type == StatisticValue.Type.LONG:
            return LongValueDTO(value.value())
        if data_type == StatisticValue.Type.DOUBLE:
            return DoubleValueDTO(value.value())
        if data_type == StatisticValue.Type.STRING:
            return StringValueDTO(value.value())
        if data_type == StatisticValue.Type.LIST:
            items = [self._to_statistic_value_dto(item) for item in value.value()]
            return ListValueDTO(items)
        if data_type == StatisticValue.Type.OBJECT:
            obj = {k: self._to_statistic_value_dto(v) for k, v in value.value().items()}
            return ObjectValueDTO(obj)
        raise ValueError(f"Unknown statistic value type: {data_type}")
