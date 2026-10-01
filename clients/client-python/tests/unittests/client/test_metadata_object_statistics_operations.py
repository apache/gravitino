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

import unittest
from unittest.mock import patch

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.stats.statistic_values import StatisticValues
from gravitino.client.metadata_object_statistics_operations import (
    MetadataObjectStatisticsOperations,
)
from gravitino.constants.error import ErrorConstants
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.requests.statistics_drop_request import StatisticsDropRequest
from gravitino.dto.requests.statistics_update_request import StatisticsUpdateRequest
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.dto.responses.statistic_list_response import StatisticListResponse
from gravitino.dto.stats.statistic_dto import StatisticDTO
from gravitino.exceptions.base import (
    IllegalArgumentException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.statistics_error_handler import (
    STATISTICS_ERROR_HANDLER,
)
from gravitino.utils import HTTPClient
from tests.unittests import mock_base


class TestMetadataObjectStatisticsOperations(unittest.TestCase):
    REST_CLIENT = HTTPClient("http://localhost:8090")
    METALAKE_NAME = "demo_metalake"

    def test_list_statistics(self) -> None:
        stats_op = MetadataObjectStatisticsOperations(
            self.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"], MetadataObject.Type.TABLE
            ),
            self.REST_CLIENT,
        )
        statistic = StatisticDTO(
            _name="rowCount",
            _reserved=False,
            _modifiable=True,
            _audit=AuditDTO(_creator="test_creator"),
            _value=StatisticValues.long_value(100),
        )
        resp_body = StatisticListResponse(0, [statistic]).to_json()
        mock_resp = mock_base.mock_http_response(resp_body)
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            result = stats_op.list_statistics()
            self.assertEqual([statistic], result)
            mock_get.assert_called_once_with(
                endpoint=stats_op.request_path,
                error_handler=STATISTICS_ERROR_HANDLER,
            )

    def test_update_statistics(self) -> None:
        stats_op = MetadataObjectStatisticsOperations(
            self.METALAKE_NAME,
            MetadataObjects.of(["catalog", "schema"], MetadataObject.Type.SCHEMA),
            self.REST_CLIENT,
        )
        stats_map = {"rowCount": StatisticValues.long_value(200)}
        mock_resp = mock_base.mock_http_response(BaseResponse(0).to_json())
        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            return_value=mock_resp,
        ) as mock_put:
            stats_op.update_statistics(stats_map)
            req = StatisticsUpdateRequest(_updates=stats_map)
            mock_put.assert_called_once_with(
                endpoint=stats_op.request_path,
                json=req,
                error_handler=STATISTICS_ERROR_HANDLER,
            )

    def test_merge_statistics(self) -> None:
        stats_op = MetadataObjectStatisticsOperations(
            self.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"], MetadataObject.Type.TABLE
            ),
            self.REST_CLIENT,
        )
        stats_map = {
            "custom-by-spec": StatisticValues.object_value(
                {"1": StatisticValues.long_value(200)}
            )
        }
        mock_resp = mock_base.mock_http_response(BaseResponse(0).to_json())
        with patch.object(
            self.REST_CLIENT, "patch", return_value=mock_resp
        ) as mock_patch:
            stats_op.merge_statistics(stats_map)
            mock_patch.assert_called_once_with(
                endpoint=stats_op.request_path,
                json=StatisticsUpdateRequest(_updates=stats_map),
                error_handler=STATISTICS_ERROR_HANDLER,
            )

    def test_merge_statistics_rejects_invalid_requests_before_sending(self) -> None:
        stats_op = MetadataObjectStatisticsOperations(
            self.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"], MetadataObject.Type.TABLE
            ),
            self.REST_CLIENT,
        )
        for invalid in (
            None,
            {},
            {"": StatisticValues.object_value({"1": StatisticValues.long_value(0)})},
            {"custom": None},
        ):
            with self.subTest(statistics=invalid), patch.object(
                self.REST_CLIENT, "patch"
            ) as mock_patch:
                with self.assertRaises(IllegalArgumentException):
                    stats_op.merge_statistics(invalid)
                mock_patch.assert_not_called()

    def test_merge_statistics_validates_response(self) -> None:
        stats_op = MetadataObjectStatisticsOperations(
            self.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"], MetadataObject.Type.TABLE
            ),
            self.REST_CLIENT,
        )
        mock_resp = mock_base.mock_http_response(BaseResponse(-1).to_json())
        with patch.object(self.REST_CLIENT, "patch", return_value=mock_resp):
            with self.assertRaises(IllegalArgumentException):
                stats_op.merge_statistics(
                    {
                        "custom": StatisticValues.object_value(
                            {"1": StatisticValues.long_value(0)}
                        )
                    }
                )

    def test_merge_statistics_propagates_unsupported_server_without_fallback(
        self,
    ) -> None:
        stats_op = MetadataObjectStatisticsOperations(
            self.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"], MetadataObject.Type.TABLE
            ),
            self.REST_CLIENT,
        )
        response = ErrorResponse(
            ErrorConstants.UNSUPPORTED_OPERATION_CODE,
            "UnsupportedOperationException",
            "Atomic statistics merging is not supported",
            [],
        )
        with patch.object(
            self.REST_CLIENT, "_make_request", return_value=(False, response)
        ) as request:
            with self.assertRaises(UnsupportedOperationException):
                stats_op.merge_statistics(
                    {
                        "custom": StatisticValues.object_value(
                            {"1": StatisticValues.long_value(0)}
                        )
                    }
                )
            request.assert_called_once()
            self.assertEqual("PATCH", request.call_args.args[1].get_method())

    def test_drop_statistics(self) -> None:
        stats_op = MetadataObjectStatisticsOperations(
            self.METALAKE_NAME,
            MetadataObjects.of(["catalog"], MetadataObject.Type.CATALOG),
            self.REST_CLIENT,
        )
        names = ["rowCount", "size"]
        resp_body = DropResponse(_code=0, _dropped=True).to_json()
        mock_resp = mock_base.mock_http_response(resp_body)
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_resp,
        ) as mock_post:
            result = stats_op.drop_statistics(names)
            self.assertTrue(result)
            req = StatisticsDropRequest(_names=names)
            mock_post.assert_called_once_with(
                endpoint=stats_op.request_path,
                json=req,
                error_handler=STATISTICS_ERROR_HANDLER,
            )
