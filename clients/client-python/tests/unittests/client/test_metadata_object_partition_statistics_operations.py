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

import io
import json
import unittest
from http.client import HTTPResponse
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlsplit

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.stats.partition_range import PartitionRange
from gravitino.api.stats.partition_statistics_drop import PartitionStatisticsDrop
from gravitino.api.stats.partition_statistics_update import PartitionStatisticsUpdate
from gravitino.api.stats.statistic_values import StatisticValues
from gravitino.client.metadata_object_partition_statistics_operations import (
    MetadataObjectPartitionStatisticsOperations,
)
from gravitino.dto.requests.partition_statistics_drop_request import (
    PartitionStatisticsDropRequest,
)
from gravitino.dto.requests.partition_statistics_update_request import (
    PartitionStatisticsUpdateRequest,
)
from gravitino.exceptions.base import (
    IllegalArgumentException,
    IllegalStatisticNameException,
    NoSuchMetadataObjectException,
    UnmodifiableStatisticException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.statistics_error_handler import (
    STATISTICS_ERROR_HANDLER,
)
from gravitino.utils import HTTPClient
from tests.unittests.mock_base import mock_http_response


class TestMetadataObjectPartitionStatisticsOperations(unittest.TestCase):
    def setUp(self):
        self.rest = Mock(spec=HTTPClient)
        self.obj = MetadataObjects.of(
            ["catalog", "schema", "table"], MetadataObject.Type.TABLE
        )
        self.ops = MetadataObjectPartitionStatisticsOperations(
            "metalake", self.obj, self.rest
        )
        self.update = PartitionStatisticsUpdate(
            "p1", {"custom-x": StatisticValues.long_value(1)}
        )
        self.drop = PartitionStatisticsDrop("p1", ["custom-x"])

    def test_all_range_query_combinations(self):
        bound = PartitionRange.BoundType
        cases = [
            (PartitionRange.ALL_PARTITIONS, {}),
            (
                PartitionRange.down_to("p1", bound.CLOSED),
                {"from": "p1", "fromInclusive": "true"},
            ),
            (
                PartitionRange.up_to("p3", bound.OPEN),
                {"to": "p3", "toInclusive": "false"},
            ),
        ]
        for low in bound:
            for high in bound:
                cases.append(
                    (
                        PartitionRange.between("p1", low, "p3", high),
                        {
                            "from": "p1",
                            "fromInclusive": str(low == bound.CLOSED).lower(),
                            "to": "p3",
                            "toInclusive": str(high == bound.CLOSED).lower(),
                        },
                    )
                )
        self.rest.get.return_value = mock_http_response(
            '{"code":0,"partitionStatistics":[]}'
        )
        for partition_range, params in cases:
            with self.subTest(params=params):
                self.rest.get.reset_mock()
                self.assertEqual(
                    [], self.ops.list_partition_statistics(partition_range)
                )
                self.rest.get.assert_called_once_with(
                    endpoint="api/metalakes/metalake/objects/table/catalog.schema.table/statistics/partitions",
                    params=params,
                    error_handler=STATISTICS_ERROR_HANDLER,
                )

    def test_update_and_drop_use_single_batch_requests(self):
        self.rest.put.return_value = mock_http_response('{"code":0}')
        self.assertIsNone(
            self.ops.update_partition_statistics([self.update, self.update])
        )
        self.rest.put.assert_called_once_with(
            endpoint=self.ops.request_path,
            json=PartitionStatisticsUpdateRequest([self.update, self.update]),
            error_handler=STATISTICS_ERROR_HANDLER,
        )
        for dropped in [True, False]:
            with self.subTest(dropped=dropped):
                self.rest.post.reset_mock()
                self.rest.post.return_value = mock_http_response(
                    json.dumps({"code": 0, "dropped": dropped})
                )
                self.assertIs(dropped, self.ops.drop_partition_statistics([self.drop]))
                self.rest.post.assert_called_once_with(
                    endpoint=self.ops.request_path,
                    json=PartitionStatisticsDropRequest([self.drop]),
                    error_handler=STATISTICS_ERROR_HANDLER,
                )

    def test_invalid_input_never_sends_a_request(self):
        with self.assertRaises(IllegalArgumentException):
            self.ops.list_partition_statistics(None)
        for method in [
            self.ops.update_partition_statistics,
            self.ops.drop_partition_statistics,
        ]:
            for values in [None, [], [None]]:
                with self.subTest(method=method, values=values), self.assertRaises(
                    IllegalArgumentException
                ):
                    method(values)
        self.rest.get.assert_not_called()
        self.rest.put.assert_not_called()
        self.rest.post.assert_not_called()

    def test_missing_drop_flag_is_not_success(self):
        self.rest.post.return_value = mock_http_response('{"code":0}')
        with self.assertRaises(IllegalArgumentException):
            self.ops.drop_partition_statistics([self.drop])

    def test_real_http_layer_preserves_names_and_query_flags(self):
        obj = Mock(spec=MetadataObject)
        obj.type.return_value = MetadataObject.Type.TABLE
        obj.full_name.return_value = "目录/一.schema.table?#+"
        ops = MetadataObjectPartitionStatisticsOperations(
            "湖/甲", obj, HTTPClient("http://localhost:8090")
        )
        raw = Mock(spec=HTTPResponse)
        raw.getcode.return_value = 200
        raw.read.return_value = b'{"code":0,"partitionStatistics":[]}'
        raw.info.return_value = None
        raw.url = None
        with patch("gravitino.utils.http_client.build_opener") as build:
            build.return_value.open.return_value = raw
            ops.list_partition_statistics(
                PartitionRange.down_to(
                    "dt=甲/region=a+b?x#", PartitionRange.BoundType.OPEN
                )
            )
            request = build.return_value.open.call_args.args[0]
        path = urlsplit(request.full_url)
        self.assertIn(quote("湖/甲", safe=""), path.path)
        self.assertIn(quote(obj.full_name(), safe=""), path.path)
        self.assertEqual(
            {"from": ["dt=甲/region=a+b?x#"], "fromInclusive": ["false"]},
            parse_qs(path.query),
        )
        self.assertEqual("GET", request.get_method().upper())

    def test_http_errors_map_business_code_and_type_for_all_operations(self):
        ops = MetadataObjectPartitionStatisticsOperations(
            "metalake", self.obj, HTTPClient("http://localhost:8090")
        )
        actions = {
            "list": (ops.list_partition_statistics, PartitionRange.ALL_PARTITIONS),
            "update": (ops.update_partition_statistics, [self.update]),
            "drop": (ops.drop_partition_statistics, [self.drop]),
        }
        for action, status, code, exception in [
            ("list", 404, 1003, NoSuchMetadataObjectException),
            ("update", 400, 1001, IllegalStatisticNameException),
            ("drop", 409, 1006, UnmodifiableStatisticException),
            ("list", 501, 1006, UnsupportedOperationException),
        ]:
            with self.subTest(action=action, status=status):
                method, args = actions[action]
                body = json.dumps(
                    {
                        "code": code,
                        "type": exception.__name__,
                        "message": "failure",
                        "stack": [],
                    }
                ).encode()
                error = HTTPError(
                    "http://localhost:8090", status, "failure", {}, io.BytesIO(body)
                )
                with patch("gravitino.utils.http_client.build_opener") as build:
                    build.return_value.open.side_effect = error
                    with self.assertRaises(exception):
                        method(args)
                    build.return_value.open.assert_called_once()
