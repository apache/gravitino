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
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.parse import quote

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.stats.partition_range import PartitionRange
from gravitino.client.metadata_object_partition_statistics_operations import (
    MetadataObjectPartitionStatisticsOperations,
)
from gravitino.exceptions.base import (
    IllegalArgumentException,
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
        self.obj = Mock(spec=MetadataObject)
        self.obj.type.return_value = MetadataObject.Type.TABLE
        self.obj.full_name.return_value = "目录/一.schema.table?#+"
        self.ops = MetadataObjectPartitionStatisticsOperations(
            "湖/甲", self.obj, self.rest
        )
        self.path = (
            f"api/metalakes/{quote('湖/甲', safe='')}/objects/table/"
            f"{quote(self.obj.full_name(), safe='')}/statistics/partitions"
        )

    def test_range_query_parameters_and_encoded_endpoint(self):
        bound = PartitionRange.BoundType
        cases = [
            (PartitionRange.ALL_PARTITIONS, {}),
            (
                PartitionRange.down_to("dt=中/a+b?#", bound.CLOSED),
                {"from": "dt=中/a+b?#", "fromInclusive": "true"},
            ),
            (
                PartitionRange.up_to("p3", bound.OPEN),
                {"to": "p3", "toInclusive": "false"},
            ),
            (
                PartitionRange.between("p1", bound.OPEN, "p3", bound.CLOSED),
                {
                    "from": "p1",
                    "fromInclusive": "false",
                    "to": "p3",
                    "toInclusive": "true",
                },
            ),
        ]
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
                    endpoint=self.path,
                    params=params,
                    error_handler=STATISTICS_ERROR_HANDLER,
                )

    def test_invalid_range_never_sends_a_request(self):
        with self.assertRaises(IllegalArgumentException):
            self.ops.list_partition_statistics(None)
        self.rest.get.assert_not_called()

    def test_structured_unsupported_query_error(self):
        ops = MetadataObjectPartitionStatisticsOperations(
            "metalake", self.obj, HTTPClient("http://localhost:8090")
        )
        body = b'{"code":1006,"type":"UnsupportedOperationException","message":"failure","stack":[]}'
        error = HTTPError("http://localhost:8090", 501, "failure", {}, io.BytesIO(body))
        with patch("gravitino.utils.http_client.build_opener") as build:
            build.return_value.open.side_effect = error
            with self.assertRaises(UnsupportedOperationException):
                ops.list_partition_statistics(PartitionRange.ALL_PARTITIONS)
            build.return_value.open.assert_called_once()
