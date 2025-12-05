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

import json
import unittest
from typing import cast

from gravitino.dto.requests.add_partitions_request import AddPartitionsRequest
from gravitino.exceptions.base import IllegalArgumentException


class TestRequests(unittest.TestCase):
    def test_add_partitions_request(self):
        partitions = ["p202508_California"]
        json_str = json.dumps({"partitions": partitions})
        req = AddPartitionsRequest.from_json(json_str)
        req_dict = cast(dict, req.to_dict())
        self.assertListEqual(req_dict["partitions"], partitions)

        exceptions = {
            "partitions must not be null": '{"partitions": null}',
            "Haven't yet implemented multiple partitions": '{"partitions": ["p1", "p2"]}',
        }
        for exception_str, json_str in exceptions.items():
            with self.assertRaisesRegex(IllegalArgumentException, exception_str):
                req = AddPartitionsRequest.from_json(json_str)
                req.validate()
