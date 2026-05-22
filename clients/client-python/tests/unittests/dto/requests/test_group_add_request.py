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
from __future__ import annotations

import json as _json
import unittest

from gravitino.dto.requests.group_add_request import GroupAddRequest


class TestGroupAddRequest(unittest.TestCase):
    def test_group_add_request_serde(self) -> None:
        group_add_request = GroupAddRequest(_name="group1")
        ser_json = _json.dumps(group_add_request.to_dict())
        deser_dict = _json.loads(ser_json)

        self.assertEqual("group1", deser_dict["name"])

    def test_group_add_request_validate(self) -> None:
        group_add_request = GroupAddRequest(_name="group1")
        group_add_request.validate()

        with self.assertRaises(ValueError):
            GroupAddRequest(_name="").validate()
