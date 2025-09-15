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
from dataclasses import dataclass, field

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.expressions.distributions.strategy import Strategy
from gravitino.dto.rel.distribution_dto import DistributionDTO
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.dto.rel.json_serdes.distribution_serdes import DistributionSerDes
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class MockDataClass(DataClassJsonMixin):
    distribution: DistributionDTO = field(
        metadata=config(
            encoder=DistributionSerDes.serialize,
            decoder=DistributionSerDes.deserialize,
        )
    )


class TestDistributionSerdes(unittest.TestCase):
    def test_distribution_serdes_invalid_json(self):
        json_strings = [
            '{"distribution": {}}',
            '{"distribution": ""}',
        ]
        for json_string in json_strings:
            with self.assertRaisesRegex(
                IllegalArgumentException, "Cannot parse distribution from invalid JSON"
            ):
                MockDataClass.from_json(json_string)

    def test_distribution_serdes(self):
        json_string = """
            {
                "distribution": {
                    "strategy": "even",
                    "number": 32,
                    "funcArgs": [
                        {
                            "type": "field",
                            "fieldName": ["id"]
                        }
                    ]
                }
            }
        """
        mock_data_class = MockDataClass.from_json(json_string)
        distribution = mock_data_class.distribution
        self.assertIs(Strategy.EVEN, distribution.strategy())
        self.assertEqual(32, distribution.number())
        self.assertListEqual(
            distribution.args(),
            [
                SerdesUtils.read_function_arg(arg)
                for arg in json.loads(json_string)["distribution"][
                    DistributionSerDes.FUNCTION_ARGS
                ]
            ],
        )

        serialized = mock_data_class.to_json()
        self.assertDictEqual(json.loads(json_string), json.loads(serialized))

    def test_distribution_serdes_without_strategy(self):
        json_string = """
            {
                "distribution": {
                    "number": 4,
                    "funcArgs": [
                        {
                            "type": "field",
                            "fieldName": ["id"]
                        }
                    ]
                }
            }
        """
        mock_data_class = MockDataClass.from_json(json_string)
        serialized_dict = json.loads(mock_data_class.to_json())

        distribution_dto = mock_data_class.distribution
        distribution_dict = serialized_dict["distribution"]
        self.assertIs(Strategy.HASH, distribution_dto.strategy())
        self.assertEqual(
            distribution_dict[DistributionSerDes.NUMBER], distribution_dto.number()
        )
        self.assertListEqual(
            distribution_dto.args(),
            [
                SerdesUtils.read_function_arg(arg)
                for arg in distribution_dict[DistributionSerDes.FUNCTION_ARGS]
            ],
        )
