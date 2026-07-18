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

from gravitino.api.rel.dialects import Dialects
from gravitino.api.rel.representation import Representation
from gravitino.dto.rel.json_serdes.representation_serdes import RepresentationSerdes
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.exceptions.base import IllegalArgumentException


class TestRepresentationSerdes(unittest.TestCase):
    def test_representation_serdes(self):
        representation = SQLRepresentationDTO(
            _dialect=Dialects.TRINO, _sql="SELECT id FROM table"
        )
        serialized = RepresentationSerdes.serialize(representation)

        self.assertEqual(Representation.TYPE_SQL, serialized["type"])
        self.assertEqual(Dialects.TRINO, serialized["dialect"])
        self.assertEqual("SELECT id FROM table", serialized["sql"])
        self.assertEqual(representation, RepresentationSerdes.deserialize(serialized))

    def test_representation_serdes_none(self):
        self.assertIsNone(RepresentationSerdes.serialize(None))
        self.assertIsNone(RepresentationSerdes.deserialize(None))

    def test_representation_serdes_unknown_type(self):
        with self.assertRaises(IllegalArgumentException):
            RepresentationSerdes.deserialize({"type": "unknown"})
