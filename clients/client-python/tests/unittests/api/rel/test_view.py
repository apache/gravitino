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
from unittest.mock import Mock

from gravitino.api.rel.dialects import Dialects
from gravitino.api.rel.representation import Representation
from gravitino.api.rel.sql_representation import SQLRepresentation
from gravitino.api.rel.view import View
from gravitino.exceptions.base import UnsupportedOperationException


class TestView(unittest.TestCase):
    def test_sql_for_returns_matching_representation(self):
        trino_representation = SQLRepresentation(Dialects.TRINO, "SELECT 1")
        spark_representation = SQLRepresentation(Dialects.SPARK, "SELECT 2")
        view = Mock(spec=View)
        view.representations.return_value = [
            trino_representation,
            spark_representation,
        ]

        self.assertEqual(
            trino_representation,
            View.sql_for(view, Dialects.TRINO.upper()),
        )

    def test_sql_for_returns_none_when_dialect_is_missing(self):
        view = Mock(spec=View)
        view.representations.return_value = [
            SQLRepresentation(Dialects.SPARK, "SELECT 1")
        ]

        self.assertIsNone(View.sql_for(view, Dialects.TRINO))

    def test_sql_for_returns_none_when_dialect_is_none(self):
        view = Mock(spec=View)

        self.assertIsNone(View.sql_for(view, None))
        view.representations.assert_not_called()

    def test_sql_for_ignores_non_sql_representations(self):
        representation = Mock(spec=Representation)
        view = Mock(spec=View)
        view.representations.return_value = [representation]

        self.assertIsNone(View.sql_for(view, Dialects.TRINO))

    def test_default_values(self):
        view = Mock(spec=View)

        self.assertIsNone(View.comment(view))
        self.assertIsNone(View.default_catalog(view))
        self.assertIsNone(View.default_schema(view))
        self.assertEqual({}, View.properties(view))

    def test_supports_tags_raises_exception(self):
        view = Mock(spec=View)

        with self.assertRaises(UnsupportedOperationException):
            View.supports_tags(view)
