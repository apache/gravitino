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

# pylint: disable=protected-access,too-many-lines,too-many-locals

import unittest

from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


class TestPrecondition(unittest.TestCase):

    def test_check_argument(self):
        with self.assertRaises(IllegalArgumentException):
            Precondition.check_argument(False, "error")
        try:
            Precondition.check_argument(True, "error")
        except IllegalArgumentException:
            self.fail("should not raise IllegalArgumentException")

    def test_check_string_empty(self):
        with self.assertRaises(IllegalArgumentException):
            Precondition.check_string_not_empty("", "empty")
        with self.assertRaises(IllegalArgumentException):
            Precondition.check_string_not_empty(" ", "empty")
        with self.assertRaises(IllegalArgumentException):
            Precondition.check_string_not_empty(None, "empty")
        try:
            Precondition.check_string_not_empty("test", "empty")
        except IllegalArgumentException:
            self.fail("should not raised an exception")
