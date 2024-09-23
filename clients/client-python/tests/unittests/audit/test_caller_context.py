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
from typing import Dict

from gravitino.audit.caller_context import CallerContextHolder, CallerContext


class TestCallerContext(unittest.TestCase):

    def test_caller_context(self):
        try:
            context: Dict = {"k1": "v1", "k2": "v2"}
            caller_context: CallerContext = CallerContext(context)
            CallerContextHolder.set(caller_context)
            self.assertEqual(CallerContextHolder.get().context()["k1"], context["k1"])
            self.assertEqual(CallerContextHolder.get().context()["k2"], context["k2"])
        finally:
            CallerContextHolder.remove()

        self.assertIsNone(CallerContextHolder.get())
