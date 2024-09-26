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
import threading
import unittest
from typing import Dict

from gravitino.audit.caller_context import CallerContextHolder, CallerContext


class TestCallerContext(unittest.TestCase):

    def test_caller_context(self):
        thread_names_and_values = [
            ("Thread1", {"k1": "v1", "k2": "v2"}),
            ("Thread2", {"k11": "v11", "k21": "v21"}),
        ]
        test_threads = []
        for thread_name, value in thread_names_and_values:
            t = threading.Thread(
                target=self._set_thread_local_context, args=(thread_name, value)
            )
            t.start()
            test_threads.append(t)

        for t in test_threads:
            t.join()

    def _set_thread_local_context(self, thread_name, context: Dict[str, str]):
        caller_context: CallerContext = CallerContext(context)
        CallerContextHolder.set(caller_context)

        try:
            if thread_name == "Thread1":
                self.assertEqual(
                    CallerContextHolder.get().context()["k1"], context["k1"]
                )
                self.assertEqual(
                    CallerContextHolder.get().context()["k2"], context["k2"]
                )
            if thread_name == "Thread2":
                self.assertEqual(
                    CallerContextHolder.get().context()["k11"], context["k11"]
                )
                self.assertEqual(
                    CallerContextHolder.get().context()["k21"], context["k21"]
                )
        finally:
            CallerContextHolder.remove()

        self.assertIsNone(CallerContextHolder.get())

        # will not throw an exception if the context is not exists
        CallerContextHolder.remove()
