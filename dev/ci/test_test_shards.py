#!/usr/bin/env python3
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

"""Regression checks for CI task ownership and catch-all exclusions."""

import json
from pathlib import Path
import subprocess
import unittest


SCRIPT = Path(__file__).with_name("test-shards.sh")


def arguments(suite, shard):
    return subprocess.check_output(["bash", str(SCRIPT), suite, shard], text=True).splitlines()


class TestShardTasks(unittest.TestCase):
    def test_build_leaves_core_to_dedicated_lanes(self):
        shards = json.loads(subprocess.check_output(
            ["bash", str(SCRIPT), "build", "--list"], text=True))
        self.assertEqual(["docker", "others"], shards)
        for shard in shards:
            self.assertNotIn(":core:test", arguments("build", shard))
        # Every build shard receives the workflow's global :core:test exclusion.
        workflow = SCRIPT.parents[2] / ".github/workflows/build.yml"
        self.assertIn("-x :core:test", workflow.read_text())

    def test_each_named_task_has_exactly_one_owner_and_is_excluded_from_others(self):
        for suite, root in [("build", "build"), ("backend-it", "test")]:
            with self.subTest(suite=suite):
                shards = json.loads(subprocess.check_output(
                    ["bash", str(SCRIPT), suite, "--list"], text=True))
                self.assertEqual("others", shards[-1])
                tasks = [task for shard in shards[:-1] for task in arguments(suite, shard)]
                self.assertTrue(tasks)
                self.assertEqual(len(tasks), len(set(tasks)))
                others = arguments(suite, "others")
                self.assertEqual(root, others[0])
                self.assertEqual(["-x"] * len(tasks), others[1::2])
                self.assertEqual(set(tasks), set(others[2::2]))

    def test_backend_lakehouse_owns_experimental_iceberg_tests(self):
        task = ":iceberg:iceberg-rest-server:experimentalTest"
        self.assertIn(task, arguments("backend-it", "lakehouse"))
        self.assertNotIn(task, arguments("backend-it", "hive"))
        others = arguments("backend-it", "others")
        self.assertIn(task, others[2::2])
        self.assertTrue(all(not t.endswith(":experimentalTest")
                            for t in arguments("build", "docker")))

    def test_cron_preserves_the_complete_backend_suite(self):
        self.assertEqual(["test", ":iceberg:iceberg-rest-server:experimentalTest"],
                         arguments("backend-it", "all"))
        cron = SCRIPT.parents[2] / ".github/workflows/cron-integration-test.yml"
        self.assertIn("      shard: all", cron.read_text())

    def test_unknown_suite_or_shard_fails(self):
        for suite, shard in [("unknown", "--list"), ("build", "core"), ("build", "all"),
                             ("backend-it", "unknown")]:
            with self.subTest(suite=suite, shard=shard):
                result = subprocess.run(["bash", str(SCRIPT), suite, shard],
                                        text=True, capture_output=True, check=False)
                self.assertNotEqual(0, result.returncode)
                self.assertIn("Unknown", result.stderr)
                self.assertEqual("", result.stdout)


if __name__ == "__main__":
    unittest.main()
