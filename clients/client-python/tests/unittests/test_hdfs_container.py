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
from unittest.mock import Mock, patch

from docker.errors import DockerException
from gravitino.exceptions.base import GravitinoRuntimeException
from tests.integration.containers.hdfs_container import (
    HDFSContainer,
    check_hdfs_container_status,
)

MODULE = "tests.integration.containers.hdfs_container"


class TestHDFSContainer(unittest.TestCase):
    def test_waits_for_remote_metastore(self):
        container = Mock()
        container.exec_run.side_effect = [
            Mock(exit_code=1, output=b"Metastore connection refused"),
            Mock(exit_code=0, output=b"default"),
        ]
        with (
            patch(f"{MODULE}.time.sleep") as sleep,
            patch(f"{MODULE}.time.monotonic", return_value=0),
        ):
            check_hdfs_container_status(container)
        self.assertEqual(2, container.exec_run.call_count)
        sleep.assert_called_once()
        command = container.exec_run.call_args.args[0]
        self.assertEqual(["timeout", "--signal=KILL", "150s"], command[:3])
        self.assertIn("hive.metastore.uris=thrift://localhost:9083", command[-1])
        self.assertIn("bash /tmp/check-status.sh &&", command[-1])
        self.assertIn("show databases;", command[-1])

    def test_deadline_bounds_probe_and_sleep(self):
        container = Mock()
        container.exec_run.return_value = Mock(exit_code=137, output=b"probe timed out")
        with (
            patch(f"{MODULE}.time.monotonic", side_effect=[0, 0, 3, 5]),
            patch(f"{MODULE}.time.sleep") as sleep,
        ):
            with self.assertRaisesRegex(GravitinoRuntimeException, "probe timed out"):
                check_hdfs_container_status(container, timeout_sec=5)
        self.assertEqual("5s", container.exec_run.call_args.args[0][2])
        sleep.assert_called_once_with(2)
        container.exec_run.assert_called_once()

    def test_retries_docker_errors(self):
        container = Mock()
        container.exec_run.side_effect = [
            DockerException("temporary failure"),
            Mock(exit_code=0, output=b"default"),
        ]
        with patch(f"{MODULE}.time.sleep"):
            check_hdfs_container_status(container)
        self.assertEqual(2, container.exec_run.call_count)

    def test_removes_container_when_readiness_fails(self):
        with (
            patch.dict("os.environ", {"GRAVITINO_CI_HIVE_DOCKER_IMAGE": "test-image"}),
            patch(f"{MODULE}.BaseContainer.__init__", return_value=None),
            patch(
                f"{MODULE}.check_hdfs_container_status",
                side_effect=RuntimeError("not ready"),
            ),
            patch.object(HDFSContainer, "close") as close,
            patch.object(HDFSContainer, "_fetch_ip") as fetch_ip,
        ):
            with self.assertRaisesRegex(RuntimeError, "not ready"):
                HDFSContainer()
        close.assert_called_once()
        fetch_ip.assert_not_called()
