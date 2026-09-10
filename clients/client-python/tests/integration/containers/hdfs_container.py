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

import logging
import os
import time

from docker.errors import DockerException
from gravitino.exceptions.base import GravitinoRuntimeException

from tests.integration.containers.base_container import BaseContainer

logger = logging.getLogger(__name__)


def check_hdfs_container_status(hdfs_container, timeout_sec=150, interval_sec=10):
    """Wait for HDFS and the remote Hive Metastore, with bounded probe commands."""
    deadline = time.monotonic() + timeout_sec
    last_output = b"No readiness probe completed"
    while (remaining := deadline - time.monotonic()) > 0:
        # The image's Hive CLI check can use an embedded metastore. Also query the
        # Thrift service used by Gravitino before allowing catalog tests to start.
        # A synchronous Docker exec and time.sleep cannot be bounded by asyncio.wait_for.
        command = [
            "timeout",
            "--signal=KILL",
            f"{remaining}s",
            "bash",
            "-c",
            "bash /tmp/check-status.sh && exec hive "
            "--hiveconf hive.metastore.uris=thrift://localhost:9083 "
            "-e 'show databases;'",
        ]
        try:
            result = hdfs_container.exec_run(command)
            last_output = result.output
            if result.exit_code == 0:
                logger.info("HDFS and Hive Metastore are ready")
                return
            logger.warning(
                "HDFS/Hive readiness probe exited with %s: %s",
                result.exit_code,
                last_output,
            )
        except DockerException as error:
            last_output = str(error)
            logger.warning("Failed to check HDFS/Hive readiness: %s", error)
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(interval_sec, remaining))
    raise GravitinoRuntimeException(
        f"HDFS/Hive Metastore did not become ready within {timeout_sec}s. "
        f"Last probe output: {last_output}"
    )


class HDFSContainer(BaseContainer):
    def __init__(self):
        container_name = "python-hdfs"
        image_name = os.environ.get("GRAVITINO_CI_HIVE_DOCKER_IMAGE")
        if image_name is None:
            raise GravitinoRuntimeException(
                "GRAVITINO_CI_HIVE_DOCKER_IMAGE env variable is not set."
            )
        environment = {"HADOOP_USER_NAME": "anonymous"}

        super().__init__(container_name, image_name, environment)

        try:
            check_hdfs_container_status(self._container)
            self._fetch_ip()
        except Exception:
            self.close()
            raise
