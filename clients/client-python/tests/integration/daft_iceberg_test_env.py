# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Real Gravitino fixture independent of the Python client's dependencies."""

import logging
import os
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, BinaryIO, Optional
from uuid import uuid4

import requests

logger = logging.getLogger(__name__)


def available_port() -> int:
    """Choose a local fixture port without relying on a shared service port."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class DaftIcebergTestEnv:  # pylint: disable=too-many-instance-attributes
    """Own a server, temporary warehouse, and HTTP-created catalog fixture."""

    def __init__(self, home: Path, log_dir: Path):
        self.home = home
        self.log_dir = log_dir
        self.metalake = f"daft_iceberg_it_{uuid4().hex[:8]}"
        self.catalog = f"daft_iceberg_{uuid4().hex[:8]}"
        server_port = available_port()
        iceberg_port = available_port()
        while iceberg_port == server_port:
            iceberg_port = available_port()
        self.server_url = f"http://127.0.0.1:{server_port}"
        self.iceberg_url = f"http://127.0.0.1:{iceberg_port}/iceberg/"
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(
            {
                "Accept": "application/vnd.gravitino.v1+json",
                "X-Gravitino-User": "anonymous",
            }
        )
        self.process: Optional[subprocess.Popen] = None
        self.warehouse: Optional[tempfile.TemporaryDirectory] = None
        self.original_conf: Optional[bytes] = None
        self.server_log: Optional[BinaryIO] = None
        self.metalake_cleanup_required = False

    def start(self):
        """Start the owned JVM and create the backing memory Iceberg catalog."""
        script = self.home / "bin/gravitino.sh"
        conf = self.home / "conf/gravitino.conf"
        if not script.is_file() or not conf.is_file():
            raise RuntimeError(
                "Build the Gravitino distribution before running this IT"
            )

        # These resources span setup and the test; close() handles partial setup too.
        # pylint: disable=consider-using-with
        self.warehouse = tempfile.TemporaryDirectory(prefix="daft_iceberg_it_")
        # pylint: enable=consider-using-with
        self.original_conf = conf.read_bytes()
        overrides = {
            "gravitino.server.webserver.host": "127.0.0.1",
            "gravitino.server.webserver.httpPort": self.server_url.rsplit(":", 1)[1],
            "gravitino.auxService.names": "iceberg-rest",
            "gravitino.iceberg-rest.host": "127.0.0.1",
            "gravitino.iceberg-rest.httpPort": self.iceberg_url.split(":")[2].split(
                "/"
            )[0],
            "gravitino.iceberg-rest.catalog-config-provider": "dynamic-config-provider",
            "gravitino.iceberg-rest.gravitino-metalake": self.metalake,
            "gravitino.iceberg-rest.default-catalog-name": self.catalog,
        }
        lines = [
            line
            for line in self.original_conf.decode("utf-8").splitlines()
            if line.partition("=")[0].strip() not in overrides
        ]
        lines.extend(f"{key} = {value}" for key, value in overrides.items())
        conf.write_text("\n".join(lines) + "\n", encoding="utf-8")

        self.log_dir.mkdir(parents=True, exist_ok=True)
        # pylint: disable=consider-using-with
        self.server_log = (self.log_dir / "gravitino-process.log").open("wb")
        # pylint: enable=consider-using-with
        env = os.environ.copy()
        env["GRAVITINO_HOME"] = str(self.home)
        env["GRAVITINO_CONF_DIR"] = str(self.home / "conf")
        env["HADOOP_USER_NAME"] = "anonymous"
        env["GRAVITINO_LOG_DIR"] = str(self.log_dir)
        # The process stays alive through the test and is reaped by close().
        # pylint: disable=consider-using-with
        self.process = subprocess.Popen(
            [str(script), "run"],
            cwd=self.home,
            env=env,
            start_new_session=True,
            stdout=self.server_log,
            stderr=subprocess.STDOUT,
        )
        # pylint: enable=consider-using-with
        logger.info("Waiting for the owned Gravitino server at %s", self.server_url)
        self.wait_ready(self.server_url + "/api/version")
        # Creation may commit before its response reaches the test driver.
        self.metalake_cleanup_required = True
        try:
            self.request(
                "POST",
                "/metalakes",
                {
                    "name": self.metalake,
                    "comment": "Daft Iceberg REST contract test",
                    "properties": {},
                },
            )
        except requests.HTTPError as error:
            if error.response is not None and error.response.status_code == 409:
                # An explicit name conflict did not create an owned resource.
                self.metalake_cleanup_required = False
            raise
        self.request(
            "POST",
            f"/metalakes/{self.metalake}/catalogs",
            {
                "name": self.catalog,
                "type": "relational",
                "provider": "lakehouse-iceberg",
                "comment": "Daft Iceberg REST contract test",
                "properties": {
                    "catalog-backend": "memory",
                    "uri": "memory://daft-iceberg-it",
                    "warehouse": self.warehouse.name,
                },
            },
        )
        self.wait_ready(self.iceberg_url + "v1/config")

    def request(self, method: str, path: str, payload=None) -> dict[str, Any]:
        """Use the public Gravitino REST API without installing its Python SDK."""
        with self.session.request(
            method, self.server_url + "/api" + path, json=payload, timeout=10
        ) as response:
            response.raise_for_status()
            result: dict[str, Any] = response.json()
            if not isinstance(result, dict) or result.get("code") != 0:
                raise RuntimeError(f"Gravitino request failed: {method} {path}")
            return result

    def wait_ready(self, url: str, timeout_s: float = 60.0):
        """Fail on an exited server or a readiness timeout, rather than skip."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self.process is not None and self.process.poll() is not None:
                raise RuntimeError(
                    f"Gravitino exited before readiness; see {self.log_dir}"
                )
            try:
                # Iceberg REST does not produce the Gravitino metadata vendor type.
                with self.session.get(
                    url, headers={"Accept": "application/json"}, timeout=2
                ) as response:
                    if response.status_code == 200:
                        return
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise RuntimeError(
            f"Gravitino readiness timed out at {url}; see {self.log_dir}"
        )

    def catalog_options(self) -> dict[str, Any]:
        """Connect PyIceberg to the configured Gravitino REST service."""
        return {
            "name": "daft_contract",
            "type": "rest",
            "uri": self.iceberg_url,
            "auth": {
                "type": "basic",
                "basic": {"username": "anonymous", "password": ""},
            },
        }

    def close(self):
        """Attempt every cleanup step and report failures after restoring config."""
        failures = []
        try:
            self._drop_metalake()
        except (requests.RequestException, RuntimeError) as error:
            failures.append(error)

        try:
            self._stop_process()
        except (OSError, subprocess.SubprocessError) as error:
            failures.append(error)

        try:
            if self.original_conf is not None:
                (self.home / "conf/gravitino.conf").write_bytes(self.original_conf)
                self.original_conf = None
        except OSError as error:
            failures.append(error)

        try:
            if self.warehouse is not None:
                self.warehouse.cleanup()
                self.warehouse = None
        except OSError as error:
            failures.append(error)
        finally:
            if self.server_log is not None:
                self.server_log.close()
                self.server_log = None
            self.session.close()

        if failures:
            raise RuntimeError("Daft Iceberg fixture cleanup failed") from failures[0]

    def _drop_metalake(self):
        if not self.metalake_cleanup_required:
            return
        try:
            self.request("DELETE", f"/metalakes/{self.metalake}?force=true")
        except requests.HTTPError as error:
            if error.response is None or error.response.status_code != 404:
                raise
        self.metalake_cleanup_required = False

    def _stop_process(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=10)
            self.process = None
