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
import shutil
import tempfile
import time
import unittest
from random import randint
from typing import Optional

import requests

from gravitino import (
    Catalog,
    GravitinoAdminClient,
    GravitinoClient,
)
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)

LANCE_REST_PORT = 9101
LANCE_REST_BASE_URL = f"http://localhost:{LANCE_REST_PORT}/lance"

# The Lance REST server runs as an auxiliary service inside the main
# Gravitino process (gravitino.auxService.names = ...,lance-rest), so its
# bind metalake is configured in the *main* gravitino.conf rather than the
# standalone lance-rest conf file.
MAIN_CONF_FILE = "conf/gravitino.conf"
LANCE_REST_METALAKE_KEY = "gravitino.lance-rest.gravitino-metalake"


def _missing_lance_ray_deps() -> Optional[str]:
    missing = []
    for mod in ("ray", "lance_ray", "lance_namespace"):
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    return ", ".join(missing) if missing else None


@unittest.skipIf(
    _missing_lance_ray_deps() is not None,
    f"lance-ray test deps not installed: {_missing_lance_ray_deps()}. "
    "Install with: pip install ray lance-ray lance-namespace",
)
class TestLanceRayIntegration(IntegrationTestEnv):
    """End-to-end test for the lance-ray Python client against a Gravitino-backed
    Lance REST namespace. Mirrors the ``ray.data`` -> ``write_lance`` ->
    ``read_lance`` flow from the upstream lance-ray docs.
    """

    # Metalake name is fixed (not randomized) so back-to-back runs in the
    # same Gravitino process can detect that the lance-rest aux service is
    # already bound and skip the costly server restart. The per-test table
    # name still gets a random suffix to keep individual test methods
    # isolated.
    METALAKE_NAME: str = "lance_ray_test_metalake"
    CATALOG_NAME: str = "lance_catalog"
    SCHEMA_NAME: str = "schema"
    TABLE_NAME: str = "lance_ray_tbl_" + str(randint(1, 100000))

    gravitino_admin_client: Optional[GravitinoAdminClient] = None
    gravitino_client: Optional[GravitinoClient] = None
    temp_dir: Optional[str] = None
    main_conf_path: Optional[str] = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        gravitino_home = os.environ.get("GRAVITINO_HOME")
        if not gravitino_home:
            raise RuntimeError(
                "GRAVITINO_HOME must be set to the distribution package directory"
            )
        cls.main_conf_path = os.path.join(gravitino_home, MAIN_CONF_FILE)

        # Bind the lance-rest aux service to our test metalake. If the same
        # binding is already present (e.g. an earlier run in the same Gradle
        # session left it there), skip the conf write and the restart. This
        # avoids restarting Gravitino in the middle of the IT suite when the
        # test class is replayed, which would briefly disrupt other ITs.
        if not cls._lance_metalake_already_bound():
            cls._append_conf(
                {LANCE_REST_METALAKE_KEY: cls.METALAKE_NAME}, cls.main_conf_path
            )
            cls.restart_server()
        if not cls._wait_for_lance_rest_ready():
            raise RuntimeError(
                "Lance REST aux service did not become ready in time at "
                + LANCE_REST_BASE_URL
            )

        cls.gravitino_admin_client = GravitinoAdminClient("http://localhost:8090")
        # Idempotent: tolerate a metalake left over from a prior failed run.
        try:
            cls.gravitino_admin_client.create_metalake(
                cls.METALAKE_NAME,
                comment="lance-ray IT metalake",
                properties={},
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            if "already exists" not in str(e).lower():
                raise
            logger.info("Metalake %s already exists, reusing", cls.METALAKE_NAME)
        cls.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=cls.METALAKE_NAME
        )
        cls.temp_dir = tempfile.mkdtemp(prefix="lance_ray_it_")
        # Idempotent catalog + schema creation too.
        try:
            cls.gravitino_client.create_catalog(
                name=cls.CATALOG_NAME,
                catalog_type=Catalog.Type.RELATIONAL,
                provider="lakehouse-generic",
                comment="lance-ray IT catalog",
                properties={"location": cls.temp_dir},
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            if "already exists" not in str(e).lower():
                raise
        catalog = cls.gravitino_client.load_catalog(cls.CATALOG_NAME)
        try:
            catalog.as_schemas().create_schema(
                schema_name=cls.SCHEMA_NAME,
                comment="lance-ray IT schema",
                properties={},
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            if "already exists" not in str(e).lower():
                raise

    @classmethod
    def tearDownClass(cls):
        failures = []

        try:
            if cls.gravitino_client is not None:
                cls.gravitino_client.drop_catalog(name=cls.CATALOG_NAME, force=True)
        except Exception as e:  # pylint: disable=broad-exception-caught
            failures.append(("drop catalog", e))

        try:
            if cls.gravitino_admin_client is not None:
                cls.gravitino_admin_client.drop_metalake(
                    name=cls.METALAKE_NAME, force=True
                )
        except Exception as e:  # pylint: disable=broad-exception-caught
            failures.append(("drop metalake", e))

        # Intentionally do NOT reset the lance-rest metalake binding in
        # gravitino.conf. Removing it would force the next setUpClass to
        # restart the server, which is disruptive when this IT is replayed
        # back-to-back or alongside other ITs in the same Gradle invocation.
        # The conf line is regenerated by `compileDistribution`, so it does
        # not survive a fresh distribution build.

        try:
            if cls.temp_dir and os.path.exists(cls.temp_dir):
                shutil.rmtree(cls.temp_dir, ignore_errors=True)
        except Exception as e:  # pylint: disable=broad-exception-caught
            failures.append(("remove temp dir", e))

        for step, err in failures:
            logger.warning("Cleanup step %s failed: %s", step, err)

        super().tearDownClass()

    @classmethod
    def _lance_metalake_already_bound(cls) -> bool:
        if cls.main_conf_path is None or not os.path.exists(cls.main_conf_path):
            return False
        needle = f"{LANCE_REST_METALAKE_KEY} = {cls.METALAKE_NAME}"
        with open(cls.main_conf_path, encoding="utf-8") as f:
            for line in f:
                if line.strip() == needle:
                    return True
        return False

    @staticmethod
    def _wait_for_lance_rest_ready(timeout_s: float = 60.0) -> bool:
        # Probe any registered Jersey path on the lance servlet. A 4xx
        # response is fine; what we want is *any* response from the lance
        # mount instead of connection refused or the bare Jetty 404.
        deadline = time.monotonic() + timeout_s
        url = LANCE_REST_BASE_URL + "/v1/namespace/list"
        while time.monotonic() < deadline:
            try:
                resp = requests.get(url, timeout=2)
                if resp.status_code < 500:
                    return True
            except requests.RequestException:
                pass
            time.sleep(0.5)
        return False

    def test_write_read_filter_via_lance_ray(self):
        # Imports are deferred so the skipIf decorator handles missing deps
        # cleanly without import errors at module load time.
        # pylint: disable=import-outside-toplevel
        import ray
        from lance_ray import read_lance, write_lance

        # pylint: enable=import-outside-toplevel

        ns_properties = {"uri": LANCE_REST_BASE_URL}
        table_id = [self.CATALOG_NAME, self.SCHEMA_NAME, self.TABLE_NAME]

        ray.init(
            ignore_reinit_error=True,
            num_cpus=2,
            include_dashboard=False,
            log_to_driver=False,
        )
        try:
            data = ray.data.range(1000).map(
                lambda row: {"id": row["id"], "value": row["id"] * 2}
            )

            write_lance(
                data,
                namespace_impl="rest",
                namespace_properties=ns_properties,
                table_id=table_id,
            )

            ray_dataset = read_lance(
                namespace_impl="rest",
                namespace_properties=ns_properties,
                table_id=table_id,
            )
            # value = id * 2, value < 100  =>  id in [0, 49]  =>  50 rows
            filtered_count = ray_dataset.filter(lambda row: row["value"] < 100).count()
            self.assertEqual(50, filtered_count)
        finally:
            ray.shutdown()
