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
KEEP_GRAVITINO_CONF_ENV = "LANCE_RAY_KEEP_GRAVITINO_CONF"


def _missing_lance_ray_deps() -> Optional[str]:
    missing = []
    for mod in ("ray", "lance_ray", "lance_namespace"):
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod)
    return ", ".join(missing) if missing else None


# Compute once at module import time so the @skipIf condition and message
# don't trigger two rounds of import attempts.
_MISSING_LANCE_RAY_DEPS = _missing_lance_ray_deps()


@unittest.skipIf(
    _MISSING_LANCE_RAY_DEPS is not None,
    f"lance-ray test deps not installed: {_MISSING_LANCE_RAY_DEPS}. "
    "Install with: pip install -r clients/client-python/requirements-dev.txt "
    "(or pip install -e .[lance]). Requires the Gravitino server to expose a "
    "lance-rest auxiliary service backed by lance-namespace-core >= 0.7.5.",
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
    appended_lance_rest_conf: bool = False

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls._get_gravitino_home()
        gravitino_home = cls.gravitino_home
        cls.main_conf_path = os.path.join(gravitino_home, MAIN_CONF_FILE)

        # Bind the lance-rest aux service to our test metalake. If the same
        # binding is already present (e.g. an earlier run in the same Gradle
        # session left it there), skip the conf write and the restart. This
        # avoids restarting Gravitino in the middle of the IT suite when the
        # test class is replayed, which would briefly disrupt other ITs.
        if not cls._lance_metalake_already_bound():
            cls._append_conf(cls._lance_rest_config(), cls.main_conf_path)
            cls.appended_lance_rest_conf = True
            cls.restart_server()
        if not cls._wait_for_lance_rest_ready():
            raise RuntimeError(
                "Lance REST aux service did not become ready in time at "
                + LANCE_REST_BASE_URL
            )

        # Probe whether the server-side `lance-namespace-core` is new enough
        # to deserialize requests from the installed PyPI `lance-namespace`.
        # We skip cleanly (rather than fail) when the server is older — this
        # happens on branches that haven't merged the `lance-namespace-core`
        # upgrade yet. The probe runs *before* metalake/catalog/schema setup
        # so a skipped run leaves no fixtures behind.
        skip_reason = cls._check_lance_namespace_compat()
        if skip_reason is not None:
            cls._reset_lance_rest_conf_if_needed()
            raise unittest.SkipTest(skip_reason)

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

        try:
            cls._reset_lance_rest_conf_if_needed()
        except Exception as e:  # pylint: disable=broad-exception-caught
            failures.append(("reset lance-rest conf", e))

        try:
            if cls.temp_dir and os.path.exists(cls.temp_dir):
                shutil.rmtree(cls.temp_dir, ignore_errors=True)
        except Exception as e:  # pylint: disable=broad-exception-caught
            failures.append(("remove temp dir", e))

        for step, err in failures:
            logger.warning("Cleanup step %s failed: %s", step, err)

        super().tearDownClass()

    @classmethod
    def _check_lance_namespace_compat(cls) -> Optional[str]:
        """Detect server/client schema drift on lance-namespace.

        The lance-namespace REST model evolves: newer client versions add
        request fields (e.g. ``check_declared``) that older
        ``lance-namespace-core`` builds on the server side reject with
        Jackson's "Unrecognized field ... not marked as ignorable". This
        helper sends a harmless ``describe_table`` for a bogus table id so
        we can observe the schema-validation error without doing any real
        work. Returns a skip reason on incompatibility, or ``None`` if the
        server understands the request shape.
        """
        try:
            # pylint: disable=import-outside-toplevel
            import lance_namespace
            from lance_namespace import DescribeTableRequest

            # pylint: enable=import-outside-toplevel
        except ImportError:
            # `@unittest.skipIf` on the class already handles this case; if
            # we reach here something odd is going on but it's not our job
            # to recover from it.
            return None

        try:
            ns = lance_namespace.connect("rest", {"uri": LANCE_REST_BASE_URL})
        except Exception as e:  # pylint: disable=broad-exception-caught
            return f"unable to connect to lance-rest aux service: {e}"

        probe = DescribeTableRequest(id=["__probe__", "__probe__", "__probe__"])
        try:
            ns.describe_table(probe)
            # Unlikely but not impossible: the probe table actually exists
            # in a leftover metalake. That's still a compatible server.
            return None
        except Exception as e:  # pylint: disable=broad-exception-caught
            msg = str(e)
            if "Unrecognized field" in msg or "not marked as ignorable" in msg:
                short = msg.splitlines()[0][:200]
                return (
                    "lance-rest server's lance-namespace-core is older than "
                    "the client's lance-namespace (request schema mismatch). "
                    f"Server reported: {short}. To run this test, use "
                    "lance-namespace-core 0.7.5 or newer on the server, "
                    "or roll the client back to a matching version."
                )
            # Any other error (table-not-found, metalake-not-found, etc.)
            # means the server *did* deserialize the request — i.e. schemas
            # are compatible.
            return None

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

    @classmethod
    def _lance_rest_config(cls):
        return {LANCE_REST_METALAKE_KEY: cls.METALAKE_NAME}

    @classmethod
    def _should_keep_lance_rest_conf(cls) -> bool:
        return os.environ.get(KEEP_GRAVITINO_CONF_ENV, "").lower() == "true"

    @classmethod
    def _reset_lance_rest_conf_if_needed(cls) -> None:
        if not cls.appended_lance_rest_conf or cls.main_conf_path is None:
            return
        if cls._should_keep_lance_rest_conf():
            logger.info(
                "Keeping lance-rest Gravitino conf because %s=true",
                KEEP_GRAVITINO_CONF_ENV,
            )
            return
        cls._reset_conf(cls._lance_rest_config(), cls.main_conf_path)
        cls.appended_lance_rest_conf = False
        cls.restart_server()

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
        # cleanly without import errors at module load time. The matrix runner
        # also swaps lance-ray versions in isolated venvs, so keep imports local.
        # pylint: disable=import-outside-toplevel,import-error
        import ray
        from lance_ray import read_lance, write_lance

        # pylint: enable=import-outside-toplevel,import-error

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
