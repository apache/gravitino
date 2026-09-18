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
from typing import Optional
from uuid import uuid4

import requests

from gravitino import Catalog, GravitinoAdminClient, GravitinoClient
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)

ICEBERG_REST_BASE_URL = "http://localhost:9001/iceberg/"
MAIN_CONF_FILE = "conf/gravitino.conf"
ICEBERG_REST_CONFIG_PROVIDER_KEY = "gravitino.iceberg-rest.catalog-config-provider"
ICEBERG_REST_METALAKE_KEY = "gravitino.iceberg-rest.gravitino-metalake"
ICEBERG_REST_DEFAULT_CATALOG_KEY = "gravitino.iceberg-rest.default-catalog-name"


def _missing_ray_iceberg_dependencies() -> Optional[str]:
    missing = []
    for module in ("ray", "pyiceberg"):
        try:
            __import__(module)
        except ImportError:
            missing.append(module)
    return ", ".join(missing) if missing else None


_MISSING_RAY_ICEBERG_DEPS = _missing_ray_iceberg_dependencies()


@unittest.skipIf(
    _MISSING_RAY_ICEBERG_DEPS is not None,
    f"Ray Iceberg test deps not installed: {_MISSING_RAY_ICEBERG_DEPS}. "
    "Install with: pip install -r clients/client-python/requirements-ray-iceberg.txt",
)
class TestRayIcebergIntegration(IntegrationTestEnv):
    """Exercise Ray Data's Iceberg REST read/write contract with Gravitino."""

    METALAKE_NAME = f"ray_iceberg_it_{uuid4().hex[:8]}"
    CATALOG_NAME = f"ray_iceberg_{uuid4().hex[:8]}"
    SCHEMA_NAME = "ray_schema"
    TABLE_NAME = "ray_table"
    TABLE_IDENTIFIER = f"{SCHEMA_NAME}.{TABLE_NAME}"

    gravitino_admin_client: Optional[GravitinoAdminClient] = None
    gravitino_client: Optional[GravitinoClient] = None
    iceberg_catalog = None
    temp_dir: Optional[str] = None
    main_conf_path: Optional[str] = None
    appended_iceberg_rest_conf = False
    setup_completed = False

    @classmethod
    def setUpClass(cls):
        cls.addClassCleanup(cls._cleanup_after_setup_failure)
        super().setUpClass()

        cls._get_gravitino_home()
        cls.main_conf_path = os.path.join(cls.gravitino_home, MAIN_CONF_FILE)
        cls._append_conf(
            {
                ICEBERG_REST_CONFIG_PROVIDER_KEY: "dynamic-config-provider",
                ICEBERG_REST_METALAKE_KEY: cls.METALAKE_NAME,
                ICEBERG_REST_DEFAULT_CATALOG_KEY: cls.CATALOG_NAME,
            },
            cls.main_conf_path,
        )
        cls.appended_iceberg_rest_conf = True
        cls.restart_server()

        if not cls._wait_for_iceberg_rest_ready():
            raise RuntimeError(
                "Iceberg REST auxiliary service did not become ready at "
                + ICEBERG_REST_BASE_URL
            )

        cls.gravitino_admin_client = GravitinoAdminClient("http://localhost:8090")
        cls.gravitino_admin_client.create_metalake(
            cls.METALAKE_NAME,
            comment="Ray Iceberg IT metalake",
            properties={},
        )
        cls.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=cls.METALAKE_NAME
        )
        cls.temp_dir = tempfile.mkdtemp(prefix="ray_iceberg_it_")
        cls.gravitino_client.create_catalog(
            name=cls.CATALOG_NAME,
            catalog_type=Catalog.Type.RELATIONAL,
            provider="lakehouse-iceberg",
            comment="Ray Iceberg IT catalog",
            properties={"catalog-backend": "memory", "warehouse": cls.temp_dir},
        )

        # Imports are deferred so the module can be collected without the optional IT deps.
        # pylint: disable=import-outside-toplevel
        from pyiceberg.catalog import load_catalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType

        # pylint: enable=import-outside-toplevel
        cls.iceberg_catalog = load_catalog(**cls._catalog_kwargs())
        cls.iceberg_catalog.create_namespace(cls.SCHEMA_NAME)
        cls.iceberg_catalog.create_table(
            cls.TABLE_IDENTIFIER,
            schema=Schema(
                NestedField(1, "id", LongType(), required=False),
                NestedField(2, "value", StringType(), required=False),
            ),
        )
        cls.setup_completed = True

    @classmethod
    def tearDownClass(cls):
        try:
            cls._cleanup_resources()
        finally:
            if cls.use_external_gravitino() or cls.gravitino_startup_script is not None:
                super().tearDownClass()

    @classmethod
    def _cleanup_after_setup_failure(cls):
        if cls.setup_completed:
            return
        try:
            cls._cleanup_resources()
        finally:
            if cls.use_external_gravitino() or cls.gravitino_startup_script is not None:
                super().tearDownClass()

    @classmethod
    def _cleanup_resources(cls):
        failures = []

        try:
            if cls.iceberg_catalog is not None:
                cls.iceberg_catalog.drop_table(cls.TABLE_IDENTIFIER)
        except Exception as error:  # pylint: disable=broad-exception-caught
            failures.append(("drop Iceberg table", error))

        try:
            if cls.gravitino_client is not None:
                cls.gravitino_client.drop_catalog(name=cls.CATALOG_NAME, force=True)
        except Exception as error:  # pylint: disable=broad-exception-caught
            failures.append(("drop Gravitino catalog", error))

        try:
            if cls.gravitino_admin_client is not None:
                cls.gravitino_admin_client.drop_metalake(
                    name=cls.METALAKE_NAME, force=True
                )
        except Exception as error:  # pylint: disable=broad-exception-caught
            failures.append(("drop Gravitino metalake", error))

        try:
            if cls.appended_iceberg_rest_conf and cls.main_conf_path is not None:
                cls._reset_conf(
                    {
                        ICEBERG_REST_CONFIG_PROVIDER_KEY: "dynamic-config-provider",
                        ICEBERG_REST_METALAKE_KEY: cls.METALAKE_NAME,
                        ICEBERG_REST_DEFAULT_CATALOG_KEY: cls.CATALOG_NAME,
                    },
                    cls.main_conf_path,
                )
                cls.restart_server()
        except Exception as error:  # pylint: disable=broad-exception-caught
            failures.append(("reset Iceberg REST conf", error))
        finally:
            if cls.temp_dir and os.path.exists(cls.temp_dir):
                shutil.rmtree(cls.temp_dir, ignore_errors=True)

        for step, error in failures:
            logger.warning("Cleanup step %s failed: %s", step, error)

    def test_ray_can_write_and_read_existing_iceberg_table(self):
        # pylint: disable=import-outside-toplevel,import-error
        import ray

        # pylint: enable=import-outside-toplevel,import-error
        ray.init(
            ignore_reinit_error=True,
            num_cpus=2,
            include_dashboard=False,
            log_to_driver=False,
        )
        try:
            catalog_kwargs = self._catalog_kwargs()
            empty_dataset = ray.data.read_iceberg(
                table_identifier=self.TABLE_IDENTIFIER,
                catalog_kwargs=catalog_kwargs,
            )
            self.assertEqual(0, empty_dataset.count())

            data = ray.data.from_items(
                [{"id": index, "value": f"value-{index}"} for index in range(8)]
            ).repartition(2)
            data.write_iceberg(
                table_identifier=self.TABLE_IDENTIFIER,
                catalog_kwargs=catalog_kwargs,
            )

            dataset = ray.data.read_iceberg(
                table_identifier=self.TABLE_IDENTIFIER,
                catalog_kwargs=catalog_kwargs,
            )
            self.assertEqual(8, dataset.count())
            self.assertEqual(["id", "value"], dataset.schema().names)
            self.assertEqual(4, dataset.filter(lambda row: row["id"] % 2 == 0).count())
        finally:
            ray.shutdown()

    @classmethod
    def _catalog_kwargs(cls):
        return {
            "name": "default",
            "type": "rest",
            "uri": ICEBERG_REST_BASE_URL,
        }

    @staticmethod
    def _wait_for_iceberg_rest_ready(timeout_s: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                response = requests.get(ICEBERG_REST_BASE_URL + "v1/config", timeout=2)
                if response.status_code == 200:
                    return True
            except requests.RequestException:
                pass
            time.sleep(0.5)
        return False
