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

"""Daft Native's existing-table read/append contract with Gravitino REST."""

import atexit
import importlib.util
import os
import signal
import unittest
from pathlib import Path

from tests.integration.daft_iceberg_test_env import DaftIcebergTestEnv


def missing_dependencies():
    """Allow default discovery without optional deps; require them in the IT task."""
    return [
        name
        for name in ("daft", "pyiceberg", "pyarrow")
        if importlib.util.find_spec(name) is None
    ]


def exit_on_termination(signum, _frame):
    """Let atexit release the owned JVM when the test driver is terminated."""
    raise SystemExit(128 + signum)


class TestDaftIcebergIntegration(unittest.TestCase):
    """Exercise the same existing-table boundary as the Ray Data contract IT."""

    NAMESPACE = "daft_schema"
    TABLE_IDENTIFIER = "daft_schema.daft_table"

    @classmethod
    def setUpClass(cls):
        required = os.environ.get("DAFT_ICEBERG_IT_REQUIRED") == "true"
        missing = missing_dependencies()
        if missing:
            message = "Daft Iceberg IT dependencies missing: " + ", ".join(missing)
            if required:
                raise RuntimeError(message)
            raise unittest.SkipTest(message)

        if not required:
            # Default integration discovery already has a running Gravitino server.
            raise unittest.SkipTest("Run the owned fixture with the daftIcebergIT task")

        home = os.environ.get("GRAVITINO_HOME")
        if home is None:
            raise RuntimeError("GRAVITINO_HOME must point to a built distribution")
        log_dir = Path(
            os.environ.get(
                "DAFT_ICEBERG_IT_LOG_DIR",
                str(Path(__file__).resolve().parents[2] / "build/daft-iceberg-it-logs"),
            )
        )
        cls.env = DaftIcebergTestEnv(Path(home), log_dir)
        cls.addClassCleanup(cls.env.close)
        cls.addClassCleanup(atexit.unregister, cls.env.close)
        atexit.register(cls.env.close)
        previous_handler = signal.signal(signal.SIGTERM, exit_on_termination)
        cls.addClassCleanup(signal.signal, signal.SIGTERM, previous_handler)
        cls.env.start()

    def test_daft_can_append_and_read_existing_iceberg_table(self):
        # Optional integrations are imported only in the dedicated test environment.
        # pylint: disable=import-outside-toplevel,import-error
        import daft
        import pyarrow as pa
        from pyiceberg.catalog import load_catalog
        from pyiceberg import schema, types

        # pylint: enable=import-outside-toplevel,import-error
        catalog = load_catalog(**self.env.catalog_options())
        catalog.create_namespace(self.NAMESPACE)
        self.addCleanup(catalog.drop_namespace, self.NAMESPACE)
        table = catalog.create_table(
            self.TABLE_IDENTIFIER,
            schema=schema.Schema(
                types.NestedField(1, "id", types.LongType(), required=False),
                types.NestedField(2, "value", types.StringType(), required=False),
            ),
        )
        self.addCleanup(catalog.drop_table, self.TABLE_IDENTIFIER)

        empty = daft.read_iceberg(table)
        self.assertEqual(["id", "value"], empty.schema().column_names())
        self.assertEqual(daft.DataType.int64(), empty.schema()["id"].dtype)
        self.assertEqual(daft.DataType.string(), empty.schema()["value"].dtype)
        self.assertEqual({"id": [], "value": []}, empty.to_pydict())
        arrow_schema = pa.schema([("id", pa.int64()), ("value", pa.string())])
        expected = {
            "id": list(range(8)),
            "value": [None if i == 1 else f"value-{i}" for i in range(8)],
        }
        for start in (0, 4):
            batch = pa.Table.from_pydict(
                {key: values[start : start + 4] for key, values in expected.items()},
                schema=arrow_schema,
            )
            daft.from_arrow(batch).write_iceberg(
                catalog.load_table(self.TABLE_IDENTIFIER), mode="append"
            )

        reloaded = catalog.load_table(self.TABLE_IDENTIFIER)
        frame = daft.read_iceberg(reloaded)
        self.assertEqual(["id", "value"], frame.schema().column_names())
        self.assertEqual(expected, frame.sort("id").to_pydict())
        self.assertEqual(
            {key: values[4:] for key, values in expected.items()},
            frame.where(daft.col("id") >= 4).sort("id").to_pydict(),
        )
        self.assertEqual(expected, reloaded.scan().to_arrow().sort_by("id").to_pydict())
