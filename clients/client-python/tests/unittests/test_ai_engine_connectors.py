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
from unittest.mock import MagicMock, patch

from gravitino.ai_engine.base_connector import BaseAIEngineConnector
from gravitino.api.function.function import Function
from gravitino.api.function.function_definition import FunctionDefinition
from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.function_type import FunctionType
from gravitino.api.function.python_impl import PythonImpl
from gravitino.api.function.sql_impl import SQLImpl


def _make_function(
    name: str,
    impls: list,
    function_type: FunctionType = FunctionType.SCALAR,
) -> MagicMock:
    """Create a mock Function with the given implementations."""
    mock_definition = MagicMock(spec=FunctionDefinition)
    mock_definition.impls.return_value = impls

    mock_function = MagicMock(spec=Function)
    mock_function.name.return_value = name
    mock_function.function_type.return_value = function_type
    mock_function.deterministic.return_value = True
    mock_function.definitions.return_value = [mock_definition]
    return mock_function


class ConcreteConnector(BaseAIEngineConnector):
    """Concrete test implementation of BaseAIEngineConnector."""

    @property
    def runtime_type(self) -> FunctionImpl.RuntimeType:
        return FunctionImpl.RuntimeType.RAY

    def _wrap_function(self, func, function_meta):
        return func  # no wrapping for base tests


class TestBaseAIEngineConnector(unittest.TestCase):
    """Tests for BaseAIEngineConnector logic."""

    @patch(
        "gravitino.ai_engine.base_connector.GravitinoClient",
        autospec=True,
    )
    def setUp(self, mock_client_cls):
        self.mock_client = mock_client_cls.return_value
        self.mock_catalog = MagicMock()
        self.mock_function_catalog = MagicMock()
        self.mock_client.load_catalog.return_value = self.mock_catalog
        self.mock_catalog.as_function_catalog.return_value = (
            self.mock_function_catalog
        )
        self.connector = ConcreteConnector(
            gravitino_uri="http://localhost:8090",
            metalake="test_ml",
            catalog="test_cat",
            schema="test_schema",
        )

    def test_list_functions_filters_by_runtime(self):
        ray_impl = PythonImpl(
            runtime=FunctionImpl.RuntimeType.RAY,
            handler="my_module.add_one",
        )
        spark_impl = PythonImpl(
            runtime=FunctionImpl.RuntimeType.SPARK,
            handler="my_module.other",
        )
        func_ray = _make_function("ray_func", [ray_impl])
        func_spark = _make_function("spark_func", [spark_impl])

        self.mock_function_catalog.list_function_infos.return_value = [
            func_ray,
            func_spark,
        ]

        names = self.connector.list_functions()
        self.assertEqual(["ray_func"], names)

    def test_load_function_with_handler(self):
        ray_impl = PythonImpl(
            runtime=FunctionImpl.RuntimeType.RAY,
            handler="os.path.join",
        )
        func = _make_function("path_join", [ray_impl])
        self.mock_function_catalog.get_function.return_value = func

        result = self.connector.load_function("path_join")
        import os.path

        self.assertIs(result, os.path.join)

    def test_load_function_with_code_block(self):
        code = "def add_one(x):\n    return x + 1\n"
        ray_impl = PythonImpl(
            runtime=FunctionImpl.RuntimeType.RAY,
            handler="add_one",
            code_block=code,
        )
        func = _make_function("add_one", [ray_impl])
        self.mock_function_catalog.get_function.return_value = func

        loaded = self.connector.load_function("add_one")
        self.assertEqual(loaded(41), 42)

    def test_load_function_no_matching_impl(self):
        spark_impl = PythonImpl(
            runtime=FunctionImpl.RuntimeType.SPARK,
            handler="my_module.func",
        )
        func = _make_function("spark_only", [spark_impl])
        self.mock_function_catalog.get_function.return_value = func

        with self.assertRaises(ValueError) as ctx:
            self.connector.load_function("spark_only")
        self.assertIn("No RAY Python implementation", str(ctx.exception))

    def test_load_function_skips_non_python_impl(self):
        sql_impl = MagicMock(spec=SQLImpl)
        sql_impl.runtime.return_value = FunctionImpl.RuntimeType.RAY

        ray_impl = PythonImpl(
            runtime=FunctionImpl.RuntimeType.RAY,
            handler="os.path.exists",
        )
        func = _make_function("mixed", [sql_impl, ray_impl])
        self.mock_function_catalog.get_function.return_value = func

        result = self.connector.load_function("mixed")
        import os.path

        self.assertIs(result, os.path.exists)

    def test_load_functions_returns_all(self):
        ray_impl1 = PythonImpl(
            runtime=FunctionImpl.RuntimeType.RAY,
            handler="os.path.join",
        )
        ray_impl2 = PythonImpl(
            runtime=FunctionImpl.RuntimeType.RAY,
            handler="os.path.exists",
        )
        func1 = _make_function("join_fn", [ray_impl1])
        func2 = _make_function("exists_fn", [ray_impl2])

        self.mock_function_catalog.list_function_infos.return_value = [
            func1,
            func2,
        ]
        self.mock_function_catalog.get_function.side_effect = [func1, func2]

        result = self.connector.load_functions()
        self.assertEqual(set(result.keys()), {"join_fn", "exists_fn"})

    def test_resolve_callable_bad_handler_no_dot(self):
        impl = PythonImpl(
            runtime=FunctionImpl.RuntimeType.RAY,
            handler="no_dot_handler",
        )
        with self.assertRaises(ImportError):
            BaseAIEngineConnector._resolve_callable(impl)

    def test_resolve_callable_code_block_handler_missing(self):
        impl = PythonImpl(
            runtime=FunctionImpl.RuntimeType.RAY,
            handler="missing_func",
            code_block="x = 1\n",
        )
        with self.assertRaises(AttributeError) as ctx:
            BaseAIEngineConnector._resolve_callable(impl)
        self.assertIn("missing_func", str(ctx.exception))

    def test_resolve_callable_not_callable(self):
        impl = PythonImpl(
            runtime=FunctionImpl.RuntimeType.RAY,
            handler="my_var",
            code_block="my_var = 42\n",
        )
        with self.assertRaises(ValueError) as ctx:
            BaseAIEngineConnector._resolve_callable(impl)
        self.assertIn("not callable", str(ctx.exception))


class TestRayConnector(unittest.TestCase):
    """Tests for RayConnector."""

    @patch(
        "gravitino.ai_engine.base_connector.GravitinoClient",
        autospec=True,
    )
    @patch("gravitino.ai_engine.ray_connector.ray", create=True)
    def test_wrap_function_calls_ray_remote(self, mock_ray, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_catalog = MagicMock()
        mock_function_catalog = MagicMock()
        mock_client.load_catalog.return_value = mock_catalog
        mock_catalog.as_function_catalog.return_value = mock_function_catalog

        # Patch the import check
        import sys

        mock_ray_module = MagicMock()
        sys.modules["ray"] = mock_ray_module

        try:
            from gravitino.ai_engine.ray_connector import RayConnector

            connector = RayConnector(
                gravitino_uri="http://localhost:8090",
                metalake="ml",
                catalog="cat",
                schema="sch",
            )

            func = lambda x: x + 1  # noqa: E731
            function_meta = _make_function("add_one", [])
            mock_ray_module.remote.return_value = "wrapped"

            result = connector._wrap_function(func, function_meta)
            mock_ray_module.remote.assert_called_once_with(func)
            self.assertEqual(result, "wrapped")
        finally:
            del sys.modules["ray"]

    @patch(
        "gravitino.ai_engine.base_connector.GravitinoClient",
        autospec=True,
    )
    def test_ray_connector_import_error(self, mock_client_cls):
        """RayConnector should raise ImportError if ray is not installed."""
        mock_client = mock_client_cls.return_value
        mock_catalog = MagicMock()
        mock_client.load_catalog.return_value = mock_catalog
        mock_catalog.as_function_catalog.return_value = MagicMock()

        import sys

        # Ensure ray is not importable
        original = sys.modules.pop("ray", None)
        try:
            with patch.dict(sys.modules, {"ray": None}):
                # Need to reload to trigger the import check
                from gravitino.ai_engine import ray_connector

                with self.assertRaises(ImportError) as ctx:
                    ray_connector.RayConnector(
                        gravitino_uri="http://localhost:8090",
                        metalake="ml",
                        catalog="cat",
                        schema="sch",
                    )
                self.assertIn("ray", str(ctx.exception))
        finally:
            if original is not None:
                sys.modules["ray"] = original

    @patch(
        "gravitino.ai_engine.base_connector.GravitinoClient",
        autospec=True,
    )
    def test_ray_connector_with_options(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_catalog = MagicMock()
        mock_function_catalog = MagicMock()
        mock_client.load_catalog.return_value = mock_catalog
        mock_catalog.as_function_catalog.return_value = mock_function_catalog

        import sys

        mock_ray_module = MagicMock()
        sys.modules["ray"] = mock_ray_module

        try:
            from gravitino.ai_engine.ray_connector import RayConnector

            options = {"num_cpus": 2, "num_gpus": 1}
            connector = RayConnector(
                gravitino_uri="http://localhost:8090",
                metalake="ml",
                catalog="cat",
                schema="sch",
                ray_options=options,
            )

            func = lambda x: x + 1  # noqa: E731
            function_meta = _make_function("add_one", [])

            # ray.remote(**options) returns a decorator, then that decorator wraps func
            mock_decorator = MagicMock(return_value="wrapped_with_opts")
            mock_ray_module.remote.return_value = mock_decorator

            result = connector._wrap_function(func, function_meta)
            mock_ray_module.remote.assert_called_once_with(num_cpus=2, num_gpus=1)
            mock_decorator.assert_called_once_with(func)
            self.assertEqual(result, "wrapped_with_opts")
        finally:
            del sys.modules["ray"]

    @patch(
        "gravitino.ai_engine.base_connector.GravitinoClient",
        autospec=True,
    )
    def test_ray_connector_runtime_type(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_catalog = MagicMock()
        mock_client.load_catalog.return_value = mock_catalog
        mock_catalog.as_function_catalog.return_value = MagicMock()

        import sys

        sys.modules["ray"] = MagicMock()
        try:
            from gravitino.ai_engine.ray_connector import RayConnector

            connector = RayConnector(
                gravitino_uri="http://localhost:8090",
                metalake="ml",
                catalog="cat",
                schema="sch",
            )
            self.assertEqual(connector.runtime_type, FunctionImpl.RuntimeType.RAY)
        finally:
            del sys.modules["ray"]


class TestDaskConnector(unittest.TestCase):
    """Tests for DaskConnector."""

    @patch(
        "gravitino.ai_engine.base_connector.GravitinoClient",
        autospec=True,
    )
    def test_wrap_function_calls_dask_delayed(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_catalog = MagicMock()
        mock_function_catalog = MagicMock()
        mock_client.load_catalog.return_value = mock_catalog
        mock_catalog.as_function_catalog.return_value = mock_function_catalog

        import sys

        mock_dask_module = MagicMock()
        sys.modules["dask"] = mock_dask_module

        try:
            from gravitino.ai_engine.dask_connector import DaskConnector

            connector = DaskConnector(
                gravitino_uri="http://localhost:8090",
                metalake="ml",
                catalog="cat",
                schema="sch",
            )

            func = lambda x: x + 1  # noqa: E731
            function_meta = _make_function("add_one", [])
            mock_dask_module.delayed.return_value = "delayed_fn"

            result = connector._wrap_function(func, function_meta)
            mock_dask_module.delayed.assert_called_once_with(func)
            self.assertEqual(result, "delayed_fn")
        finally:
            del sys.modules["dask"]

    @patch(
        "gravitino.ai_engine.base_connector.GravitinoClient",
        autospec=True,
    )
    def test_dask_connector_import_error(self, mock_client_cls):
        """DaskConnector should raise ImportError if dask is not installed."""
        mock_client = mock_client_cls.return_value
        mock_catalog = MagicMock()
        mock_client.load_catalog.return_value = mock_catalog
        mock_catalog.as_function_catalog.return_value = MagicMock()

        import sys

        original = sys.modules.pop("dask", None)
        try:
            with patch.dict(sys.modules, {"dask": None}):
                from gravitino.ai_engine import dask_connector

                with self.assertRaises(ImportError) as ctx:
                    dask_connector.DaskConnector(
                        gravitino_uri="http://localhost:8090",
                        metalake="ml",
                        catalog="cat",
                        schema="sch",
                    )
                self.assertIn("dask", str(ctx.exception))
        finally:
            if original is not None:
                sys.modules["dask"] = original

    @patch(
        "gravitino.ai_engine.base_connector.GravitinoClient",
        autospec=True,
    )
    def test_dask_connector_runtime_type(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_catalog = MagicMock()
        mock_client.load_catalog.return_value = mock_catalog
        mock_catalog.as_function_catalog.return_value = MagicMock()

        import sys

        sys.modules["dask"] = MagicMock()
        try:
            from gravitino.ai_engine.dask_connector import DaskConnector

            connector = DaskConnector(
                gravitino_uri="http://localhost:8090",
                metalake="ml",
                catalog="cat",
                schema="sch",
            )
            self.assertEqual(connector.runtime_type, FunctionImpl.RuntimeType.DASK)
        finally:
            del sys.modules["dask"]

    @patch(
        "gravitino.ai_engine.base_connector.GravitinoClient",
        autospec=True,
    )
    def test_dask_connector_with_options(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_catalog = MagicMock()
        mock_function_catalog = MagicMock()
        mock_client.load_catalog.return_value = mock_catalog
        mock_catalog.as_function_catalog.return_value = mock_function_catalog

        import sys

        mock_dask_module = MagicMock()
        sys.modules["dask"] = mock_dask_module

        try:
            from gravitino.ai_engine.dask_connector import DaskConnector

            options = {"pure": True, "nout": 1}
            connector = DaskConnector(
                gravitino_uri="http://localhost:8090",
                metalake="ml",
                catalog="cat",
                schema="sch",
                dask_options=options,
            )

            func = lambda x: x + 1  # noqa: E731
            function_meta = _make_function("add_one", [])
            mock_dask_module.delayed.return_value = "delayed_fn"

            result = connector._wrap_function(func, function_meta)
            mock_dask_module.delayed.assert_called_once_with(func, pure=True, nout=1)
            self.assertEqual(result, "delayed_fn")
        finally:
            del sys.modules["dask"]


if __name__ == "__main__":
    unittest.main()
