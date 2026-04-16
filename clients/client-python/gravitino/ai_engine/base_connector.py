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

"""Base connector for AI engine UDF integration with Gravitino."""

import importlib
import logging
import types as builtin_types
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

from gravitino.api.function.function import Function
from gravitino.api.function.function_definition import FunctionDefinition
from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.python_impl import PythonImpl
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace

logger = logging.getLogger(__name__)


class BaseAIEngineConnector(ABC):
    """Base class for AI engine connectors that load Gravitino UDFs.

    Subclasses implement engine-specific wrapping of loaded Python functions
    (e.g. ``ray.remote`` for Ray, ``dask.delayed`` for Dask).
    """

    def __init__(
        self,
        gravitino_uri: str,
        metalake: str,
        catalog: str,
        schema: str,
    ):
        """Create a BaseAIEngineConnector.

        Args:
            gravitino_uri: The URI of the Gravitino server (e.g. ``http://localhost:8090``).
            metalake: The metalake name.
            catalog: The catalog name.
            schema: The schema name.
        """
        self._gravitino_uri = gravitino_uri
        self._metalake = metalake
        self._catalog = catalog
        self._schema = schema
        self._client = GravitinoClient(
            uri=gravitino_uri, metalake_name=metalake
        )
        self._catalog_client = self._client.load_catalog(catalog)
        self._function_catalog = self._catalog_client.as_function_catalog()

    @property
    @abstractmethod
    def runtime_type(self) -> FunctionImpl.RuntimeType:
        """The runtime type this connector targets."""

    @abstractmethod
    def _wrap_function(self, func: Callable, function_meta: Function) -> Any:
        """Wrap a loaded Python callable for the target engine.

        Args:
            func: The raw Python callable.
            function_meta: The Gravitino function metadata.

        Returns:
            An engine-specific wrapped callable.
        """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_functions(self) -> List[str]:
        """List names of functions that have an implementation for this engine's runtime.

        Returns:
            A list of function names.
        """
        namespace = Namespace.of(self._schema)
        functions = self._function_catalog.list_function_infos(namespace)
        return [
            f.name()
            for f in functions
            if self._has_matching_impl(f)
        ]

    def load_function(self, function_name: str) -> Any:
        """Load a Gravitino UDF and wrap it for the target AI engine.

        The method resolves the first matching ``PythonImpl`` whose runtime matches
        this connector's ``runtime_type``.  Inline ``code_block`` definitions are
        supported as well as module-level ``handler`` references.

        Args:
            function_name: The name of the function to load.

        Returns:
            An engine-specific wrapped callable.

        Raises:
            ValueError: If no matching implementation is found.
        """
        ident = NameIdentifier.of(self._schema, function_name)
        function = self._function_catalog.get_function(ident)
        python_impl = self._find_matching_impl(function)
        if python_impl is None:
            raise ValueError(
                f"No {self.runtime_type.name} Python implementation found "
                f"for function '{function_name}'"
            )
        raw_callable = self._resolve_callable(python_impl)
        return self._wrap_function(raw_callable, function)

    def load_functions(self) -> Dict[str, Any]:
        """Load all functions with a matching runtime implementation.

        Returns:
            A dict mapping function name to engine-wrapped callable.
        """
        result: Dict[str, Any] = {}
        for name in self.list_functions():
            result[name] = self.load_function(name)
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _has_matching_impl(self, function: Function) -> bool:
        return self._find_matching_impl(function) is not None

    def _find_matching_impl(
        self, function: Function
    ) -> Optional[PythonImpl]:
        """Find the first PythonImpl with a matching runtime type."""
        for definition in function.definitions():
            for impl in definition.impls():
                if (
                    isinstance(impl, PythonImpl)
                    and impl.runtime() == self.runtime_type
                ):
                    return impl
        return None

    @staticmethod
    def _resolve_callable(impl: PythonImpl) -> Callable:
        """Resolve a PythonImpl to a Python callable.

        If ``code_block`` is set, compile and execute it then look up ``handler``
        inside the resulting namespace.  Otherwise, import ``handler`` as a
        dotted module path (e.g. ``my_module.my_func``).

        Args:
            impl: The PythonImpl to resolve.

        Returns:
            A Python callable.

        Raises:
            ImportError: If the handler module cannot be imported.
            AttributeError: If the handler function is not found.
            ValueError: If the resolved object is not callable.
        """
        handler = impl.handler()

        if impl.code_block():
            # Execute inline code and extract the handler
            namespace: Dict[str, Any] = {}
            # The code_block is executed in an isolated namespace
            compiled = compile(impl.code_block(), f"<gravitino-udf:{handler}>", "exec")
            exec(compiled, namespace)  # noqa: S102 – trusted UDF code from Gravitino server
            if handler not in namespace:
                raise AttributeError(
                    f"Handler '{handler}' not found in code block. "
                    f"Available names: {list(namespace.keys())}"
                )
            func = namespace[handler]
        else:
            # Import from installed module
            module_path, _, func_name = handler.rpartition(".")
            if not module_path:
                raise ImportError(
                    f"Handler '{handler}' must be a dotted path (e.g. module.func)"
                )
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)

        if not callable(func):
            raise ValueError(f"Resolved handler '{handler}' is not callable")
        return func
