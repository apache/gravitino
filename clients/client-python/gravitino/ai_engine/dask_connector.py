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

"""Dask connector for Gravitino UDF management.

This module provides the :class:`DaskConnector` that loads UDFs registered
in Gravitino and wraps them as ``dask.delayed`` callables so they can be
executed lazily across a Dask cluster.

Usage example::

    from gravitino.ai_engine.dask_connector import DaskConnector

    connector = DaskConnector(
        gravitino_uri="http://localhost:8090",
        metalake="my_metalake",
        catalog="my_catalog",
        schema="my_schema",
    )

    # Load a single function
    delayed_fn = connector.load_function("add_one")
    result = delayed_fn(41).compute()

    # Load all Dask-runtime functions at once
    functions = connector.load_functions()
"""

import logging
from typing import Any, Callable, Dict, List, Optional

from gravitino.ai_engine.base_connector import BaseAIEngineConnector
from gravitino.api.function.function import Function
from gravitino.api.function.function_impl import FunctionImpl

logger = logging.getLogger(__name__)


class DaskConnector(BaseAIEngineConnector):
    """Gravitino UDF connector for the Dask parallel compute engine.

    Functions loaded through this connector are wrapped with ``dask.delayed``
    so they can participate in Dask's lazy task-graph execution model.
    """

    def __init__(
        self,
        gravitino_uri: str,
        metalake: str,
        catalog: str,
        schema: str,
        dask_options: Optional[Dict[str, Any]] = None,
    ):
        """Create a DaskConnector.

        Args:
            gravitino_uri: The URI of the Gravitino server.
            metalake: The metalake name.
            catalog: The catalog name.
            schema: The schema name.
            dask_options: Optional dict of keyword arguments forwarded to
                ``dask.delayed`` (e.g. ``{"pure": True, "nout": 1}``).

        Raises:
            ImportError: If the ``dask`` package is not installed.
        """
        super().__init__(gravitino_uri, metalake, catalog, schema)
        try:
            import dask  # noqa: F401 – availability check
        except ImportError as exc:
            raise ImportError(
                "The 'dask' package is required for DaskConnector. "
                "Install it with: pip install dask"
            ) from exc
        self._dask_options: Dict[str, Any] = dask_options or {}

    @property
    def runtime_type(self) -> FunctionImpl.RuntimeType:
        """Returns the DASK runtime type."""
        return FunctionImpl.RuntimeType.DASK

    def _wrap_function(self, func: Callable, function_meta: Function) -> Any:
        """Wrap *func* as a ``dask.delayed`` callable.

        Args:
            func: The raw Python callable resolved from the Gravitino UDF.
            function_meta: The Gravitino function metadata.

        Returns:
            A ``dask.delayed`` wrapper around the callable.
        """
        import dask

        delayed_fn = dask.delayed(func, **self._dask_options)
        logger.info(
            "Loaded Gravitino UDF '%s' as Dask delayed function",
            function_meta.name(),
        )
        return delayed_fn
