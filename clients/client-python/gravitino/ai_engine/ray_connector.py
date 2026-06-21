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

"""Ray connector for Gravitino UDF management.

This module provides the :class:`RayConnector` that loads UDFs registered in
Gravitino and wraps them as Ray remote functions so they can be distributed
across a Ray cluster.

Usage example::

    from gravitino.ai_engine.ray_connector import RayConnector

    connector = RayConnector(
        gravitino_uri="http://localhost:8090",
        metalake="my_metalake",
        catalog="my_catalog",
        schema="my_schema",
    )

    # Load a single function
    remote_fn = connector.load_function("add_one")
    result = ray.get(remote_fn.remote(41))

    # Load all Ray-runtime functions at once
    functions = connector.load_functions()
"""

import logging
from typing import Any, Callable, Dict, Optional

from gravitino.ai_engine.base_connector import BaseAIEngineConnector
from gravitino.api.function.function import Function
from gravitino.api.function.function_impl import FunctionImpl

logger = logging.getLogger(__name__)


class RayConnector(BaseAIEngineConnector):
    """Gravitino UDF connector for the Ray distributed compute engine.

    Functions loaded through this connector are wrapped with ``ray.remote``
    so they can be submitted as tasks on a Ray cluster.  Optional
    ``ray_options`` (num_cpus, num_gpus, etc.) can be passed at construction
    time and will be forwarded to every ``ray.remote`` call.
    """

    def __init__(
        self,
        gravitino_uri: str,
        metalake: str,
        catalog: str,
        schema: str,
        ray_options: Optional[Dict[str, Any]] = None,
    ):
        """Create a RayConnector.

        Args:
            gravitino_uri: The URI of the Gravitino server.
            metalake: The metalake name.
            catalog: The catalog name.
            schema: The schema name.
            ray_options: Optional dict of Ray resource options forwarded to
                ``ray.remote`` (e.g. ``{"num_cpus": 2, "num_gpus": 1}``).

        Raises:
            ImportError: If the ``ray`` package is not installed.
        """
        super().__init__(gravitino_uri, metalake, catalog, schema)
        try:
            import ray  # noqa: F401 – availability check
        except ImportError as exc:
            raise ImportError(
                "The 'ray' package is required for RayConnector. "
                "Install it with: pip install ray"
            ) from exc
        self._ray_options: Dict[str, Any] = ray_options or {}

    @property
    def runtime_type(self) -> FunctionImpl.RuntimeType:
        """Returns the RAY runtime type."""
        return FunctionImpl.RuntimeType.RAY

    def _wrap_function(self, func: Callable, function_meta: Function) -> Any:
        """Wrap *func* as a ``ray.remote`` function.

        Args:
            func: The raw Python callable resolved from the Gravitino UDF.
            function_meta: The Gravitino function metadata.

        Returns:
            A Ray ``RemoteFunction`` that can be invoked with ``.remote()``.
        """
        import ray

        if self._ray_options:
            remote_fn = ray.remote(**self._ray_options)(func)
        else:
            remote_fn = ray.remote(func)
        logger.info(
            "Loaded Gravitino UDF '%s' as Ray remote function", function_meta.name()
        )
        return remote_fn
