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

from typing import Dict, Optional

from gravitino.api.function.function_impl import FunctionImpl
from gravitino.api.function.function_resources import FunctionResources


class PythonImpl(FunctionImpl):
    """Python implementation with handler and optional inline code."""

    def __init__(
        self,
        runtime: FunctionImpl.RuntimeType,
        handler: str,
        code_block: Optional[str] = None,
        resources: Optional[FunctionResources] = None,
        properties: Optional[Dict[str, str]] = None,
    ):
        """Create a PythonImpl instance.

        Args:
            runtime: The runtime of the function implementation.
            handler: The handler entrypoint.
            code_block: The Python UDF code block.
            resources: The resources required by the function implementation.
            properties: The properties of the function implementation.

        Raises:
            ValueError: If handler is null or empty.
        """
        super().__init__(FunctionImpl.Language.PYTHON, runtime, resources, properties)
        if not handler or not handler.strip():
            raise ValueError("Python handler cannot be null or empty")
        self._handler = handler
        self._code_block = code_block

    def handler(self) -> str:
        """Returns the handler entrypoint."""
        return self._handler

    def code_block(self) -> Optional[str]:
        """Returns the Python UDF code block."""
        return self._code_block

    def __eq__(self, other) -> bool:
        if not isinstance(other, PythonImpl):
            return False
        return (
            self.language() == other.language()
            and self.runtime() == other.runtime()
            and self.resources() == other.resources()
            and self.properties() == other.properties()
            and self._handler == other._handler
            and self._code_block == other._code_block
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.language(),
                self.runtime(),
                self.resources(),
                tuple(sorted(self.properties().items())),
                self._handler,
                self._code_block,
            )
        )

    def __repr__(self) -> str:
        return (
            f"PythonImpl(language={self.language()}, runtime={self.runtime()}, "
            f"handler='{self._handler}', codeBlock='{self._code_block}', "
            f"resources={self.resources()}, properties={self.properties()})"
        )
