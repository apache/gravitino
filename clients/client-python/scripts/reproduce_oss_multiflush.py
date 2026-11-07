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

"""Reproduce the OSSFile append-position defect without cloud credentials.

Install ossfs 2025.5.0 and the fsspec version to inspect, then run this file.
For example: python -m pip install ossfs==2025.5.0 fsspec==2026.2.0
The known defect raises AssertionError (exit 1); correct writes finish with exit 0.
JSON output records library versions and append positions. The in-memory backend
isolates file buffering; it does not validate OSS HTTP, storage or authorization.
"""

from importlib.metadata import version
import json
import platform
from unittest.mock import patch
from uuid import uuid4

from fsspec.implementations.memory import MemoryFileSystem
from ossfs.file import OSSFile


def main() -> None:
    """Write two buffers through the real OSSFile and report the requests."""
    backend = MemoryFileSystem()
    content = bytearray()
    requests = []

    def append_object(_path: str, position: int, data: bytes) -> int:
        expected = len(content)
        requests.append({"position": position, "expected": expected, "size": len(data)})
        if position != expected:
            raise AssertionError(f"Expected append position {expected}, got {position}")
        content.extend(data)
        return len(content)

    try:
        with patch.object(backend, "append_object", append_object, create=True):
            with OSSFile(
                backend, f"oss-multiflush-{uuid4().hex}", mode="wb", block_size=3
            ) as stream:
                stream.write(b"abc")
                stream.write(b"de")
    finally:
        print(
            json.dumps(
                {
                    "python": platform.python_version(),
                    "ossfs": version("ossfs"),
                    "fsspec": version("fsspec"),
                    "requests": requests,
                }
            ),
            flush=True,
        )


if __name__ == "__main__":
    main()
