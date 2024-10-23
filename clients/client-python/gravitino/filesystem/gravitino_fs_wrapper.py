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

from fsspec.implementations.arrow import ArrowFSWrapper
from fsspec.utils import infer_storage_options

from gravitino.exceptions.base import GravitinoRuntimeException


# This class is a wrapper for ArrowFSWrapper, which is only used for OSS.
class GravitinoArrowFSWrapper(ArrowFSWrapper):
    @classmethod
    def _strip_protocol(cls, path):
        ops = infer_storage_options(path)
        path = ops["path"]

        if path.startswith("//"):
            # special case for "hdfs://path" (without the triple slash)
            path = path[1:]
        elif path.startswith("/") and ops["protocol"] == "oss":
            # special case for "oss://path" (with the single slash)
            # ops['host'] is bucket name, path is '/path'
            path = ops["host"] + path
        return path

    def _rm(self, path):
        raise GravitinoRuntimeException(
            "Deprecated method, use `rm_file` method instead."
        )

    def created(self, path):
        raise GravitinoRuntimeException("Created is not implemented for OSS.")

    def sign(self, path, expiration=100, **kwargs):
        raise GravitinoRuntimeException("Sign is not implemented for OSS.")
