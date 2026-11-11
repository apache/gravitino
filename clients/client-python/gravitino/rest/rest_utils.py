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

import urllib.parse

from gravitino.exceptions.base import IllegalArgumentException


def validate_domain_name(name: str) -> None:
    if not isinstance(name, str) or not name.strip():
        raise IllegalArgumentException("Domain name must not be blank")
    if name in (".", ".."):
        raise IllegalArgumentException(f"Unsafe domain path segment: {name}")


def domain_path(metalake: str, name: str) -> str:
    validate_domain_name(name)
    return f"api/metalakes/{encode_string(metalake)}/domains/{encode_string(name)}"


def encode_string(to_encode: str):
    if to_encode is None:
        raise IllegalArgumentException("Invalid string to encode: None")

    return urllib.parse.quote(to_encode, safe="", encoding="utf-8")
