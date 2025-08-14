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

import json
import logging

from mcp_server.client.plain.exception import GravitinoException


def extract_content_from_response(response, field: str, default="") -> str:
    response_json = response.json()
    _handle_gravitino_exception(response_json)
    return json.dumps(response_json.get(field, default))


def _handle_gravitino_exception(response: dict):
    error_code = response.get("code", 0)
    if error_code != 0:
        t = response.get("type", "")
        message = response.get("message", "")
        error_message = f"Error code: {error_code}, Error type: {t}, Error message: {message}"
        logging.warning(error_message)
        raise GravitinoException(error_message)
