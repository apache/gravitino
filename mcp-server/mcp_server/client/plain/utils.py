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
from urllib.parse import quote

import simplejson

from mcp_server.client.plain.exception import GravitinoException


def encode_path_segment(segment: str) -> str:
    """Validate and URL-encode a single REST path segment."""
    segment = str(segment)
    # HTTP clients normalize dot segments, which can target another resource.
    if segment in ("", ".", ".."):
        raise ValueError("REST path names must not be empty, '.' or '..'")
    return quote(segment, safe="")


def extract_content_from_response(response, field: str, default="") -> str:
    response_json = response.json()
    _handle_gravitino_exception(response_json)
    return json.dumps(response_json.get(field, default))


def extract_list_from_response(response, field: str) -> str:
    """Extract a required JSON array without rounding fractional numbers.

    Propagate server errors and reject malformed envelopes instead of returning
    an empty result for a failed discovery request.
    """
    try:
        payload = simplejson.loads(response.content, use_decimal=True)
    except simplejson.JSONDecodeError:
        response.raise_for_status()
        raise
    if isinstance(payload, dict):
        _handle_gravitino_exception(payload)
    response.raise_for_status()
    if not isinstance(payload, dict) or "code" not in payload:
        raise GravitinoException("Invalid Gravitino response: missing code")
    if not isinstance(payload.get(field), list):
        raise GravitinoException(
            f"Invalid Gravitino response: {field} must be a JSON array"
        )
    return simplejson.dumps(payload[field], use_decimal=True)


def extract_response(response) -> str:
    response_json = response.json()
    _handle_gravitino_exception(response_json)
    return json.dumps(response_json)


def _handle_gravitino_exception(response: dict):
    error_code = response.get("code", 0)
    if error_code != 0:
        t = response.get("type", "")
        message = response.get("message", "")
        error_message = f"Error code: {error_code}, Error type: {t}, Error message: {message}"
        logging.warning(error_message)
        raise GravitinoException(error_message)
