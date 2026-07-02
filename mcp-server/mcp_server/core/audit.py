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

import base64
import binascii
import json
import logging
from datetime import datetime, timezone

_audit_logger = logging.getLogger("gravitino.mcp.audit")


def _extract_principal(authorization: str) -> str:
    """Derive a display principal from a raw Authorization header value.

    - "Basic <base64(user:secret)>" → "<user>"  (Gravitino simple auth)
    - "Bearer <token>"              → "bearer:<first-8-chars-of-token>"
    - empty / missing / unparsable  → "anonymous"
    """
    if not authorization:
        return "anonymous"
    parts = authorization.split()
    if len(parts) != 2:
        return "anonymous"
    scheme, credential = parts[0].lower(), parts[1]
    if scheme == "basic":
        try:
            decoded = base64.b64decode(credential, validate=True).decode(
                "utf-8"
            )
        except (binascii.Error, UnicodeDecodeError, ValueError):
            return "anonymous"
        user = decoded.split(":", 1)[0]
        return user if user else "anonymous"
    if scheme == "bearer":
        return f"bearer:{credential[:8]}"
    return "anonymous"


def emit(
    *,
    principal: str,
    tool: str,
    outcome: str,
    error_type: str = "",
) -> None:
    """Write one structured JSON audit record to the audit logger.

    Args:
        principal: Identity derived from the request (e.g. "bearer:abc12345" or "anonymous").
        tool:      MCP tool name that was invoked.
        outcome:   "allow" for successful calls, "deny" for failed calls. Note the
                   AuditMiddleware emits "deny" for any tool-call exception (an
                   authorization denial being the common case), not only
                   authorization failures; inspect error_type to disambiguate.
        error_type: Exception class name when outcome is "deny", empty otherwise.
    """
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "principal": principal,
        "tool": tool,
        "outcome": outcome,
    }
    if error_type:
        record["error_type"] = error_type

    _audit_logger.info(json.dumps(record))
