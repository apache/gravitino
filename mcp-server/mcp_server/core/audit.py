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
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pydantic import TypeAdapter

_audit_logger = logging.getLogger("gravitino.mcp.audit")
_VERSION = TypeAdapter(int)


@dataclass(frozen=True)
class _WriteTargetSpec:
    """Describes how to extract a target object's type and name from tool args."""

    target_type: str
    name_fields: tuple[str, ...]
    include_metalake: bool = True
    type_field: str = ""


# Every mutating (write / delete / action) MCP tool and how to derive the
# target metadata object from its arguments for audit records.
WRITE_TARGET_SPECS: dict[str, _WriteTargetSpec] = {
    # Catalogs
    "create_catalog": _WriteTargetSpec("catalog", ("name",)),
    "alter_catalog": _WriteTargetSpec("catalog", ("catalog_name",)),
    "drop_catalog": _WriteTargetSpec("catalog", ("catalog_name",)),
    "set_catalog_in_use": _WriteTargetSpec("catalog", ("catalog_name",)),
    # Schemas
    "create_schema": _WriteTargetSpec("schema", ("catalog_name", "name")),
    "alter_schema": _WriteTargetSpec("schema", ("catalog_name", "schema_name")),
    "drop_schema": _WriteTargetSpec("schema", ("catalog_name", "schema_name")),
    # Tables
    "create_table": _WriteTargetSpec(
        "table", ("catalog_name", "schema_name", "name")
    ),
    "alter_table": _WriteTargetSpec(
        "table", ("catalog_name", "schema_name", "table_name")
    ),
    "drop_table": _WriteTargetSpec(
        "table", ("catalog_name", "schema_name", "table_name")
    ),
    # Filesets
    "create_fileset": _WriteTargetSpec(
        "fileset", ("catalog_name", "schema_name", "name")
    ),
    "alter_fileset": _WriteTargetSpec(
        "fileset", ("catalog_name", "schema_name", "fileset_name")
    ),
    "drop_fileset": _WriteTargetSpec(
        "fileset", ("catalog_name", "schema_name", "fileset_name")
    ),
    # Topics
    "create_topic": _WriteTargetSpec(
        "topic", ("catalog_name", "schema_name", "name")
    ),
    "alter_topic": _WriteTargetSpec(
        "topic", ("catalog_name", "schema_name", "topic_name")
    ),
    "delete_topic": _WriteTargetSpec(
        "topic", ("catalog_name", "schema_name", "topic_name")
    ),
    # Models and model versions
    "register_model": _WriteTargetSpec(
        "model", ("catalog_name", "schema_name", "name")
    ),
    "alter_model": _WriteTargetSpec(
        "model", ("catalog_name", "schema_name", "model_name")
    ),
    "delete_model": _WriteTargetSpec(
        "model", ("catalog_name", "schema_name", "model_name")
    ),
    # The server assigns the version number after the call. The request names its parent.
    "link_model_version": _WriteTargetSpec(
        "model", ("catalog_name", "schema_name", "model_name")
    ),
    "alter_model_version": _WriteTargetSpec(
        "model_version",
        ("catalog_name", "schema_name", "model_name", "version"),
    ),
    "delete_model_version": _WriteTargetSpec(
        "model_version",
        ("catalog_name", "schema_name", "model_name", "version"),
    ),
    "alter_model_version_by_alias": _WriteTargetSpec(
        "model_version",
        ("catalog_name", "schema_name", "model_name", "alias"),
    ),
    "delete_model_version_by_alias": _WriteTargetSpec(
        "model_version",
        ("catalog_name", "schema_name", "model_name", "alias"),
    ),
    # Semantic models
    "create_semantic_model": _WriteTargetSpec(
        "semantic_model", ("catalog_name", "schema_name", "name")
    ),
    "alter_semantic_model": _WriteTargetSpec(
        "semantic_model",
        ("catalog_name", "schema_name", "semantic_model_name"),
    ),
    "drop_semantic_model": _WriteTargetSpec(
        "semantic_model",
        ("catalog_name", "schema_name", "semantic_model_name"),
    ),
    # Tags and tag associations
    "create_tag": _WriteTargetSpec("tag", ("name",)),
    "alter_tag": _WriteTargetSpec("tag", ("name",)),
    "delete_tag": _WriteTargetSpec("tag", ("name",)),
    "associate_tag_with_metadata": _WriteTargetSpec(
        "", ("metadata_full_name",), type_field="metadata_type"
    ),
    "disassociate_tag_from_metadata": _WriteTargetSpec(
        "", ("metadata_full_name",), type_field="metadata_type"
    ),
    # Policies on tags
    "associate_policy_with_tag": _WriteTargetSpec("tag", ("tag_name",)),
    "disassociate_policy_from_tag": _WriteTargetSpec("tag", ("tag_name",)),
    # Jobs
    "run_job": _WriteTargetSpec("job_template", ("job_template_name",)),
    "cancel_job": _WriteTargetSpec("job", ("job_id",), include_metalake=False),
}


def _format_target_part(field: str, value: Any) -> str:
    if field == "version":
        # FastMCP validates this argument as an int before constructing the REST path.
        try:
            return str(_VERSION.validate_python(value))
        except ValueError:
            return ""
    return value if isinstance(value, str) else ""


def extract_write_target(
    tool: str, arguments: Any, metalake: str = ""
) -> tuple[str, str]:
    """Extract ``(target_type, target_name)`` for write/delete MCP tools.

    Never raises: runs on both success and failure audit paths, including when
    a call failed validation because arguments were missing or malformed.
    Returns ``("", "")`` for read-only tools or when required target fields are
    absent or invalid, including an unresolved tenant for a scoped target.
    """
    spec = WRITE_TARGET_SPECS.get(tool)
    if spec is None or not isinstance(arguments, dict):
        return "", ""

    if spec.type_field:
        raw_type = arguments.get(spec.type_field)
        if not isinstance(raw_type, str) or not raw_type.strip():
            return "", ""
        target_type = raw_type.strip().lower()
    else:
        target_type = spec.target_type

    parts: list[str] = []
    for field in spec.name_fields:
        part = _format_target_part(field, arguments.get(field))
        if not part:
            return "", ""
        parts.append(part)

    if spec.include_metalake and target_type != "metalake":
        if not metalake:
            return "", ""
        parts.insert(0, metalake)
    return target_type, ".".join(parts)


def _extract_principal(authorization: str) -> str:
    """Derive a display principal from a raw Authorization header value.

    - "Basic <base64(user:secret)>" → "<user>"  (Gravitino simple auth)
    - "Bearer <token>"              → "bearer:<sha256(token)[:16]>"
    - "<scheme> <credential>"       → "<scheme>:<sha256(credential)[:16]>"
    - empty / missing / unparsable  → "anonymous"

    The credential may itself contain spaces (a custom scheme is free to use a
    comma-separated parameter list), so only the scheme is split off. Falling
    back to the scheme name keeps a static custom-scheme identity attributable
    in the audit log instead of recording it as anonymous.
    """
    if not authorization:
        return "anonymous"
    parts = authorization.split(None, 1)
    if len(parts) != 2:
        return "anonymous"
    scheme, credential = parts[0].lower(), parts[1].strip()
    if not credential:
        return "anonymous"
    if scheme == "basic":
        try:
            decoded = base64.b64decode(credential, validate=True).decode(
                "utf-8"
            )
        except (binascii.Error, UnicodeDecodeError, ValueError):
            return "anonymous"
        user = decoded.split(":", 1)[0]
        return user if user else "anonymous"
    digest = hashlib.sha256(credential.encode("utf-8", "surrogatepass"))
    return f"{scheme}:{digest.hexdigest()[:16]}"


def emit(
    *,
    principal: str,
    tool: str,
    outcome: str,
    error_type: str = "",
    error_category: str = "",
    metalake: str = "",
    target_type: str = "",
    target_name: str = "",
) -> None:
    """Write one structured JSON audit record to the audit logger.

    Args:
        principal: Identity derived from the request (e.g. "bearer:f39dac6cbaba535e" or "anonymous").
        tool:      MCP tool name that was invoked.
        outcome:   "allow" for successful calls, "deny" for authentication or
                   authorization denials (Gravitino 401/403, PermissionError, or
                   --no-service-identity-fallback rejection), and "error" for
                   all other failures.
        error_type: Original Gravitino error type (e.g. "ForbiddenException",
                    "NoSuchTableException") or exception class name (e.g.
                    "ConnectError", "ValueError") when outcome is "deny" or
                    "error"; empty on "allow".
        error_category: Normalized failure category ("unauthorized",
                        "permission_denied", "invalid_params", "not_found",
                        "conflict", "timeout", "rate_limited", "unavailable", "internal_error")
                        when outcome is "deny" or "error"; empty on "allow".
        metalake:  Metalake the call operated on, resolved - so a call that
                   relied on the server default records that default. Empty
                   when the tenant could not be resolved or when the tool
                   spans tenants, such as the metalake listing.
        target_type: Metadata object type for write/delete tools (e.g.
                     "table", "schema", "catalog"); empty for read-only tools
                     and unresolved targets.
        target_name: Target object full name for write/delete tools (e.g.
                     "authz.mcpcat.alice_s"); empty when target_type is empty.
    """
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "principal": principal,
        "tool": tool,
        "outcome": outcome,
    }
    if metalake:
        record["metalake"] = metalake
    if target_type:
        record["target_type"] = target_type
    if target_name:
        record["target_name"] = target_name
    if error_type:
        record["error_type"] = error_type
    if error_category:
        record["error_category"] = error_category

    _audit_logger.info(json.dumps(record))
