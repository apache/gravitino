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

import simplejson

from mcp_server.client.glossary_operation import GlossaryOperation
from mcp_server.client.plain.exception import GravitinoException
from mcp_server.client.plain.utils import encode_path_segment, response_content

_MAX_MATCH_WORDS = 20


def _binding(binding: dict) -> dict:
    """Project a REST binding into the coordinates of its data object."""
    target = binding["object"]
    target_type = target["type"].upper()
    fields = {
        "CATALOG": ("catalog",),
        "SCHEMA": ("catalog", "schema"),
        "COLUMN": ("catalog", "schema", "name", "column"),
    }.get(target_type, ("catalog", "schema", "name"))
    parts = target["fullName"].split(".", len(fields) - 1)
    result = {
        "kind": binding["kind"].upper(),
        "type": target_type,
        **dict(zip(fields, parts, strict=True)),
    }
    if "metric" in binding:
        result["metric"] = binding["metric"]
    return result


def _match(term: dict, include_bindings: bool) -> dict:
    match = {
        "term": term["name"],
        "definition": term.get("definition"),
        "domains": term["domains"],
        "broader": term["broader"],
        "related": term["related"],
    }
    if include_bindings:
        match["bindings"] = [_binding(binding) for binding in term["bindings"]]
        match["bindings_truncated"] = term["bindingsTruncated"]
    return match


class PlainRESTClientGlossaryOperation(GlossaryOperation):
    """Read glossary data through the current principal's REST client."""

    def __init__(self, metalake_name: str | None, rest_client):
        self._metalake_name = metalake_name
        self._rest_client = rest_client

    async def _get(self, path: str, **params) -> dict:
        if not self._metalake_name or not self._metalake_name.strip():
            raise ValueError("Select a metalake before using Glossary tools")
        query = {
            key: value for key, value in params.items() if value is not None
        }
        response = await self._rest_client.get(
            f"/api/metalakes/{encode_path_segment(self._metalake_name)}/{path}",
            params={"status": "APPROVED", **query},
        )
        payload = response_content(response)
        if not isinstance(payload, dict) or "code" not in payload:
            raise GravitinoException("Invalid Gravitino response: missing code")
        return payload

    async def resolve_business_term(
        self,
        words: list[str],
        domain: str | None = None,
        include_bindings: bool = True,
    ) -> str:
        if not words or any(not word.strip() for word in words):
            raise ValueError(
                "Provide at least one business phrase; phrases must not be blank"
            )
        matches: dict[str, list] = {word: [] for word in words}
        distinct = list(matches)
        for start in range(0, len(distinct), _MAX_MATCH_WORDS):
            payload = await self._get(
                "terms",
                match=distinct[start : start + _MAX_MATCH_WORDS],
                domain=domain,
                details="true",
                bindings=str(include_bindings).lower(),
            )
            for term in payload["terms"]:
                match = _match(term, include_bindings)
                for matched_by in term["matchedBy"]:
                    matches[matched_by["word"]].append(
                        {**match, "matched_by": matched_by["field"]}
                    )

        return simplejson.dumps(
            {
                "results": [
                    {"word": word, "matches": matches[word]}
                    for word in words
                    if matches[word]
                ],
                "unmatched": [word for word in words if not matches[word]],
            }
        )

    async def get_term(self, term: str) -> str:
        payload = await self._get(f"terms/{encode_path_segment(term)}")
        return simplejson.dumps(payload["term"])

    async def list_terms(
        self, domain: str | None = None, name_contains: str | None = None
    ) -> str:
        payload = await self._get(
            "terms", details="true", domain=domain, nameContains=name_contains
        )
        return simplejson.dumps(payload["terms"])

    async def get_term_hierarchy(
        self, term: str, direction: str = "NARROWER"
    ) -> str:
        payload = await self._get(
            f"terms/{encode_path_segment(term)}/hierarchy", direction=direction
        )
        return simplejson.dumps(payload["terms"])

    async def list_metadata_by_term(
        self, term: str, kind: str | None = None, types: list[str] | None = None
    ) -> str:
        payload = await self._get(
            f"terms/{encode_path_segment(term)}/objects", kind=kind, types=types
        )
        return simplejson.dumps(
            [_binding(binding) for binding in payload["bindings"]]
        )

    async def list_terms_for_metadata(
        self, metadata_full_name: str, metadata_type: str
    ) -> str:
        children = metadata_type.upper() in ("TABLE", "SEMANTIC_MODEL")
        payload = await self._get(
            f"objects/{encode_path_segment(metadata_type)}/{encode_path_segment(metadata_full_name)}/terms",
            details="true",
            children="true" if children else None,
        )
        return simplejson.dumps(
            {"terms": payload["terms"], "children": payload["children"]}
        )
