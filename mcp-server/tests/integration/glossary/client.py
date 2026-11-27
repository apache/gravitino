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

"""HTTP helpers for the live glossary contract tests."""

from urllib.parse import quote
from uuid import uuid4

import httpx

from tests.integration.gravitino_setup import basic_auth_header


def allow(*names_):
    return [(p, "ALLOW") for p in names_]


def auth(user):
    return {"Authorization": basic_auth_header(user)}


def segment(name):
    return quote(name, safe="")


def edit(kind, **fields):
    return {"@type": kind, **fields}


def names(terms):
    return sorted(t["name"] for t in terms)


class Lake:
    """REST client for a single test metalake on two Gravitino servers."""

    def __init__(self, primary, peer):
        self.name = "domain_qa_" + uuid4().hex[:12]
        self.urls = [
            uri.rstrip("/") + "/api/metalakes" for uri in (primary, peer)
        ]
        self.http = httpx.Client(timeout=60, trust_env=False)

    def send(
        self, method, path, body=None, *, params=None, user="admin", node=0
    ):
        url = self.urls[node] + "/" + self.name + ("/" + path if path else "")
        response = self.http.request(
            method, url, json=body, params=params, headers=auth(user)
        )
        data = response.json()
        return response.status_code, data

    def call(self, method, path, body=None, *, status=200, **kwargs):
        code, data = self.send(method, path, body, **kwargs)
        assert code == status, (
            method,
            path,
            code,
            data.get("type"),
            data.get("message"),
        )
        return data

    def create(self, name, **fields):
        return self.call("POST", "terms", {"name": name, **fields})["term"]

    def term(self, name, **kwargs):
        return self.call("GET", "terms/" + segment(name), **kwargs)["term"]

    def alter(self, name, *ops, **kwargs):
        return self.call(
            "PUT", "terms/" + segment(name), {"updates": list(ops)}, **kwargs
        )

    def hierarchy(self, name, direction, user="admin", status=None, *, node=0):
        params = {"direction": direction}
        if status:
            params["status"] = status
        return self.call(
            "GET",
            "terms/" + segment(name) + "/hierarchy",
            params=params,
            user=user,
            node=node,
        )["terms"]

    def domain(self, name, parent=None, publish=True, *, tag=None):
        data = {
            "name": name,
            "backingTag": {"name": tag or "tag_" + name, "create": True},
        }
        if parent:
            data["parent"] = parent
        self.call("POST", "domains", data)
        if publish:
            self.call(
                "PATCH", "domains/" + segment(name), {"status": "published"}
            )

    def role(self, user, grants):
        self.call("POST", "users", {"name": user})
        objects = [
            {
                "type": kind,
                "fullName": name,
                "privileges": [
                    {"name": p, "condition": condition}
                    for p, condition in privileges
                ],
            }
            for kind, name, privileges in grants
        ]
        self.call(
            "POST",
            "roles",
            {"name": user, "properties": {}, "securableObjects": objects},
        )
        self.call(
            "PUT", f"permissions/users/{user}/grant", {"roleNames": [user]}
        )

    def lookup(self, user="admin", node=0, **query):
        return self.call(
            "GET",
            "terms",
            params={"details": "true", **query},
            user=user,
            node=node,
        )["terms"]


def by_alias(lake, alias, domain=None, **kwargs):
    params = {"match": alias, "status": "APPROVED"}
    if domain:
        params["domain"] = domain
    return names(lake.lookup(**kwargs, **params))


def binding_keys(items):
    return sorted(
        (x["object"]["type"], x["object"]["fullName"], x.get("metric"))
        for x in items
    )
