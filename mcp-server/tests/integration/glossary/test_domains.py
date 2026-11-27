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

"""Domain membership, scoped lookup, and reverse binding contracts."""

import pytest

from tests.integration.glossary.client import (
    allow,
    binding_keys,
    by_alias,
    edit,
    names,
    segment,
)
from tests.integration.glossary.graph import (
    ALIAS,
    LEAF,
    MEMBERS,
    VISIBLE_BINDINGS,
    projected_domains,
    visible,
)


@pytest.mark.parametrize("user", ["admin", "sales", "no_retail", "manager"])
def test_domain_lookup_intersects_alias_status_visibility_and_targets(
    lake, user
):
    for domain, allowed_domains in [
        (None, None),
        ("Sales", {"Sales", "Retail", "Wholesale"}),
        ("Retail", {"Retail"}),
        ("Finance", {"Finance"}),
    ]:
        base = {
            "match": [ALIAS, LEAF, "nomatch"],
            "nameContains": "t",
            "status": "APPROVED",
            "bindings": "true",
        }
        if domain:
            base["domain"] = domain
            if domain not in (
                {"Sales", "Retail", "Finance"}
                if user == "admin"
                else (
                    {"Sales", "Retail"}
                    if user == "sales"
                    else {"Sales"} if user == "no_retail" else set()
                )
            ):
                error = lake.call(
                    "GET",
                    "terms",
                    user=user,
                    params={"details": "true", **base},
                    status=403,
                )
                assert error["type"] == "ForbiddenException"
                continue
        terms = lake.lookup(user, node=1, **base)
        expected = [
            n
            for n in MEMBERS
            if visible(n, user, "APPROVED")
            and "t" in n.lower()
            and (
                allowed_domains is None
                or set(projected_domains(n, user)) & allowed_domains
            )
        ]
        assert names(terms) == sorted(expected)
        for t in terms:
            n = t["name"]
            assert sorted(t["domains"]) == projected_domains(n, user)
            evidence = {(ALIAS, "SYNONYM")}
            if n == LEAF:
                evidence.add((LEAF, "NAME"))
            assert {(x["word"], x["field"]) for x in t["matchedBy"]} == evidence
            expected_bind = set()
            if n in (LEAF, "FinanceAlt") and user in ("admin", "sales"):
                expected_bind.update(VISIBLE_BINDINGS)
                if user == "admin":
                    expected_bind.add(("table", "dw.s.private_orders", None))
            assert binding_keys(t["bindings"]) == sorted(expected_bind)
            assert t["bindingsTruncated"] is False
        without = lake.lookup(user, **{**base, "bindings": "false"})
        assert names(without) == names(terms)
        assert all(
            "bindings" not in t and "bindingsTruncated" not in t
            for t in without
        )
    # Direct readability is independent of domain membership.
    assert sorted(
        lake.term("FinanceAlt", user=user)["domains"]
    ) == projected_domains("FinanceAlt", user)


def test_reverse_reads_project_hidden_domains_and_relations_in_each_child(lake):
    for user in ("admin", "sales"):
        for path, child in [
            ("table/dw.s.orders", "amount"),
            ("semantic_model/dw.s.metrics", "volume"),
        ]:
            data = lake.call(
                "GET",
                "objects/" + path + "/terms",
                user=user,
                params={
                    "details": "true",
                    "children": "true",
                    "status": "APPROVED",
                },
            )
            assert [c["name"] for c in data["children"]] == [child]
            assert names(data["children"][0]["terms"]) == sorted(
                (LEAF, "FinanceAlt")
            )
            assert names(data["terms"]) == (
                sorted((LEAF, "FinanceAlt"))
                if path.startswith("table/")
                else []
            )
            for t in data["terms"] + data["children"][0]["terms"]:
                assert sorted(t["domains"]) == projected_domains(
                    t["name"], user
                )
                if t["name"] == LEAF:
                    assert sorted(t["broader"]) == ["Left", "Right"]
                    assert t["related"] == ["LivePeer"]
        bindings = lake.call(
            "GET",
            "terms/" + segment(LEAF) + "/objects",
            user=user,
            params={
                "status": "APPROVED",
                "types": ["COLUMN", "SEMANTIC_MODEL"],
            },
        )["bindings"]
        assert binding_keys(bindings) == sorted(
            k for k in VISIBLE_BINDINGS if k[0] != "table"
        )
    lake.call(
        "GET",
        "objects/table/dw.s.private_orders/terms",
        user="sales",
        status=403,
    )
    assert sorted(lake.term(LEAF)["domains"]) == sorted(MEMBERS[LEAF])


def test_move_rename_detach_and_recreate_domain(lake):
    # The reader's only domain grant follows the parent rather than the term.
    src, dst, child, new = (
        "Origin",
        "Destination",
        "Channel-Online",
        "RenamedChannel",
    )
    lake.domain(src)
    lake.domain(dst)
    lake.domain(child, src)
    lake.role(
        "originreader",
        [
            ("METALAKE", lake.name, allow("VIEW_DEFINITION")),
            ("DOMAIN", src, allow("USE_DOMAIN")),
        ],
    )
    lake.role(
        "destreader",
        [
            ("METALAKE", lake.name, allow("VIEW_DEFINITION")),
            ("DOMAIN", dst, allow("USE_DOMAIN")),
        ],
    )
    alias = "moving channel"
    n = "ChannelTerm"
    lake.create(n, synonyms=[alias], domains=[child], broader=["Concept"])
    lake.call(
        "POST", "objects/column/dw.s.orders.amount/terms", {"termsToAdd": [n]}
    )

    def check(scope, parent, reader, outsider):
        assert by_alias(lake, alias, parent, user=reader, node=1) == [n]
        assert lake.term(n, user=reader, node=1)["domains"] == [scope]
        assert lake.term(n, user=outsider, node=1)["domains"] == []
        assert names(lake.hierarchy(n, "BROADER", node=1)) == ["Concept"]
        assert (
            len(lake.call("GET", "terms/" + n + "/objects", node=1)["bindings"])
            == 1
        )

    check(child, src, "originreader", "destreader")
    lake.call("PUT", "domains/" + segment(child) + "/parent", {"parent": dst})
    assert by_alias(lake, alias, src, node=1) == []
    check(child, dst, "destreader", "originreader")
    lake.call(
        "PUT",
        "domains/" + segment(child),
        {"updates": [edit("rename", newName=new)]},
    )
    assert lake.term(n, node=1)["domains"] == [new]
    lake.call("GET", "terms", params={"domain": child}, status=404, node=1)
    lake.domain(child, src, tag="channel_replacement")
    assert by_alias(lake, alias, child, node=1) == []
    check(new, dst, "destreader", "originreader")
    lake.call("DELETE", "domains/" + new + "/parent")
    assert by_alias(lake, alias, dst, node=1) == []
    assert lake.term(n, user="destreader", node=1)["domains"] == []
    assert by_alias(lake, alias, new, node=1) == [n]


def test_domain_classifier_and_glossary_bindings_are_independent(lake):
    root, child = "AssetDomain", "AssetChild"
    lake.domain(root)
    lake.domain(child, root)
    n = "DefinitionWithoutMembership"
    alias = "asset membership check"
    lake.create(n, synonyms=[alias], broader=["Concept"])
    lake.call("POST", "objects/table/dw.s.orders/terms", {"termsToAdd": [n]})
    lake.call(
        "POST",
        "objects/table/dw.s.orders/tags",
        {"tagsToAdd": ["tag_" + child], "tagsToRemove": []},
    )

    def members(domain):
        return {
            m["object"]["fullName"]: [r["relation"] for r in m["relations"]]
            for m in lake.call(
                "GET",
                "domains/" + domain + "/objects",
                params={"types": "TABLE"},
            )["members"]
        }

    assert members(child) == {"dw.s.orders": ["direct"]}
    assert members(root) == {"dw.s.orders": ["via_subdomain"]}
    assert lake.term(n)["domains"] == [] and by_alias(lake, alias, root) == []
    lake.alter(n, edit("addDomain", domain=child))
    assert by_alias(lake, alias, root) == [n]
    lake.call(
        "POST",
        "objects/table/dw.s.orders/tags",
        {"tagsToAdd": [], "tagsToRemove": ["tag_" + child]},
    )
    assert members(root) == {} and by_alias(lake, alias, root) == [n]
    assert (
        lake.call("GET", "terms/" + n + "/objects")["bindings"][0]["object"][
            "fullName"
        ]
        == "dw.s.orders"
    )
    lake.alter(n, edit("removeDomain", domain=child))
    assert by_alias(lake, alias, root) == []
    assert n in lake.call("GET", "objects/table/dw.s.orders/terms")["names"]


def test_nonempty_domain_guard_with_hidden_draft_and_steward(lake):
    d = "Embargo"
    n = "ConfidentialDraft"
    lake.domain(d)
    lake.role("steward", [("DOMAIN", d, allow("MANAGE_DOMAIN"))])
    lake.role(
        "author",
        [
            ("METALAKE", lake.name, allow("CREATE_DEFINITION")),
            ("DOMAIN", d, allow("USE_DOMAIN")),
        ],
    )
    before = lake.call(
        "POST",
        "terms",
        {"name": n, "domains": [d], "status": "DRAFT"},
        user="author",
    )["term"]
    error = lake.call("DELETE", "domains/" + d, user="steward", status=409)
    assert error["type"] == "NonEmptyDomainException"
    assert (
        n not in error["message"] and "membership" in error["message"].lower()
    )
    lake.alter(n, edit("removeDomain", domain=d), user="steward", status=404)
    assert (
        lake.term(n, node=1, user="author")["revisionToken"]
        == before["revisionToken"]
    )
    lake.alter(n, edit("removeDomain", domain=d), user="author")
    assert lake.call("DELETE", "domains/" + d, user="steward")["dropped"]
    after = lake.term(n, node=1, user="author")
    assert (after["status"], after["domains"], after["version"]) == (
        "DRAFT",
        [],
        before["version"] + 1,
    )


def assert_scoped_names(lake, root, expected):
    for user, expected_names in expected.items():
        scoped = lake.call(
            "GET", "terms", params={"domain": root}, user=user, node=1
        )
        assert sorted(scoped["names"]) == sorted(expected_names), user


def test_wide_domain_scope_shared_membership_and_hidden_children(lake):
    root = "WideScope"
    lake.domain(root)
    lake.role(
        "scope_reader",
        [
            ("METALAKE", lake.name, allow("VIEW_DEFINITION")),
            ("DOMAIN", root, allow("USE_DOMAIN")),
        ],
    )
    both = "WideScopeDuplicate"
    alias = "scope common"
    lake.create(both, domains=[root], synonyms=[alias])
    expected = {"admin": [both], "scope_reader": [both]}
    for i in range(501):
        domain = f"ScopeD{i:03d}"
        name = f"ScopeTerm{i:03d}"
        draft_domain = i == 497
        draft_term = i == 496
        lake.domain(domain, root, publish=not draft_domain)
        lake.create(
            name,
            status="DRAFT" if draft_term else "APPROVED",
            domains=[domain],
            synonyms=[alias] if i in (0, 497, 500) else [],
        )
        if i in (0, 500):
            lake.alter(both, edit("addDomain", domain=domain))
        expected["admin"].append(name)
        if not (draft_term or draft_domain):
            expected["scope_reader"].append(name)
        if i + 1 in (498, 499, 500, 501):
            assert_scoped_names(lake, root, expected)
    for user in expected:
        scoped = lake.lookup(
            user,
            node=1,
            domain=root,
            match=alias,
            status="APPROVED",
            bindings="true",
        )
        matched = [both, "ScopeTerm000", "ScopeTerm500"]
        if user == "admin":
            matched.append("ScopeTerm497")
        assert names(scoped) == sorted(matched)
        assert names(
            lake.lookup(user, node=1, domain=root, nameContains="Term500")
        ) == ["ScopeTerm500"]
        term = next(t for t in scoped if t["name"] == both)
        assert sorted(term["domains"]) == sorted(
            [root, "ScopeD000", "ScopeD500"]
        )
        assert term["bindings"] == []


def test_term_move_and_hidden_membership_survives_ordinary_edit(lake):
    n = "MovedDefinition"
    alias = "domain move check"
    first = lake.create(
        n,
        definition="one",
        domains=["Retail", "Finance"],
        synonyms=[alias],
        broader=["Concept"],
    )
    lake.call(
        "POST",
        "objects/semantic_model/dw.s.metrics/metrics/volume/terms",
        {"termsToAdd": [n]},
    )
    before = lake.term(n, node=1)
    assert before["version"] == first["version"] + 1
    r = lake.alter(
        n, edit("updateDefinition", newDefinition="two"), user="manager"
    )["term"]
    assert r["domains"] == [] and sorted(lake.term(n, node=1)["domains"]) == [
        "Finance",
        "Retail",
    ]
    assert by_alias(lake, alias, "Sales", user="sales", node=1) == [n]
    body = {
        "expectedRevision": r["revisionToken"],
        "updates": [
            edit("removeDomain", domain="Retail"),
            edit("addDomain", domain="Ops"),
        ],
    }
    moved = lake.call("PUT", "terms/" + n, body)["term"]
    assert moved["version"] == r["version"] + 1
    assert by_alias(lake, alias, "Sales", user="sales", node=1) == []
    assert by_alias(lake, alias, "Ops", node=1) == [n]
    assert by_alias(lake, alias, "Finance", node=1) == [n]
    assert lake.term(n, node=1, user="sales")["domains"] == []
    assert lake.term(n, node=1, user="sales")["broader"] == ["Concept"]
    assert (
        lake.call("PUT", "terms/" + n, body, status=409)["type"]
        == "TermVersionConflictException"
    )
    assert lake.term(n, node=1)["revisionToken"] == moved["revisionToken"]
    reverse = lake.call(
        "GET",
        "objects/semantic_model/dw.s.metrics/metrics/volume/terms",
        params={"details": "true"},
        user="sales",
        node=1,
    )
    term = next(t for t in reverse["terms"] if t["name"] == n)
    assert term["domains"] == [] and term["definition"] == "two"
