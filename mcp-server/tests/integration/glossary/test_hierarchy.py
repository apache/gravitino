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

"""Reachability, visibility, and atomic updates in glossary hierarchies."""

import random

import pytest

from tests.integration.glossary.client import (
    allow,
    by_alias,
    edit,
    names,
    segment,
)
from tests.integration.glossary.graph import (
    LEAF,
    PARENTS,
    PEERS,
    projected_domains,
    reachable,
    visible,
)


@pytest.mark.parametrize("user", ["admin", "manager", "sales", "no_retail"])
@pytest.mark.parametrize("status", [None, "APPROVED"])
def test_hierarchy_reachability_and_nested_projection(lake, user, status):
    for root, direction in [
        (LEAF, "BROADER"),
        ("Concept", "NARROWER"),
        ("OldOnlyRoot", "NARROWER"),
        ("DraftOnlyRoot", "NARROWER"),
    ]:
        terms = lake.hierarchy(root, direction, user, status, node=1)
        expected = reachable(
            root, direction, PARENTS, lambda n: visible(n, user, status)
        )
        assert names(terms) == expected
        for t in terms:
            n = t["name"]
            assert sorted(t["domains"]) == projected_domains(n, user)
            assert sorted(t["broader"]) == sorted(
                p for p in PARENTS[n] if visible(p, user, status)
            )
            assert sorted(t["narrower"]) == sorted(
                c
                for c, parents in PARENTS.items()
                if n in parents and visible(c, user, status)
            )
            assert sorted(t["related"]) == sorted(
                k for k in PEERS if n == LEAF and visible(k, user, status)
            )


@pytest.mark.parametrize("reverse_storage_order", [False, True])
def test_cross_cut_related_conflict_rolls_back_mixed_changes(
    lake, reverse_storage_order
):
    # x -> a and b -> y. A proposed a -> b would make x and y ancestor/descendant.
    suffix = str(int(reverse_storage_order))
    x, a, b, y = ["Cut" + p + suffix for p in "XABY"]
    for name in ([x, y] if reverse_storage_order else [y, x]) + [a, b]:
        lake.create(name, definition="original", domains=["Retail"])
    lake.alter(x, edit("addBroader", broader=a))
    lake.alter(b, edit("addBroader", broader=y))
    lake.alter(
        y if reverse_storage_order else x,
        edit("addRelated", related=x if reverse_storage_order else y),
    )
    lake.call(
        "POST", "objects/column/dw.s.orders.amount/terms", {"termsToAdd": [a]}
    )
    before = {n: lake.term(n) for n in (x, a, b, y)}
    lake.alter(
        a,
        edit("setProperty", property="marker", value="bad"),
        edit("addSynonym", synonym="ghost" + suffix),
        edit("addDomain", domain="Finance"),
        edit("rename", newName="FailedRename" + suffix),
        edit("addBroader", broader=b),
        status=400,
    )
    after = {n: lake.term(n, node=1) for n in (x, a, b, y)}
    assert before == after
    assert by_alias(lake, "ghost" + suffix, node=1) == []
    lake.call("GET", "terms/FailedRename" + suffix, node=1, status=404)
    assert (
        a
        in lake.call("GET", "objects/column/dw.s.orders.amount/terms", node=1)[
            "names"
        ]
    )
    assert names(lake.hierarchy(x, "BROADER", node=1)) == [a]
    lake.alter(x, edit("removeRelated", related=y))
    lake.alter(
        a, edit("addBroader", broader=b), edit("addDomain", domain="Finance")
    )
    assert names(lake.hierarchy(x, "BROADER", node=1)) == sorted([a, b, y])
    lake.alter(y, edit("addRelated", related=x), status=400)


def test_cleanup_multi_parent_domain_and_reverse_bindings(lake):
    n, parent, other, peer, child = (
        "Cleanup/Term",
        "CleanupP",
        "CleanupQ",
        "CleanupPeer",
        "CleanupChild",
    )
    for p in (parent, other, peer):
        lake.create(p)
    lake.create(
        n, broader=[parent, other], related=[peer], domains=["Sales", "Retail"]
    )
    lake.create(child, broader=[n, other], domains=["Retail"])
    target = "column/dw.s.orders.amount"
    lake.call(
        "POST", "objects/" + target + "/terms", {"termsToAdd": [n, child]}
    )
    assert names(lake.hierarchy(child, "BROADER")) == sorted([n, parent, other])
    before = {k: lake.term(k)["version"] for k in (parent, other, child, peer)}
    lake.call("DELETE", "terms/" + segment(n))
    for k in before:
        assert lake.term(k, node=1)["version"] == before[k] + int(
            k in (child, peer)
        )
    assert lake.term(peer, node=1)["related"] == []
    assert lake.term(child, node=1)["broader"] == [other]
    assert names(lake.hierarchy(child, "BROADER", node=1)) == [other]
    assert names(lake.hierarchy(parent, "NARROWER", node=1)) == []
    assert by_alias(lake, n, "Sales", node=1) == []
    reverse = lake.call("GET", "objects/" + target + "/terms", node=1)["names"]
    assert n not in reverse and child in reverse
    lake.create(n)
    assert lake.term(n, node=1)["domains"] == []
    assert (
        lake.call("GET", "terms/" + segment(n) + "/objects", node=1)["bindings"]
        == []
    )
    assert lake.hierarchy(n, "NARROWER", node=1) == []


def test_depth_of_join_includes_descendants_and_ancestors(lake):
    # Default public API depth limit on the tested revision is 20.
    for i in range(11):
        lake.create(
            "Lower" + str(i), broader=["Lower" + str(i - 1)] if i else []
        )
    for i in range(11):
        lake.create(
            "Upper" + str(i), broader=["Upper" + str(i - 1)] if i else []
        )
    before = lake.term("Lower0")
    lake.alter(
        "Lower0",
        edit("addDomain", domain="Retail"),
        edit("addBroader", broader="Upper10"),
        status=400,
    )
    assert before == lake.term("Lower0", node=1)
    lake.alter("Lower0", edit("addBroader", broader="Upper9"))
    assert len(lake.hierarchy("Lower10", "BROADER", node=1)) == 20
    lake.alter("Upper0", edit("addBroader", broader="Lower10"), status=400)
    assert len(lake.hierarchy("Lower10", "BROADER", node=1)) == 20


@pytest.mark.parametrize("seed", [14913, 15402])
def test_seeded_graph_against_independent_reachability_model(lake, seed):
    rng = random.Random(seed)
    nodes = [f"N{seed}_{i}" for i in range(32)]
    edges = {}
    statuses = {}
    for i, n in enumerate(nodes):
        pool = [p for j, p in enumerate(nodes[:i]) if j // 4 < i // 4]
        edges[n] = rng.sample(pool, min(len(pool), rng.randint(1, 3)))
        statuses[n] = rng.choice(
            ["APPROVED", "APPROVED", "APPROVED", "DRAFT", "DEPRECATED"]
        )
        lake.create(
            n,
            broader=edges[n],
            status=statuses[n],
            domains=["Finance", "Retail"] if i % 3 else ["Wholesale"],
        )
    for user, filter_status in (
        ("admin", None),
        ("sales", None),
        ("sales", "APPROVED"),
        ("manager", "APPROVED"),
    ):

        assert_seeded_hierarchy(lake, edges, statuses, user, filter_status)


def assert_seeded_hierarchy(lake, edges, statuses, user, filter_status):
    def allowed(name):
        return (user != "sales" or statuses[name] != "DRAFT") and (
            not filter_status or statuses[name] == filter_status
        )

    for root in edges:
        for direction in ("BROADER", "NARROWER"):
            if not allowed(root):
                params = {"direction": direction}
                if filter_status:
                    params["status"] = filter_status
                lake.call(
                    "GET",
                    "terms/" + root + "/hierarchy",
                    params=params,
                    user=user,
                    node=1,
                    status=404,
                )
                continue
            expected = reachable(root, direction, edges, allowed)
            rows = lake.hierarchy(root, direction, user, filter_status, node=1)
            assert names(rows) == expected, (
                root,
                direction,
                user,
                filter_status,
            )
            for row in rows:
                assert sorted(row["broader"]) == sorted(
                    p for p in edges[row["name"]] if allowed(p)
                )


def test_frontier_batch_boundaries(lake):
    top = "WideTop"
    lake.create(top)
    expected = []
    for i in range(501):
        mid = f"WideMid{i:03d}"
        tip = f"WideTip{i:03d}"
        lake.create(mid, broader=[top])
        lake.create(tip, broader=[mid])
        expected.extend([mid, tip])
        if i + 1 in (499, 500, 501):
            assert names(lake.hierarchy(top, "NARROWER", node=1)) == sorted(
                expected
            )
    lake.create("WideJoint", broader=["WideTip000", "WideTip500"])
    assert names(lake.hierarchy(top, "NARROWER", node=1)) == sorted(
        expected + ["WideJoint"]
    )
    assert names(lake.hierarchy("WideJoint", "BROADER", node=1)) == sorted(
        [top, "WideMid000", "WideTip000", "WideMid500", "WideTip500"]
    )


def test_rejected_proposed_parent_from_invisible_draft(lake):
    n = "OwnWriterTerm"
    lake.role(
        "graph_writer",
        [
            ("METALAKE", lake.name, allow("CREATE_DEFINITION")),
            ("DOMAIN", "Sales", allow("USE_DOMAIN")),
        ],
    )
    before = lake.call(
        "POST",
        "terms",
        {"name": n, "domains": ["Retail"], "broader": ["Left"]},
        user="graph_writer",
    )["term"]
    for kind, key in (("addBroader", "broader"), ("addRelated", "related")):
        body = {
            "updates": [
                edit("setProperty", property="added", value="no"),
                edit("addDomain", domain="Sales"),
                edit(kind, **{key: "DraftPeer"}),
            ]
        }
        err = lake.call(
            "PUT", "terms/" + n, body, user="graph_writer", status=404
        )
        assert err["type"] == "NoSuchTermException"
        assert lake.term(n, node=1, user="graph_writer") == before
    assert names(lake.hierarchy(n, "BROADER", user="graph_writer", node=1)) == [
        "Concept",
        "Left",
    ]


@pytest.mark.parametrize("status", ["DRAFT", "DEPRECATED"])
def test_status_filtered_subgraph_after_parent_changes_state(lake, status):
    prefix = "State" + status
    root, via, barrier, leaf, other = [
        prefix + s for s in ["Root", "Via", "Barrier", "Leaf", "Other"]
    ]
    for n, parents, current in [
        (root, [], status),
        (other, [], status),
        (via, [root], status),
        (barrier, [other], "APPROVED"),
        (leaf, [via, barrier], status),
    ]:
        lake.create(
            n, broader=parents, status=current, domains=["Retail", "Finance"]
        )
    lake.call(
        "POST",
        "objects/column/dw.s.orders.amount/terms",
        {"termsToAdd": [leaf]},
    )
    assert names(
        lake.hierarchy(leaf, "BROADER", "manager", status, node=1)
    ) == sorted([via, root])
    assert names(
        lake.hierarchy(root, "NARROWER", "manager", status, node=1)
    ) == sorted([via, leaf])
    assert lake.hierarchy(other, "NARROWER", "manager", status, node=1) == []
    if status == "DRAFT":
        lake.call(
            "GET",
            "terms/" + leaf + "/hierarchy",
            params={"direction": "BROADER", "status": status},
            user="sales",
            node=1,
            status=404,
        )
    else:
        assert names(
            lake.hierarchy(leaf, "BROADER", "sales", status, node=1)
        ) == sorted([via, root])
    before = lake.term(leaf, node=1)
    lake.alter(barrier, edit("setStatus", status=status))
    assert names(
        lake.hierarchy(leaf, "BROADER", "manager", status, node=1)
    ) == sorted([via, barrier, root, other])
    assert names(
        lake.hierarchy(other, "NARROWER", "manager", status, node=1)
    ) == sorted([barrier, leaf])
    assert lake.term(leaf, node=1) == before
    assert (
        lake.call(
            "GET",
            "terms/" + leaf + "/objects",
            params={"status": status},
            node=1,
        )["bindings"][0]["object"]["fullName"]
        == "dw.s.orders.amount"
    )
    assert by_alias(lake, leaf, "Retail", node=1) == []
