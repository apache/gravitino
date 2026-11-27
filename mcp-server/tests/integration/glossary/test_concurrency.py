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

"""Glossary read and write contracts across two servers sharing a store."""

from concurrent.futures import ThreadPoolExecutor
from functools import partial
from threading import Barrier

from tests.integration.glossary.client import by_alias, edit

METRIC_TERMS = "objects/semantic_model/dw.s.metrics/metrics/volume/terms"


def race(first, second):
    barrier = Barrier(2, timeout=15)

    def start(action):
        barrier.wait()
        return action()

    with ThreadPoolExecutor(2) as pool:
        first_result = pool.submit(start, first)
        second_result = pool.submit(start, second)
        return first_result.result(), second_result.result()


def test_domain_delete_races_mixed_term_edit_on_two_nodes(lake):
    for i in range(20):
        domain = f"RaceDomain{i}"
        term = f"RaceMember{i}"
        lake.domain(domain)
        before = lake.create(term)
        change = partial(
            lake.send,
            "PUT",
            "terms/" + term,
            {
                "updates": [
                    edit("addDomain", domain=domain),
                    edit("addBroader", broader="Concept"),
                    edit("setProperty", property="round", value=str(i)),
                ]
            },
            node=i % 2,
        )
        drop = partial(lake.send, "DELETE", "domains/" + domain, node=1 - i % 2)
        (change_status, changed), (drop_status, dropped) = race(change, drop)
        after = lake.term(term, node=1)
        if (change_status, drop_status) == (200, 409):
            assert dropped["type"] == "NonEmptyDomainException"
            assert (
                after["domains"],
                after["broader"],
                after["properties"],
                after["version"],
            ) == (
                [domain],
                ["Concept"],
                {"round": str(i)},
                before["version"] + 1,
            )
            assert by_alias(lake, term, domain, node=1) == [term]
        else:
            assert (
                change_status,
                changed.get("type"),
                drop_status,
                dropped.get("dropped"),
            ) == (
                404,
                "NoSuchDomainException",
                200,
                True,
            )
            assert after == before


def test_readers_see_committed_domain_and_graph_together(lake):
    n = "SnapshotTerm"
    for suffix in "AB":
        lake.domain("SnapshotD" + suffix)
        lake.create("SnapshotP" + suffix)
    lake.create(
        n,
        definition="A",
        broader=["SnapshotPA"],
        domains=["SnapshotDA"],
        synonyms=["snapshot phrase"],
    )
    barrier = Barrier(2, timeout=15)

    def writer():
        for i in range(40):
            old, new = ("A", "B") if i % 2 == 0 else ("B", "A")
            barrier.wait()
            lake.alter(
                n,
                edit("removeDomain", domain="SnapshotD" + old),
                edit("addDomain", domain="SnapshotD" + new),
                edit("removeBroader", broader="SnapshotP" + old),
                edit("addBroader", broader="SnapshotP" + new),
                edit("updateDefinition", newDefinition=new),
            )
            barrier.wait()

    def reader():
        observed = set()
        for i in range(40):
            barrier.wait()
            scope = "A" if (i // 2) % 2 == 0 else "B"
            data = lake.lookup(
                node=1,
                domain="SnapshotD" + scope,
                match="snapshot phrase",
                status="APPROVED",
                bindings="true",
            )
            barrier.wait()
            assert len(data) <= 1
            if data:
                (term,) = data
                assert (
                    term["name"],
                    term["definition"],
                    term["domains"],
                    term["broader"],
                    term["bindings"],
                ) == (
                    n,
                    scope,
                    ["SnapshotD" + scope],
                    ["SnapshotP" + scope],
                    [],
                )
            final = lake.term(n, node=1)
            state = final["definition"]
            assert (final["domains"], final["broader"]) == (
                ["SnapshotD" + state],
                ["SnapshotP" + state],
            )
            assert state == ("B" if i % 2 == 0 else "A")
            for parent in ("A", "B"):
                expected = [n] if state == parent else []
                assert (
                    by_alias(
                        lake, "snapshot phrase", "SnapshotD" + parent, node=1
                    )
                    == expected
                )
            observed.add(state)
        assert observed == {"A", "B"}

    with ThreadPoolExecutor(2) as pool:
        x = pool.submit(writer)
        y = pool.submit(reader)
        x.result()
        y.result()


def test_delete_parent_racing_rename_domain_and_relation(lake):
    lake.domain("MixedParentRace")
    for i in range(22):
        parent = f"RemovedParent{i}"
        child = f"StableChild{i}"
        renamed = f"RenamedChild{i}"
        alias = f"mixed edit {i}"
        lake.create(parent)
        lake.create(child, domains=["Finance"], broader=["Concept"])
        lake.call("POST", METRIC_TERMS, {"termsToAdd": [child]})
        before = lake.term(child, node=1)
        change = partial(
            lake.send,
            "PUT",
            "terms/" + child,
            {
                "updates": [
                    edit("rename", newName=renamed),
                    edit("addSynonym", synonym=alias),
                    edit("addDomain", domain="MixedParentRace"),
                    edit("addBroader", broader=parent),
                ]
            },
            node=i % 2,
        )
        drop = partial(lake.send, "DELETE", "terms/" + parent, node=1 - i % 2)
        # Exercise each serial outcome before racing on different JVMs.
        if i == 0:
            changed, dropped = change(), drop()
        elif i == 1:
            dropped, changed = drop(), change()
        else:
            changed, dropped = race(change, drop)
        assert (dropped[0], dropped[1].get("dropped")) == (200, True)
        visible = lake.call("GET", METRIC_TERMS, node=1)["names"]
        if changed[0] == 200:
            assert i != 1
            after = lake.term(renamed, node=1)
            assert sorted(after["domains"]) == sorted(
                ["Finance", "MixedParentRace"]
            )
            assert after["broader"] == ["Concept"] and after["synonyms"] == [
                alias
            ]
            assert after["version"] == before["version"] + 2
            assert by_alias(lake, alias, "MixedParentRace", node=1) == [renamed]
            assert renamed in visible and child not in visible
            lake.call("GET", "terms/" + child, node=1, status=404)
        else:
            assert i != 0 and (changed[0], changed[1].get("type")) == (
                404,
                "NoSuchTermException",
            )
            assert lake.term(child, node=1) == before
            assert by_alias(lake, alias, "MixedParentRace", node=1) == []
            assert child in visible and renamed not in visible
            lake.call("GET", "terms/" + renamed, node=1, status=404)
