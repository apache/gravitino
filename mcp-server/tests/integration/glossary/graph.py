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

"""Seed terms, their relationships, and the expected caller-visible projections."""

LEAF = "Active/Customer"

ALIAS = "net activity"

# The diamond has additional paths through draft and deprecated terms.
PARENTS = {
    "Concept": (),
    "DraftOnlyRoot": (),
    "OldOnlyRoot": (),
    "Left": ("Concept",),
    "Right": ("Concept",),
    "DraftBridge": ("Concept", "DraftOnlyRoot"),
    "OldBridge": ("OldOnlyRoot",),
    LEAF: ("Left", "Right", "DraftBridge", "OldBridge"),
}

STATUS = {
    "DraftBridge": "DRAFT",
    "OldBridge": "DEPRECATED",
    "DraftPeer": "DRAFT",
    "LegacyPeer": "DEPRECATED",
    "DraftHit": "DRAFT",
    "OldHit": "DEPRECATED",
}

MEMBERS = {
    LEAF: ["Sales", "Retail", "Finance"],
    "RetailAlt": ["Retail"],
    "FinanceAlt": ["Finance"],
    "WholesaleAlt": ["Wholesale"],
    "Unscoped": [],
    "DraftHit": ["Retail"],
    "OldHit": ["Retail"],
}

PEERS = ["LivePeer", "DraftPeer", "LegacyPeer"]

VISIBLE_BINDINGS = {
    ("table", "dw.s.orders", None),
    ("column", "dw.s.orders.amount", None),
    ("semantic_model", "dw.s.metrics", "volume"),
}


def visible(name, user, status):
    state = STATUS.get(name, "APPROVED")
    return (state != "DRAFT" or user in ("admin", "manager")) and (
        not status or state == status
    )


def reachable(root, direction, edges, allowed):
    neighbor = {n: set(v) for n, v in edges.items()}
    if direction == "NARROWER":
        neighbor = {n: {c for c, p in edges.items() if n in p} for n in edges}
    seen = set()
    frontier = [root]
    while frontier:
        current = frontier.pop()
        for n in neighbor[current]:
            if n not in seen and allowed(n):
                seen.add(n)
                frontier.append(n)
    return sorted(seen)


def projected_domains(name, user):
    actual = MEMBERS.get(name, ["Finance"])
    scope = {
        "admin": set(actual),
        "sales": {"Sales", "Retail"},
        "no_retail": {"Sales"},
        "manager": set(),
    }
    return sorted(set(actual) & scope[user])
