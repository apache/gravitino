<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Entity-store aggregate read consistency audit

Related issue: #13853, table-read P0. This audit covers entity-store reads, not consistency with external catalogs or authorization plugins.

## Invariant

An aggregate returned by an entity-store get must describe one committed statement snapshot: table metadata and its columns, role metadata and its stored privileges, or principal metadata and its memberships. A concurrent overwrite/drop may leave the reader with the complete older snapshot or with a not-found result; it must not produce a root from one state and dependent rows from another. This also applies to the old entity supplied to an updater.

The reads use ordinary, non-locking SELECTs at the existing transaction isolation level. No retry, timeout, new HTTP failure signal, migration, or change to write CAS is introduced. The standard entity-cache population path receives an already-complete table. Cross-node invalidation remains eventual: this change prevents torn snapshots, not the normal delay before a peer replays a committed change log.

## Read audit

| Read | Before | Result |
| --- | --- | --- |
| Table get and updater input | Root/current-version join followed by a separate column-history query | One SELECT joins the root, current version, and latest live history row for every column. Both parent-ID and full-name routes are covered. |
| Table list and batch get | Metadata-only, without columns | Unchanged; these APIs deliberately return metadata-only entities. |
| Role get, updater input, and all-fields list by metadata object | Role row followed by privilege rows | One SELECT joins the role and all live stored privileges. Metadata-only lists remain metadata-only. |
| User and group get and updater input | Principal row followed by membership/role rows | One SELECT joins the principal, live memberships, and live roles. Empty memberships retain the existing representation. |
| User/role batch get | Calls the single-entity getter once per identifier | Each entity now benefits from its joined snapshot. There is no snapshot covering the entire batch. |
| Group batch get; user/group namespace and paginated list | Joined/aggregated principal and membership queries | Each payload query already uses one SQL statement; unchanged. |
| Fileset | Joined root and location/version rows | Already one statement through the nested result map; unchanged. |
| Function, view, semantic model | Joined root and current-version rows; structured content serialized in the version row | Already one statement; unchanged. |
| Metalake, catalog, schema, topic, model, tag, policy, job, job template | Entity content comes from one root/version query | No separate dependent-row assembly found in their single-entity getters; unchanged. Parent-name/ID resolution can precede the read, but does not splice two versions of an entity payload. |
| Model version | Version rows and aliases protected by aggregate-version revalidation | Existing bounded stable-read loop remains. Conflict rates during linking are the separate P1 in #13853. |

Role privilege target names are still resolved by stable metadata IDs after reading the role/privilege snapshot. A target deleted before name resolution is omitted, with the existing warning; target renames and the parent-name reads are not a global snapshot of the hierarchy. This patch does not change that behavior or promise cross-entity snapshot isolation.

Relation lists that first resolve a principal/target and then query its relations retain their existing list semantics. They do not assemble a full root-plus-dependent payload for the resolved object. They are not converted into transactions by this patch.

## SQL and cost

Table columns are versioned deltas, not complete per-version snapshots. The joined query groups live history rows by `(table_id, column_id)` and takes the maximum version no greater than the root's current version. DELETE markers must participate in this selection; the final join excludes them only after resolving the latest version. LEFT JOINs preserve tables with zero columns and full-name missing-parent placeholders.

History is aggregated once, rather than scanned in a correlated subquery for every output column. Root selection appears twice inside the single statement so both occurrences have explicitly bound parameters across supported backends. The final history-row lookup uses the existing `(table_id, table_version, column_id, deleted_at)` unique key in H2, MySQL and PostgreSQL. Role privilege rows are grouped by their physical `id` primary key in the nested mapping. The database does not require `(role_id, metadata_object_id, type)` to be unique; direct store callers can persist multiple privilege rows for one target, and reads preserve them.

Root/name predicates and soft-delete filtering are inherited from the existing providers, including backend overrides.

Joined results repeat root fields for each child row, increasing result bytes for wide tables or large properties. This trades additional root projection bytes for one consistent read and fewer round trips; no throughput improvement is claimed. Row-only identity and delete lookups retain their existing smaller projection.

## Regression verification

`TestConsistentEntityReads` commits a write on another connection after the reader's root-mapper call completes and before its next statement. Real mapper/service/backend calls run on both threads; the mock only controls the statement boundary. With the former split read this deterministically loses columns, changes privileges, or mixes memberships. With the joined read, all dependent rows were fetched before that boundary.

Coverage includes same-ID overwrite, replacement by another ID, drop, a cache miss followed by a cache hit, genuinely empty tables, parent-ID/full-name table routing, missing-parent errors, role overwrite/drop/all-fields listing, empty privilege snapshots, multiple privilege rows for the same target, and user/group overwrite/drop. For table, role, user and group updates, a concurrent overwrite commits after the root read; the updater must receive the complete older snapshot before its stale CAS is rejected, and the committed replacement must remain intact. Existing service tests also cover column delta histories and tombstones, empty memberships, and updater CAS behavior.
