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

# Design: Domain

| Version | Author | Date | Status |
| :---- | :---- | :---- | :---- |
| 1.17 | Jerry Shao | 2026-10-15 | Draft |

## Goals

1. **Business grouping**: group assets from any catalog under named domains (e.g. "Finance") for discovery, browsing and agent retrieval. An asset can belong to several domains.
2. **Membership is tags**: an asset is a member when it carries the domain's backing tag. There is no separate membership store.
3. **Separate privileges**: domain privileges govern the `Domain` object; tag privileges govern membership. Membership grants no access to data.
4. **One level of sub-domains**, each with one parent; a sub-domain can be moved.
5. **Draft and published status**: a `DRAFT` domain is visible only to its owner and managers, the same on REST, MCP and any UI.
6. **Inherited membership**: tagging a catalog or schema makes the objects under it members, through Gravitino's existing tag inheritance.

## Non-Goals

1. **Approval workflow**: adding members, attaching sub-domains and publishing take effect at once; status is a manual switch.
2. **Data-access grants on domains**: a domain carries no data privileges such as `SELECT_TABLE` (see Grants and policies).
3. **Deeper or multi-parent hierarchies**: the storage allows them later without a schema change.
4. **Automatic membership**, glossary and metric designs, and the ABAC policy language. Glossary terms become members once they are taggable.
5. **UI**: this design covers the server, REST APIs, clients and MCP tools.

## Usage Scenario

1. A steward with `MANAGE_DOMAIN` and `CREATE_TAG` on the metalake creates domain Finance. Gravitino creates tag `Finance` for it; the steward owns both.
2. The steward creates Trading under Finance (`"parent": "Finance"`); it gets tag `Finance/Trading`. The steward grants `APPLY_TAG` on that tag to the trading table owners.
3. A table owner tags `cat.db.trades` with `Finance/Trading`. The table is in Trading, and appears under Finance through the sub-domain roll-up. Tagging schema `cat.db` with `Finance` would put every table in it in Finance too.
4. A user or agent who can see Trading browses it and gets the tables they can access.
5. Trading is moved under Markets. Its members now appear under Markets; nothing is retagged and the tag keeps its name.

## Design

### Data model

```
Domain    { id, name, comment, status: DRAFT | PUBLISHED, properties, audit_info }
DomainRel { parentId, childId, audit_info }      // one edge per sub-domain
```

- A unique index on `DomainRel (child_id, deleted_at)` keeps one parent per domain. `parent` is not stored on `Domain`; the API derives it. Dropping the index and checks would allow multiple parents later without a schema change.
- A domain name cannot contain `/`, so default tag names stay unambiguous.
- Domains live under a metalake. A sub-domain and its parent are in the same metalake.

### Backing tag

Each domain has **exactly one** backing tag: a tag, optionally with a value (tag values: apache/gravitino#12162).

- **Default**: if none is given at creation, Gravitino creates a `NO_VALUE` tag named after the domain's path, as in Unity Catalog: `Finance`, and `Finance/Tax` for sub-domain Tax. The creator owns it, so creating a domain this way also needs `CREATE_TAG`. If a tag with that name already exists, the domain binds to it only when the caller owns it (or is the metalake owner), adopting the assets that carry it; otherwise creation fails.
- **Explicit**: any existing (tag, value), such as `dept=finance`, bound by that tag's owner (or the metalake owner). A (tag, value) may back several domains, which then have the same members.
- **Storage**: the backing tag is the `DOMAIN` object's own row in `tag_relation_meta`; there is no extra field to keep in sync. Reverse lookup uses `idx_tid_value` filtered to `DOMAIN` objects. Domains never inherit tags (`METALAKE` is not taggable).
- **Changing it**: through the existing tag association API on the domain (`tagsToRemove=[old]`, `tagsToAdd=[new]` in one request). For `DOMAIN` targets that API also enforces at most one backing tag, under the domain's row lock. Assets carrying only the old tag drop out; nothing is migrated.
- **Never renamed**: moving or renaming a domain does not rename its tag; the hierarchy lives in `DomainRel`.
- **Deleting the tag** is allowed, as for any tag; its domains are left with no backing tag and no members until a new one is set. There is no domain-side audit of this.
- **`/` in tag names** must be URL-encoded in REST paths (`%2F`). The Java, Python and MCP clients already do this, and Gravitino's Jetty 9.4 / Jersey stack decodes it into the `{tag}` parameter.

### Sub-domains

Every operation that sets a parent (create with `parent`, attach, move) checks:

| # | Constraint |
| :---- | :---- |
| C1 | A domain cannot be its own parent |
| C2 | The parent is top-level |
| C3 | The attached domain has no sub-domains |
| C4 | Parent and child are in the same metalake |
| C5 | At most one parent; attaching a domain that has one is a move |
| C6 | A `PUBLISHED` domain can only be attached under a `PUBLISHED` parent |

C1–C3 keep the hierarchy one level deep and acyclic, so no cycle detection is needed.

- **Attach / move** re-points the child's single edge in one transaction. Attaching to the current parent is a no-op.
- **Detach** removes the edge; the child becomes top-level.
- **Locking**: each operation locks the domain rows involved (child, new parent, old parent) in id order, then checks the constraints. Otherwise two valid calls could together create two levels, a cycle, or a dangling edge.
- **Delete** is refused while the domain has sub-domains. Deleting removes the domain's own parent edge and its backing tag association. The tag and the tags on assets stay.
- **Scope**: a parent's members include those of the sub-domains the caller can see; a sub-domain's members are its own only.

### Status

- New domains are `DRAFT`. The owner or a `MANAGE_DOMAIN` holder publishes or unpublishes, at any time and without approval.
- **Visibility is the same on every surface** (REST, MCP, UI). A `DRAFT` domain is visible only to its owner and `MANAGE_DOMAIN` holders, including those on its parent. For anyone else it does not exist: lists omit it, get returns 404, and it is not in `list_domains`, member listings, an object's domains or a parent's roll-up. Responses carry `status`, so a caller that sees drafts can filter them out.
- **Consistency**: publishing a sub-domain needs a published parent; unpublishing needs no published sub-domains (C6 covers attach and move).
- Assets can be tagged into a `DRAFT` domain to prepare it; anyone with `APPLY_TAG` on its tag can do so, as in Unity Catalog.
- Roll-ups depend on the caller: someone who can see a `DRAFT` sub-domain sees its members under the parent; others do not.

### Membership and inheritance

- **Rule**: membership reuses `MetadataObjectTagOperations#listTagsForMetadataObject` unchanged. An object's effective tags are its own plus its schema's and catalog's; for the same tag the nearest level wins, and different tags accumulate. An object is a member of domain D when its effective tags include D's backing (tag, value).
- **Dedicated tags (the default)** accumulate: with schema `Finance` and table `Sales`, the table is in both, and a child cannot leave a domain it inherits, as in Unity Catalog.
- **Values of one tag** backing different domains override each other: with schema `dept=finance` and table `dept=hr`, the table is only in the `dept=hr` domain, unless it uses `dept=[finance, hr]`. This is the one case where a child can leave an inherited domain, and it follows from the tag owner's choice of binding.
- **Member types**: every taggable type except `DOMAIN` — today catalogs, schemas, tables, views, filesets, topics, models, columns, functions and semantic models. Types that become taggable later join automatically. Metrics belong through their semantic model.
- **Domains of an object**: the object's effective tags mapped to the domains the caller can see, each marked `inherited` and, if so, `inheritedFrom` (the catalog or schema). The source is known while resolving inheritance, at no extra cost.
- **Domain filters** check each candidate object with the rule above instead of listing all members first.

### Member listing

`GET /domains/{domain}/objects` resolves the backing tags of the domain and its visible sub-domains, lists the objects carrying them (`listMetadataObjectsForTag`, by tag and value), and unions the results. It drops `DOMAIN` objects and objects the caller cannot access (`filterMetadataObject`), de-duplicates, and records the sub-domain each member was reached through. It needs no tag privilege.

Only directly tagged objects are listed; a catalog or schema stands for the children that inherit its tag. Whether to expand them, and what member counts mean, is open (see Open questions).

### Access control

**Domain privileges**

| Privilege | On the metalake | On a domain |
| :---- | :---- | :---- |
| `MANAGE_DOMAIN` | Create domains and manage all of them | Edit, publish, sub-domains, backing tag, delete; see it while `DRAFT` |
| `USE_DOMAIN` | See all published domains and browse their members | See this domain and browse its members |

`MANAGE_DOMAIN` includes `USE_DOMAIN`; owners and the metalake owner hold both. Privileges on a parent also apply to its sub-domains, as in Unity Catalog. There is no separate `CREATE_DOMAIN`.

**What each operation needs**

| Operation | Required |
| :---- | :---- |
| See a domain | Published: `USE_DOMAIN`. Draft: owner or `MANAGE_DOMAIN` |
| Browse members, domains of an object | See the domain; results filtered by each object's own privileges; no tag privilege |
| Add or remove members | Existing tag authorization (`CAN_ACCESS_METADATA_AND_TAG`): access to the object and `APPLY_TAG` on the backing tag. No domain privilege |
| Create (default tag) | `MANAGE_DOMAIN` and `CREATE_TAG` on the metalake |
| Create (existing tag) | `MANAGE_DOMAIN` on the metalake and ownership of the tag |
| Create as a sub-domain | Also `MANAGE_DOMAIN` on the parent |
| Set or replace the backing tag | `MANAGE_DOMAIN` and ownership of the new tag |
| Remove the backing tag | `MANAGE_DOMAIN` |
| Attach / move | `MANAGE_DOMAIN` on both the domain and the new parent |
| Detach | `MANAGE_DOMAIN` on either |
| Publish, unpublish, edit, delete | `MANAGE_DOMAIN` |

- Browsing without `VIEW_TAG` is safe because only a tag's owner can bind it to a domain: exposing that one (tag, value) to the domain's viewers is the owner's decision, and viewers still see only objects they can access.
- A user with `MANAGE_DOMAIN` but no `APPLY_TAG` on the tag can manage the domain but not add members. The domain owner and the tag owner are separate and not synchronized.

**Implementation**

- `CAN_ACCESS_METADATA` gets a `DOMAIN` branch, `ANY(OWNER, METALAKE) || MANAGE_DOMAIN || USE_DOMAIN`, for reading a domain's own tags, subject to the status rule.
- The tag-association executor special-cases `DOMAIN` targets: `MANAGE_DOMAIN` on the domain (never `USE_DOMAIN`), ownership of each added tag, at most one backing tag. Tagging other objects is unchanged.
- Sub-domain endpoints have two `DOMAIN` path parameters, which collide in `ParameterUtil#extractEntitiesFromAnnotations` (a map keyed by entity type). A new `AuthorizationRequest.RequestType` (e.g. `DOMAIN_HIERARCHY`), like `ASSOCIATE_TAG`, checks both sides. `parent` and `backingTag` in a create request are checked imperatively.

### Grants and policies

- **No data-access grants on domains.** Since anyone with `APPLY_TAG` on a backing tag can add an asset, an allow grant on a domain would turn tagging into granting access without the asset owner's consent. Unity Catalog has no domain-scoped grants either. Possible future designs are listed in Open questions.
- **A domain's policies are the policies on its backing tag**, through the existing policy-tag association. Policies stay on tags and objects; domains add no policy mechanism. They reach assets that carry or inherit the tag, but not a sub-domain's assets (see Open questions).

### REST API

Under `/api/metalakes/{metalake}`, following Gravitino conventions: `?details=` on lists, `PUT` + updates to alter, `PATCH` + a set request to switch state, `DropResponse` on delete, owners through the owner API.

| Operation | Endpoint | Request → response |
| :---- | :---- | :---- |
| List | `GET /domains?details=false` | → `NameListResponse` / `DomainListResponse` |
| Create | `POST /domains` | `DomainCreateRequest{name, comment, properties, backingTag{name, value}, parent}` → `DomainResponse` |
| Get | `GET /domains/{domain}` | → `DomainResponse` |
| Alter | `PUT /domains/{domain}` | `DomainUpdatesRequest{updates: [rename, updateComment, setProperty, removeProperty]}` → `DomainResponse` |
| Publish / unpublish | `PATCH /domains/{domain}` | `DomainSetRequest{status}` → `BaseResponse` |
| Drop | `DELETE /domains/{domain}` | → `DropResponse` (`false` if absent) |
| Attach / move | `POST /domains/{parent}/subdomains/{domain}` | → `BaseResponse` |
| Detach | `DELETE /domains/{parent}/subdomains/{domain}` | → `DropResponse` |
| List sub-domains | `GET /domains/{domain}/subdomains?details=false` | → `NameListResponse` / `DomainListResponse` |
| List members | `GET /domains/{domain}/objects` | → `DomainMemberListResponse` |
| Domains of an object | `GET /objects/{type}/{fullName}/domains?details=false` | → `NameListResponse` / `DomainListResponse` |
| Add / remove members | Existing `POST /objects/{type}/{fullName}/tags` with the domain's backing tag | `TagsAssociateRequest` |
| Replace backing tag | Existing `POST /objects/domain/{domain}/tags` | `TagsAssociateRequest` |

```
DomainDTO       { name, comment, properties, status, parent, backingTag{name, value}, audit,
                  inherited?, inheritedFrom? }   // the last two only on domains of an object
DomainMemberDTO { object: MetadataObjectDTO, subdomain? }
```

- **Create** happens in one transaction: domain, default tag, tag association and parent edge. On any failure nothing is created, and no default tag is left behind.
- **Member listing parameters**: `includeSubdomains` (default `true`), `types` (comma-separated `MetadataObject.Type`s), `nameContains` (case-insensitive, on the full name), `subdomain`. No paging, like the other core list APIs. Filters see only the listed members, so they reach a catalog's tables only if catalogs are expanded.
- **Errors**: 404 `NoSuchDomainException` (missing, or a `DRAFT` domain the caller cannot see); 409 `DomainAlreadyExistsException`; 409 `NonEmptyEntityException` (drop with sub-domains); 400 `IllegalArgumentException` (C1–C6, status consistency, invalid tag value, more than one backing tag, `/` in a name); 403 `ForbiddenException` (default tag name owned by someone else, or binding a tag the caller does not own).
- **Reused**: owners at `/owners/domain/{domain}`; privileges through the role and permission APIs, with `DOMAIN` as a securable type; policies through the policy-tag association APIs.
- **Clients** (Java, Python) mirror the tag clients: domain operations on `GravitinoMetalake`, and `SupportsDomains` on catalogs, schemas, tables and other objects.
- **Not included**: a batch lookup of the domains of many objects.

### MCP tools

A new read-only `domain` tool category in `mcp-server/`, added to `SUPPORTED_TOOL_TAGS` (enabled with `--include-tool-tags`):

| Tool | Endpoint |
| :---- | :---- |
| `list_domains` | `GET /domains?details=true`; returns name, comment, parent, status and backing tag, so an agent can pick a domain |
| `get_domain_by_name` | `GET /domains/{domain}` |
| `list_metadata_by_domain` | `GET /domains/{domain}/objects`, with `include_subdomains`, `types`, `name_contains`, `subdomain`; lets an agent look within a domain by type and name |
| `list_domains_for_metadata` | `GET /objects/{type}/{fullName}/domains` |

Adding members uses the existing `associate_tag_with_metadata` tool. The MCP server forwards the caller's `Authorization` header, so the server's visibility rules apply; with OAuth client-credentials, the agent sees what that service identity sees.

### Events and audit

- Every domain operation emits pre, success and failure events through a `DomainEventDispatcher`, and reaches the audit log through the existing pipeline. `Domain` and `DomainRel` carry `audit_info`.
- Membership changes are tag association changes, audited by the existing `AssociateTagsForMetadataObjectEvent`. To audit a domain's membership, query the events of its backing tag (and earlier ones if it was replaced). Tagging a catalog, or deleting a backing tag, is one tag-level event.

## Open Questions

1. **Catalog or schema members**: list them as one member (cheap; counts are direct members only), expand them from Gravitino's store (slow for large catalogs; misses tables not yet recorded), or keep the list and count asynchronously (approximate, delayed)? Clients that show asset counts need expanded counts.
2. **Policy inheritance to sub-domains**: should a parent's policies also apply to its sub-domains' assets? Options: no (proposed), inheritance in policy evaluation, or copying policies to sub-domain tags.
3. **Data-access grants on domains**, if wanted later: deny-only grants (tagging can only tighten access), or allow grants that also require grant authority on the asset when tagging. Either works only where Gravitino enforces authorization itself and adds a membership lookup to every check.

## Task Breakdown

### Phase 0: Prerequisites

- [x] None. Tag values and constraints (apache/gravitino#12162), tag inheritance, listing objects by tag and value, `/` encoding in the clients, and policy-tag association already exist.

### Phase 1: Domain

- [ ] `Domain` API in `api/`; `MetadataObject.Type.DOMAIN`, registered as taggable in `TagManager.SUPPORTED_METADATA_OBJECT_TYPES_FOR_TAGS`
- [ ] Storage and manager in `core/`: `Domain` and `DomainRel` tables (H2, MySQL, PostgreSQL, upgrade scripts), constraints C1–C6, row locking, status and its consistency checks, status-based visibility on every path
- [ ] Default backing tag: create it with the domain (checking `CREATE_TAG`), bind an existing one only for its owner; at-most-one backing tag on the association API
- [ ] Membership: member listing with filters and sub-domain roll-up; domains of an object with `inherited` and `inheritedFrom`
- [ ] Authorization: `MANAGE_DOMAIN` and `USE_DOMAIN` with parent-to-sub-domain inheritance; the `DOMAIN` branch of `CAN_ACCESS_METADATA`; the `DOMAIN`-target tag-association executor; the `DOMAIN_HIERARCHY` executor
- [ ] REST endpoints in `server/` as in REST API
- [ ] Java and Python clients
- [ ] MCP tools
- [ ] Events (`DomainEventDispatcher`) and audit
- [ ] Unit and integration (Docker) tests

### Phase 2: Documentation

- [ ] OpenAPI spec (`docs/open-api/*.yaml`, validated with `./gradlew :docs:build`) and user docs: default and explicit backing tags, adding members by tagging, privileges, `/` encoding, tags not renamed on move or rename
