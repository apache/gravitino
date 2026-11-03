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

## Goals

1. **Business grouping**: group assets from any catalog under named domains (e.g. "Finance") for discovery, browsing and agent retrieval. An asset can belong to several domains.
2. **Membership is tags**: a leaf asset is a member when it carries the domain's backing tag, directly or inherited from its catalog or schema. There is no separate membership store.
3. **Separate privileges**: domain privileges govern the `Domain` object; tag privileges govern membership. Membership grants no access to data.
4. **One level of subdomains**, each with one parent; a subdomain can be moved.
5. **Draft and published status**: a `DRAFT` domain is returned only to its owner and managers, the same on REST, MCP and any UI.
6. **Inherited membership**: tagging a catalog or schema makes the leaf objects under it members, through Gravitino's existing tag inheritance.

## Non-Goals

1. **Approval workflow**: adding members, attaching subdomains and publishing take effect at once; status is a manual switch.
2. **Data-access grants on domains**: a domain carries no data privileges such as `SELECT_TABLE` (see Grants and policies).
3. **Domain-level policies**: domains add no policy mechanism; policies keep following tags (see Grants and policies).
4. **Deeper or multi-parent hierarchies**: the storage allows them later without a data migration.
5. **Automatic membership**, glossary and metric designs, and the ABAC policy language. Glossary terms can become members once they are taggable and added to the member types.
6. **UI**: this design covers the server, REST APIs, clients and MCP tools.

## Usage Scenario

1. A steward with `MANAGE_DOMAIN` and `CREATE_TAG` on the metalake creates domain Finance with a new backing tag `finance`. The steward owns both.
2. The steward creates subdomain Trading under Finance with a new backing tag `trading`, and grants `APPLY_TAG` on tag `trading` to the trading table owners.
3. A table owner tags `cat.db.trades` with `trading`. The table is a member of Trading, and is listed under Finance as "via subdomain Trading". Tagging schema `cat.db` with `finance` would make every leaf object in it a member of Finance.
4. A user or agent with `USE_DOMAIN` on Finance (which also covers Trading, unless denied there) or on Trading browses it and gets the tables they can access.
5. Trading is moved under Markets. Its members are now listed under Markets; nothing is retagged.

## Design

### Data model

```
Domain    { id, name, comment, status: DRAFT | PUBLISHED, properties,
            backingTagId?, backingTagValue?, audit_info }
DomainRel { parentId, childId, audit_info }      // one edge per subdomain
```

- Domains live under a metalake. A subdomain and its parent are in the same metalake.
- Domain names are unique within a metalake across both levels: top-level domains and subdomains share one namespace, so a domain is addressed by its name alone. Domain names follow the same constraints as tag names (up to 128 characters) and may contain `/` (see REST API, Encoding).
- A unique index on `DomainRel (child_id, deleted_at)` keeps one parent per domain. `parent` is not stored on `Domain`; the API derives it. Dropping the index and checks would allow multiple parents later without a data migration.
- The backing tag is stored on the domain row as the tag's ID and an optional value, with an index on `(metalake_id, backing_tag_id, backing_tag_value, deleted_at)` for the reverse lookup "which domain does this (tag, value) back". `DOMAIN` is not a taggable object type, so the tag code paths are unchanged (see Alternatives considered).
- The binding refers to the tag by ID, and every membership path (member listing, domains of an object) looks up and compares tag IDs, never tag names: if the tag is deleted, the ID stays on the row and matches nothing, and a new tag with the same name does not match it, even if it is recreated while a request is running.
- Domains, their parent and their backing tag are not cached; every request reads them from the store, so all servers see a move or a backing-tag change at once. Caching them later needs a version bump of the affected domain rows and a changelog entry.

### Backing tag

Each domain has **at most one** backing tag: an existing tag, optionally with a value (tag values: apache/gravitino#12162). Creating a domain always sets one; a domain has none only after its backing tag is removed or deleted, until a new one is set.

- **Explicit at creation**: every create request names the backing tag; there is no default name derived from the domain. The caller either
  - creates a new tag: a tag without values (`NO_VALUE`) with the given name, owned by the caller, which needs `CREATE_TAG`. If a tag with that name exists, creation fails with 409 `TagAlreadyExistsException`; an existing tag is never adopted implicitly; or
  - binds an existing (tag, value), such as `dept=finance`. The caller must own the tag or the metalake; a missing tag fails with 404.

  A create request without a backing tag, or one that creates a new tag and also gives a value, fails with 400.
- **Tag value rules apply as for any assignment**: a tag with a non-empty allowed-values list can only be bound with one of its values; a `NO_VALUE` tag only without a value.
- **No sharing**: following the tag assignment rules, a tag has either one binding without a value, or bindings with values where each value backs at most one domain; the two are never mixed. A conflicting binding fails with 409 `BackingTagAlreadyBoundException` (a new `AlreadyExistsException` subclass). The reverse lookup is therefore one domain per (tag, value).
- **Matching** follows the tag lookup semantics (`GET /tags/{tag}/objects`). A backing tag without a value matches every assignment of the tag, with or without values, like a lookup without `value`. A backing tag with a value matches assignments whose values contain it, exact and case-sensitive, like `?value=`; a valueless assignment never matches it. The same rule applies to member listing and domains of an object, always against the object's effective assignment.
- **Changing it**: `PUT /domains/{domain}/backingTag` replaces or removes it on the server, under the domain and tag row locks. A missing tag fails with 404, a value the tag does not allow with 400, a tag the caller does not own (nor the metalake) with 403, and a binding that conflicts with another domain with 409; setting the current tag is a no-op. Assets carrying only the old tag drop out; nothing is migrated. This endpoint is the only way to change a backing tag: `DOMAIN` is not taggable, so the generic tag association API rejects it.
- **Never renamed**: moving or renaming a domain does not rename its tag; the hierarchy lives in `DomainRel`. A renamed tag keeps backing the domain, since the binding is by tag ID.
- **Deleting the tag** behaves as for any tag: it is allowed, and the tag's assignments on assets are removed with it. Its domain is left with no backing tag (`backingTag` reads as empty) and no members until a new one is set; the membership cannot be recovered by recreating a tag with the same name. Only the tag deletion event records this.
- **Separate from domain privileges**: the domain owner and the tag owner are independent. Managing a domain grants no `APPLY_TAG` on its tag, and transferring the domain's ownership does not transfer the tag's.

### Subdomains

Every operation that sets a parent (create a subdomain, attach, move) checks:

| #    | Constraint                                                           |
| :--- | :------------------------------------------------------------------- |
| C1   | A domain cannot be its own parent                                    |
| C2   | The parent is top-level                                              |
| C3   | The attached domain has no subdomains                                |
| C4   | Parent and child are in the same metalake                            |
| C5   | At most one parent; attaching a domain that has one is a move        |
| C6   | A `PUBLISHED` domain can only be attached under a `PUBLISHED` parent |

C1–C3 keep the hierarchy one level deep and acyclic, so no cycle detection is needed.

- **Attach / move** re-points the child's single edge in one transaction. Attaching to the current parent is a no-op.
- **Detach** removes the current edge; the child becomes top-level.
- **Delete** is refused while the domain has subdomains. Deleting removes the domain row and its own parent edge. The tag and the tags on assets stay.
- **Membership and scope**: membership comes from backing tags only (see Membership and inheritance), so an asset tagged only `trading` is a member of Trading, not of Finance. A top-level domain's *scope* is its own members plus the members of the subdomains the caller can see; a subdomain's scope is its own members. Member listing and domains of an object report scope, with the relation of each entry; policies follow tags only (see Grants and policies).

### Locking

Domain operations use the store's existing locking primitives, in one global order: the metalake fence (`LiveEndpointService.lockLiveEndpoint`, shared), then the domain rows involved in ID order, then tag rows (exclusive, as `lockTagsForAssignment` does). Each row is locked once, in its final mode, so no lock is upgraded inside a transaction. `DOMAIN` is added to `LiveEndpointSQLProvider`.

| Operation                     | Rows locked                                                    | Conflicts it excludes                                                                                        |
| :---------------------------- | :------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------- |
| Create a top-level domain     | metalake (shared) → existing tag (exclusive)                   | metalake drop; two domains binding the same (tag, value)                                                     |
| Create a subdomain            | metalake (shared) → parent (shared) → existing tag (exclusive) | the parent being attached elsewhere (C2)                                                                     |
| Attach / move                 | child (exclusive) and new parent (shared), in ID order         | the new parent being attached elsewhere (C2); a subdomain being created under the child (C3); crossing moves |
| Detach                        | child (exclusive)                                              |                                                                                                              |
| Publish                       | domain (exclusive) and parent (shared)                         | the parent being unpublished                                                                                 |
| Unpublish, delete             | domain (exclusive)                                             | a subdomain being created, attached or published under it                                                    |
| Set or remove the backing tag | domain (exclusive) → tag (exclusive)                           | a conflicting binding; the tag being deleted                                                                 |
| Rename, alter                 | domain (exclusive)                                             |                                                                                                              |

- Every name-addressed operation resolves the name to an ID, locks the row, then re-checks that the row still has that name and is not deleted; otherwise it fails with 404.
- The parent is read before locking, so it may change before the locks are held. After locking, the operation re-reads the parent edge from `DomainRel`; if it changed, it rolls back and retries with the new lock set (a bounded number of times). C6, the status checks and the inherited authorization are evaluated against this locked relation.
- A tag named in a request is re-checked after locking; a missing tag fails with 404 instead of being skipped.
- A new backing tag has no row to lock yet; its name is protected by the tag name's unique key, so a concurrent create with the same name fails with 409.
- The unique index on `DomainRel` and the domain row's own version (optimistic locking, never exposed to callers) remain as backstops.

### Status

- New domains are `DRAFT`. The owner or a `MANAGE_DOMAIN` holder publishes or unpublishes, at any time and without approval.
- **Visibility is the same on every surface** (REST, MCP, UI). A `DRAFT` domain is returned only to the users who can manage it, including through its parent or the metalake (see Access control). Domain APIs do not return it to anyone else: lists omit it, get returns 404, and it is not in `list_domains`, member listings, an object's domains or a parent's roll-up. Responses carry `status`, so a caller that sees drafts can filter them out.
- **Known channels**: hiding a draft is not confidentiality. Its existence can still show through:
  - creating a domain with the same name fails with 409;
  - its backing tag is an ordinary tag, visible to tag readers and on tagged assets (use a non-descriptive tag name if that matters);
  - `parent` is always returned, even when the caller cannot see the parent. Only the name is exposed; getting the parent still returns 404, and nothing can be done on it.
- **Consistency**: publishing a subdomain needs a published parent; unpublishing needs no published subdomains (C6 covers attach and move).
- Assets can be tagged into a `DRAFT` domain to prepare it; anyone with `APPLY_TAG` on its tag can do so.
- Roll-ups depend on the caller: someone who can see a `DRAFT` subdomain sees its members under the parent; others do not.

### Membership and inheritance

- **Rule**: an object's effective tags are its own plus its schema's and catalog's; for the same tag the nearest level wins, and different tags accumulate. A leaf object (see Member types) is a member of domain D when its effective tags match D's backing (tag, value). Membership reuses core's `EffectiveTagResolver`, which applies no tag-privilege filtering: domain visibility and object access are applied afterwards, so a caller needs no tag privilege.
- **Different tags accumulate**: with schema `finance` and table `sales`, the table is in both Finance and Sales, and a child cannot leave a domain it inherits.
- **Values of one tag** backing different domains override each other: with schema `dept=finance` and table `dept=hr`, the table is only in the `dept=hr` domain, unless it uses `dept=[finance, hr]`. This is the one case where a child can leave an inherited domain, and only for bindings with a value; a binding without a value matches every value.
- **Member types**: leaf objects only: tables, views, filesets, topics, models and semantic models (`DomainMember.SUPPORTED_TYPES`). Catalogs and schemas are never members: one that carries a backing tag passes membership to the leaf objects under it through tag inheritance. Columns and functions are not members either: a column or function carrying a backing tag is not listed. A type that becomes taggable later joins only when it is added to this list. Metrics belong through their semantic model.
- **Relations**: a leaf object relates to a domain in one of three ways, reported with its source by member listing and by domains of an object:

  | Relation      | Meaning                                                        | Source                                    |
  | :------------ | :------------------------------------------------------------- | :---------------------------------------- |
  | Direct        | the object itself carries the domain's backing tag             | none                                      |
  | Inherited     | the object inherits the backing tag from its catalog or schema | the nearest catalog or schema carrying it |
  | Via subdomain | a visible subdomain of the domain has the object in its scope  | that subdomain                            |

  Every relation that applies is reported, as a list of (relation, source) pairs, with no precedence: a table whose schema carries `finance` and which itself carries `trading` gets `[Inherited from cat.db, Via subdomain Trading]` under Finance, and an object in two subdomains gets one Via subdomain pair for each. Clients decide how to display them. Relations are always relative to the listed domain. An object is a member of the domain itself when its relations include Direct or Inherited. The relation is an enum; the exact field shape is an implementation choice.
- **Domains of an object**: for a leaf object, every domain the caller can see whose scope contains the object, each with its relations. An object tagged only `trading` gets Trading (Direct) and Finance (Via subdomain Trading). Catalogs and schemas do not support this lookup, since they are never members; the domain a catalog or schema passes down is visible from its tags.
- **Removing a member**: membership ends when the object no longer carries the backing tag, by the tag rules. To take an object out of a domain backed by a tag without a value, remove the tag by name (v1 association request), which removes all its values on the object; a v2 request removes one (tag, value) pair. An inherited membership can only be removed on the catalog or schema.
- **Domain filters**: Phase 1 adds no domain filter to other list APIs. When one is added, it matches scope, consistent with member listing.

### Member listing

`GET /domains/{domain}/objects` lists leaf objects, expanding catalogs and schemas:

1. Resolve the backing tags of the domains in the selected scopes (by default the listed domain's whole scope: itself and its visible subdomains).
2. List the objects carrying them by the bound tag ID and value. The existing lookup by tag name (`listMetadataObjectsForTag`) is not reused, since a tag deleted and recreated with the same name between steps would contribute its objects; this applies to bindings without a value too. Leaf objects are candidates as they are; a tagged catalog or schema is replaced by the leaf objects under it, from Gravitino's store.
3. Keep the candidates whose effective tags match a selected backing tag by tag ID (the matching rules above, nearest level wins), so a leaf object that overrides an inherited value drops out. Only bindings with a value need this check.
4. Drop objects the caller cannot access (`filterMetadataObject`), de-duplicate, and record each member's relations and sources.

- **A view of recorded objects**: membership is defined by the rule above; the listing shows the objects recorded in Gravitino's store. Objects in an external catalog that Gravitino has not recorded yet are not listed, so an empty listing does not mean an object is not a member.
- **Cost**: a domain backed by a tag on a large catalog lists every recorded leaf object under it (see Open Questions).
- **Per caller**: the listing depends on the subdomains and objects the caller can see, so its length is not a property of the domain. Paging and counts come in Phase 2; counts follow the listing and depend on the caller too, so they never reveal assets the caller cannot see.

### Access control

**Domain privileges**

| Privilege       | On the metalake                                    | On a domain                                                          |
| :-------------- | :------------------------------------------------- | :------------------------------------------------------------------- |
| `MANAGE_DOMAIN` | Create domains and manage all of them              | Edit, publish, subdomains, backing tag, delete; see it while `DRAFT` |
| `USE_DOMAIN`    | See all published domains and browse their members | See this domain and browse its members                               |

`MANAGE_DOMAIN` includes `USE_DOMAIN`; owners and the metalake owner hold both. There is no separate `CREATE_DOMAIN`. Privileges on a domain, and its ownership, also apply to its subdomains; metalake-level grants cover every domain.

- **Access follows the current parent**: moving or detaching a subdomain changes who can see and manage it, without any grant changing. After Trading moves from Finance to Markets, Finance's holders lose access to it and Markets' holders gain it. A Finance manager can detach Trading through that inherited privilege, and then has none on it. Move and detach events carry the old and new parent, so an auditor can reconstruct why someone had access.

**What each operation needs**

| Operation                            | Required                                                                                                                                 |
| :----------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------- |
| See a domain                         | Published: `USE_DOMAIN`. Draft: owner or `MANAGE_DOMAIN`                                                                                 |
| Browse members, domains of an object | See the domain; results filtered by each object's own privileges; no tag privilege                                                       |
| Add or remove members                | Existing tag authorization (`CAN_ACCESS_METADATA_AND_TAG`): access to the object and `APPLY_TAG` on the backing tag. No domain privilege |
| Create with a new tag                | `MANAGE_DOMAIN` and `CREATE_TAG` on the metalake                                                                                         |
| Create with an existing tag          | `MANAGE_DOMAIN` on the metalake and ownership of the tag (or the metalake)                                                               |
| Create a subdomain                   | Also `MANAGE_DOMAIN` on the parent                                                                                                       |
| Set or replace the backing tag       | `MANAGE_DOMAIN` and ownership of the new tag (or the metalake)                                                                           |
| Remove the backing tag               | `MANAGE_DOMAIN`                                                                                                                          |
| Attach / move                        | `MANAGE_DOMAIN` on both the domain and the new parent                                                                                    |
| Detach                               | `MANAGE_DOMAIN` on the domain, including through its current parent or the metalake                                                      |
| Publish, unpublish, edit, delete     | `MANAGE_DOMAIN`                                                                                                                          |

- **Membership is controlled by tag privileges only**: anyone with read access to an asset and `APPLY_TAG` on a backing tag can add the asset, without the asset owner's consent. This is the existing tag behavior; a steward controls membership by controlling `APPLY_TAG` on the backing tag.
- Browsing without `VIEW_TAG` is safe because only a tag's owner can bind it to a domain: exposing that one (tag, value) to the domain's viewers is the owner's decision, and viewers still see only objects they can access.
- A user with `MANAGE_DOMAIN` but no `APPLY_TAG` on the tag can manage the domain but not add members.

**Implementation**

- **One parent resolver**: domain names carry no parent, so the name-based parent walk (`MetadataObjects.parent`) cannot find it. A single resolver returns a domain's current parent from `DomainRel` (one lookup, batched when filtering a list of domains). It is used by the domain endpoints and by the reused owner and grant checks, `JcasbinAuthorizer#hasSetOwnerPermission` and `#hasMetadataPrivilegePermission`, which consult it for `DOMAIN` objects where they walk to the parent. So inherited ownership also works through `/owners/domain/{domain}` and the role grant and revoke endpoints.
- **Deny still wins**: the three levels, metalake, current parent and domain, are combined in one check, the same way catalog, schema and table are today, never as "the domain, else the parent". An owner of any of the three passes. Otherwise a privilege holds when it is allowed on at least one level and denied on none, so `DENY USE_DOMAIN` on Trading overrides `ALLOW USE_DOMAIN` on Finance, and a deny on Finance overrides an allow on Trading.
- A parent or backing tag named in a request body (`parent` on create and attach, `backingTag` on create and on `PUT .../backingTag`) is checked imperatively: `MANAGE_DOMAIN` on the new parent; for an existing tag, ownership of the tag (or the metalake); for a new tag, `CREATE_TAG` on the metalake.

### Grants and policies

- **No data-access grants on domains.** Since anyone with `APPLY_TAG` on a backing tag can add an asset, an allow grant on a domain would turn tagging into granting access without the asset owner's consent. Possible future designs are listed in Open Questions.
- **Policies follow tags only.** Domains add no policy mechanism, and there are no "domain policies". An asset is governed by the policies associated with the tags it carries, the backing tag included, under the existing tag-policy semantics (association selectors for all values or one value). A domain passes no policy to its members or to its subdomains: domains and assets have no containment relation. An asset listed under Finance only via subdomain Trading is not governed by the policies on Finance's backing tag. Conversely, tagging an asset with a backing tag brings it under that tag's policies, as for any tag, so `APPLY_TAG` on a backing tag has a governance effect as well.

### REST API

Under `/api/metalakes/{metalake}`, following Gravitino conventions: `?details=` on lists, `PUT` + updates to alter, `PATCH` + a set request to switch state, `DropResponse` on delete, owners through the owner API.

| Operation                | Endpoint                                                                      | Request → response                                                                                           |
| :----------------------- | :---------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------- |
| List                     | `GET /domains?details=false`                                                  | → `NameListResponse` / `DomainListResponse`                                                                  |
| Create                   | `POST /domains`                                                               | `DomainCreateRequest{name, comment, properties, backingTag{name, value, create}, parent}` → `DomainResponse` |
| Get                      | `GET /domains/{domain}`                                                       | → `DomainResponse`                                                                                           |
| Alter                    | `PUT /domains/{domain}`                                                       | `DomainUpdatesRequest{updates: [rename, updateComment, setProperty, removeProperty]}` → `DomainResponse`     |
| Publish / unpublish      | `PATCH /domains/{domain}`                                                     | `DomainSetRequest{status}` → `DomainResponse`                                                                |
| Drop                     | `DELETE /domains/{domain}`                                                    | → `DropResponse` (`false` if absent)                                                                         |
| Attach / move            | `PUT /domains/{domain}/parent`                                                | `DomainParentSetRequest{parent}` → `DomainResponse`                                                          |
| Detach                   | `DELETE /domains/{domain}/parent`                                             | → `DomainResponse` (no-op for a top-level domain)                                                            |
| Set / remove backing tag | `PUT /domains/{domain}/backingTag`                                            | `DomainBackingTagSetRequest{backingTag{name, value}}` (null `backingTag` removes it) → `DomainResponse`      |
| List subdomains          | `GET /domains/{domain}/subdomains?details=false`                              | → `NameListResponse` / `DomainListResponse`                                                                  |
| List members             | `GET /domains/{domain}/objects`                                               | → `DomainMemberListResponse`                                                                                 |
| Domains of an object     | `GET /objects/{type}/{fullName}/domains?details=false` (leaf types only)      | → `NameListResponse` / `DomainListResponse`                                                                  |
| Add / remove members     | Existing `POST /objects/{type}/{fullName}/tags` with the domain's backing tag | `TagValuesAssociateRequest` (v2) or `TagsAssociateRequest` (v1)                                              |

```
DomainDTO       { name, comment, properties, status, parent, backingTag{name, value}, audit,
                  relations? }   // [{relation, source?}], only on domains of an object
DomainMemberDTO { object: MetadataObjectDTO, relations: [{relation, source?}] }
```

- **Create**: `backingTag.create = true` creates a new tag with the given name (no `value`); `false` binds the existing (tag, value). Domain, new tag and parent edge are written in one transaction; on any failure nothing is created, and no new tag is left behind.
- **Membership requests**: adding a member with a value needs the v2 `TagValuesAssociateRequest` (`Content-Type: application/vnd.gravitino.v2+json`), which carries (name, value) pairs. Removing a tag by name with the v1 `TagsAssociateRequest` removes all its values on the object. The Java and Python clients already support both.
- **Parent endpoints** address the child only, matching `Domain#attachToParent` and `Domain#detachFromParent`; detaching needs no parent name.
- **State changes return the domain**: publish, unpublish, attach, move, detach and backing-tag changes return the updated domain, because the Java API returns a new `Domain` snapshot from them.
- **Concurrency**: changes apply to the current state on the server and the last write wins, as for other Gravitino metadata operations; there is no version check. Detach removes the current parent edge and removing the backing tag removes the current one, whatever the caller's snapshot shows; detaching a top-level domain is a no-op, not an error. Domains are addressed by name, so if a domain is renamed and a new one takes its old name, later calls with that name act on the new domain.
- **Member listing parameters**:
  - `domains` (repeatable, `?domains=Tax&domains=Trading`; not comma-separated, since domain names may contain `,`): each name, the listed domain itself or one of its visible subdomains, selects that domain's scope, and the result is the union. On Finance with subdomains Tax and Trading, not setting it or `domains=Finance` lists the whole scope (Finance, Tax and Trading), `domains=Tax` lists only Tax's members, and `domains=Tax&domains=Trading` lists Tax's and Trading's. Names that are neither the domain nor a subdomain visible to the caller are ignored; if all given names are ignored, the result is empty rather than everything, so a typo cannot widen it.
  - `types` (comma-separated, each a leaf member type) and `nameContains` (case-insensitive, on the full name). Filters apply to the expanded leaf list, so `types=TABLE` finds the tables under a tagged catalog.
  - The listed domain's own members are those whose relations include Direct or Inherited.
  - No paging in Phase 1, like the other core list APIs; paging and counts come in Phase 2.
- **Errors**:
  - 404: `NoSuchDomainException` (missing, or a `DRAFT` domain the caller cannot see); `NoSuchTagException` (a backing tag to bind that does not exist).
  - 409: `DomainAlreadyExistsException` (create, or rename to an existing name); `NonEmptyDomainException` (drop with subdomains); `TagAlreadyExistsException` (a new backing tag whose name exists); `BackingTagAlreadyBoundException` (a binding that conflicts with another domain).
  - 400: `IllegalArgumentException` (C1–C6, status consistency, a value the tag does not allow, a create request without a backing tag or with both `create` and a value, a `DOMAIN` target on the generic tag association API).
  - 403: `ForbiddenException` (binding a tag the caller does not own, nor the metalake; creating a new tag without `CREATE_TAG`).
- **Encoding**: tag and domain names may contain `/`, which must be URL-encoded (`%2F`) in every path segment: `{tag}`, and `{domain}` in `/domains/{domain}`, `/objects/domain/{domain}/...`, `/owners/domain/{domain}` and `/permissions/roles/{role}/domain/{domain}/grant|revoke`. The Java, Python and MCP clients already encode path segments, and Gravitino's Jetty 9.4 / Jersey stack decodes them into the path parameters. An end-to-end test through the real server, with authentication and authorization enabled, will cover these paths, including filters that read the decoded path. Deployments behind a reverse proxy must pass `%2F` through without decoding it (for example, nginx `proxy_pass` without a URI part), and a later Jetty upgrade must keep ambiguous path separators allowed.
- **Reused**: owners at `/owners/domain/{domain}`; privileges through the role and permission APIs, with `DOMAIN` as a securable type.
- **Clients** (Java, Python) mirror the tag clients:
  - `DomainOperations` on `GravitinoMetalake`: list, get, `createDomain` (top-level domains only, with an explicit backing tag), alter and delete.
  - On a `Domain`, each returning a new `Domain` snapshot: `publish()`, `unpublish()`, `attachToParent(parent)` (attach or move), `detachFromParent()`, and `updateBackingTag(TagValue)` (`null` removes it; maps to `PUT /domains/{domain}/backingTag`). Also on a `Domain`: `createSubdomain(...)` (with an explicit backing tag), `listSubdomains()` / `listSubdomainsInfo()` and `listMembers(DomainMemberFilter)`.
  - `SupportsDomains` (`listDomains()`, `listDomainsInfo()`) on tables, views, filesets, topics, models and semantic models; not on catalogs, schemas, columns or functions.
- **Not included**: a batch lookup of the domains of many objects.

### MCP tools

A new read-only `domain` tool category in `mcp-server/`, added to `SUPPORTED_TOOL_TAGS` (enabled with `--include-tool-tags`):

| Tool                        | Endpoint                                                                                                                                                                                                                   |
| :-------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `list_domains`              | `GET /domains?details=true`; returns name, comment, parent, status and backing tag, so an agent can pick a domain                                                                                                          |
| `get_domain_by_name`        | `GET /domains/{domain}`                                                                                                                                                                                                    |
| `list_metadata_by_domain`   | `GET /domains/{domain}/objects`, with `domains` (a list of scopes to include, the domain itself or its subdomains; default the whole scope), `types`, `name_contains`; lets an agent look within a domain by type and name |
| `list_domains_for_metadata` | `GET /objects/{type}/{fullName}/domains`, for leaf objects                                                                                                                                                                 |

Adding members uses the existing `associate_tag_with_metadata` tool. It keeps removing tags by name (v1, all values) and is extended to accept (name, value) pairs and send the v2 request, so value-backed domains work. The MCP server forwards the caller's `Authorization` header, so the server's visibility rules apply; with OAuth client-credentials, the agent sees what that service identity sees.

### Events and audit

- Every domain operation emits pre, success and failure events through a `DomainEventDispatcher`, and reaches the audit log through the existing pipeline. Backing-tag changes are domain events. `Domain` and `DomainRel` carry `audit_info`.
- Membership is derived, so there is no membership event. It changes through operations that are already audited by existing events:
  - tag association changes (`AssociateTagsForMetadataObjectEvent`) on the asset or on its catalog or schema, and tag deletion;
  - asset lifecycle and hierarchy changes: creating or dropping an asset under a tagged catalog or schema, or moving it to another schema (for example `AlterTableEvent`);
  - backing-tag changes and subdomain moves, which are domain events.

  No per-member event is emitted.
- **The default audit log is not enough to reconstruct membership.** It records each event's common fields and `customInfo` (`JsonAuditFormatter`, `SimpleFormatterV2`), and tag association events carry no tag names or values there, so it shows which operations ran but not which (tag, value) changed. Reconstructing a domain's membership at a point in time needs an event listener that stores the full events above; this is not a Phase 1 goal. Adding tag payloads to the audit log would be a change for all tags, not a domain one.

## Alternatives Considered

1. **Tags alone**, with hierarchy and status added to tags. Tags would gain a lifecycle, a parent and curation privileges that every other tag user has to understand, and domain privileges (`MANAGE_DOMAIN`, `USE_DOMAIN`) do not fit tag privileges. A separate `Domain` object keeps the tag model unchanged.
2. **A dedicated membership table.** It would be a second source of truth next to tags, and catalog/schema inheritance, `APPLY_TAG` control, tag-based policies and tag audit events would all have to be rebuilt for it. Membership by tags reuses all of them.
3. **`DOMAIN` as a taggable object**, with the backing tag stored as the domain's row in `tag_relation_meta`. Every tag code path would need a `DOMAIN` special case (association APIs, tag object listings, policy resolution, name resolution, cleanup), the reverse lookup would scan all asset assignments of the tag, and a binding without a value would clash with allowed-values validation. Storing the binding on the domain row keeps the tag code unchanged.

## Prior Art

Unity Catalog domains ([Domains and subdomains](https://docs.databricks.com/aws/en/uc-semantics/domains), [Discover page](https://docs.databricks.com/aws/en/discover/discover-page)) are built the same way: a domain groups assets by governed tags, an asset joins a domain by carrying its tag, catalog and schema tags pass down to the assets inside, parent and subdomain tags are independent, and a subdomain's assets also appear under its parent. Access policies there are attached to the object hierarchy and target governed tags, not domains ([ABAC](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/)).

## Open Questions

1. **Cost of expanding large catalogs**: member listing expands a tagged catalog or schema to every recorded leaf object under it. Paging (Phase 2) bounds the response, not the work; is a cheaper strategy needed for very large catalogs?
2. **Data-access grants on domains**, if wanted later: deny-only grants (tagging can only tighten access), or allow grants that also require grant authority on the asset when tagging. Either works only where Gravitino enforces authorization itself and adds a membership lookup to every check.

## Task Breakdown

### Phase 0: Prerequisites

- [x] Tag values and constraints (apache/gravitino#12162), tag inheritance (`EffectiveTagResolver`), listing objects by tag and value, `/` encoding in the clients.
- [ ] End-to-end test through the real server, with authentication and authorization enabled, for `%2F` in `/domains/{domain}`, `/objects/domain/{domain}/...`, `/owners/domain/{domain}` and the role grant and revoke paths.

### Phase 1a: Domain, storage and authorization

- [ ] `Domain` API in `api/`; `MetadataObject.Type.DOMAIN` (not taggable)
- [ ] Storage: `domain_meta` with the backing tag columns and index, and `domain_rel` (H2, MySQL, PostgreSQL, upgrade scripts)
- [ ] Manager: create with an explicit backing tag, the binding rules, constraints C1–C6, status and its consistency checks, locking as in Locking (with `DOMAIN` in `LiveEndpointSQLProvider`), status-based visibility
- [ ] Authorization: `MANAGE_DOMAIN` and `USE_DOMAIN`, inherited from the current parent through one parent resolver (domain endpoints, plus the `DOMAIN` case in `JcasbinAuthorizer#hasSetOwnerPermission` and `#hasMetadataPrivilegePermission`), deny over allow across metalake, parent and domain, imperative checks for a parent or backing tag in a request body
- [ ] REST endpoints for domains, status, parent and backing tag; events (`DomainEventDispatcher`)
- [ ] Tests, including a multi-node test: move or change a backing tag on one server, then check visibility, inherited access, roll-ups and reverse lookups on another

### Phase 1b: Membership

- [ ] Membership resolution in `core/` on `EffectiveTagResolver`, returning the inherited source and matching by tag ID
- [ ] Lookup of tag relations by tag ID and value (the existing lookup is by tag name)
- [ ] Member listing: expansion to leaf objects, relation lists, filters; domains of a leaf object
- [ ] REST endpoints and tests

### Phase 1c: Clients, MCP and documentation

- [ ] Java and Python clients
- [ ] MCP tools, and (name, value) support in `associate_tag_with_metadata`
- [ ] OpenAPI spec (`docs/open-api/*.yaml`, validated with `./gradlew :docs:build`) and user docs: explicit backing tags, adding and removing members by tagging, relations, privileges, `/` encoding and the reverse-proxy note, tags not renamed on move or rename

### Phase 2: Paging and counts

- [ ] Paging for member listing, and domain asset counts
