---
title: "Domains"
slug: "/domains"
keyword: "domain, subdomain, metadata discovery, membership"
license: "This software is licensed under the Apache License version 2."
---

## Domains in a metalake

A Domain is a business grouping of metadata in a metalake. For example, `Finance` can bring together
a table in an Iceberg catalog, a Kafka topic and a registered model. Domains organize discovery;
they do not change catalog structure, move data or grant data access.

Names are unique across all domains in a metalake, including subdomains. Two parents cannot each
have a child with the same name. Domain names must match `[A-Za-z0-9_][A-Za-z0-9_-]{0,63}`.
The names `*` and `system` (in any letter case) are reserved. Valid names retain their spelling:
a child named `Planning_US` under `Finance` is addressed by `Planning_US`, not `Finance.Planning_US`.
A domain is either top-level or the child of exactly one top-level domain. There is only one level
of subdomains. A top-level domain's scope includes its own members and those of its visible children.

Every domain starts as `DRAFT`. Only its managers and owners (including inherited managers/owners)
can see it until publication. `PUBLISHED` domains are visible to users with `USE_DOMAIN`.
Publication does not make metadata public. A child can be published only under a published parent;
a parent cannot be unpublished while a child is published. See the
[Domain guide](./manage-domains-in-gravitino.md) for lifecycle and snapshot behavior.

## Membership through tags

A domain explicitly binds a backing tag, optionally with one value. Membership is derived from
existing [tag assignments and inheritance](./tags.md), rather than maintained in a separate list.
Use the [tag API](./manage-tags-in-gravitino.md) to assign/remove a backing tag on a leaf, schema or
catalog. There is no Domain add-member or remove-member API.

Only these leaf types are members: **table, view, fileset, topic, model and semantic model**.
Tagged catalogs and schemas contribute their recorded leaves; they are never members themselves.
Only objects already recorded by Gravitino are enumerated: a tag on an external catalog is not a
request to crawl its backend. Empty results do not establish that external objects are absent.

For the same tag, the assignment nearest the leaf wins: leaf before schema before catalog.
Assignments are replaced as a whole at each nearer level, not combined with ancestor values.
For example, `department=hr` on a table masks `department=finance` on its schema, so the table does
not belong to the domain backed by `department=finance`. A binding without a value matches any
effective assignment of that tag; a valued binding tests exact, case-sensitive containment in the
assignment's values. Different tags resolve independently, allowing membership in multiple domains.

Removing a tag from a leaf changes only its own assignment; it is not an opt-out from inheritance.
If its schema or catalog still assigns that backing tag, the leaf remains a member through
inheritance. Removing the last directly assigned value can also reveal an ancestor assignment and
move the leaf back into a different valued domain. To remove inherited membership, change the
assignment at its source or use a nearer assignment that does not match the valued binding.
A valueless backing-tag binding matches any assignment of that tag, so assigning another value
cannot exclude the leaf from that domain.

## Relations and sources

A member query lists each object once, with **all** applicable relations relative to the queried
domain. The same relations appear on detailed object-to-domain lookup. Relations have no precedence;
clients must preserve their whole array.

| SDK relation | REST `relation` | `source` |
| --- | --- | --- |
| `DIRECT` | `direct` | Omitted: the leaf itself carries the backing tag. |
| `INHERITED` | `inherited` | The nearest assigned catalog or schema, with its complete `fullName` and `type`. |
| `VIA_SUBDOMAIN` | `via_subdomain` | The contributing child's literal name as `fullName`, with `type: domain`. |

For example, a schema `ice.sales` carries Finance's backing tag and its table
`ice.sales.forecast` carries Planning's separate backing tag. Querying Finance, whose child is
Planning, returns the table with both `inherited` from `ice.sales` and `via_subdomain` from Planning.
A direct assignment of Finance's tag masks its inherited assignment of that same tag, but the
independent Planning relation remains. Multiple contributing children produce multiple
`via_subdomain` relations. A child scope selected by a filter still reports relations relative to
the domain addressed in the query. Filtering to one child selects matching objects but does not
trim their relations: a selected object can still report the queried domain's own relation and
relations through other visible children.

Moving or detaching Planning changes parent roll-ups without changing any object assignment.
Renaming Planning changes the child source name on subsequent queries. See
[Backing tag operations](./manage-domains-in-gravitino.md#backing-tag-operations) for binding conflicts,
tag renames, tag identity and removal behavior.

## Membership filters

Java uses `DomainMemberFilter.builder()`; Python uses `DomainMemberFilter(...)`.
REST uses `GET /api/metalakes/{metalake}/domains/{domain}/objects`.
All supplied options intersect; an unset option does not narrow the result.

| Option | Meaning |
| --- | --- |
| `types` | Union of supported leaf types; REST accepts comma-separated, case-insensitive names with whitespace trimmed. Omit for all six types. |
| `nameContains` (Python `name_contains`) | Case-insensitive substring of the full object name, including catalog/schema text. No wildcard or regex interpretation. Omit for any name; blank text is invalid. |
| `domains` | Union of selected scopes within the queried domain. Omit or use an empty SDK collection for its whole scope. |

For Finance with children Tax and Planning:

- Unset `domains`, or selecting `Finance`, includes Finance, Tax and Planning (subject to visibility).
- Selecting `Tax` includes only Tax's scope; selecting Tax and Planning includes their union.
- Selecting Finance does **not** mean only Finance's own members. To retain those, inspect relations
  for `DIRECT` or `INHERITED` in the returned members.
- Well-formed unknown names, unrelated domains and invisible children are ignored. If every supplied name is
  ignored, the result is empty. Duplicate names do not duplicate members.

REST `domains` is a **repeated query parameter**, not a comma-separated list. For example,
`?domains=Planning_US&domains=Tax` selects the child named `Planning_US` plus Tax.
Use `curl --get --data-urlencode 'domains=Planning_US' --data-urlencode 'domains=Tax' ...`.
In contrast, `types=TABLE,VIEW` selects two types. Empty/unsupported type tokens, invalid domain scope
names and blank `nameContains` return 400. Empty successful member responses are
`{"code":0,"members":[]}`; domain names and details return `names: []` and `domains: []` respectively.
No paging or total count is provided. The current implementation sorts members by metadata type
name and then full object name; clients should use object identity rather than array position.

## Object-to-domain lookup

The REST `type` spellings are:

| Leaf resource | REST `type` |
| --- | --- |
| Table | `table` |
| View | `view` |
| Fileset | `fileset` |
| Topic | `topic` |
| Model | `model` |
| Semantic model | `semantic_model` |

All six leaf resources expose Java `supportsDomains().listDomains()` and `listDomainsInfo()`.
In Python, tables, views, filesets, models and semantic models expose
`supports_domains().list_domains()` and `list_domains_info()`. For topics, use the REST endpoint
with `type=topic`, for example `GET /api/metalakes/test/objects/topic/kafka.sales.orders/domains`.
REST uses `/api/metalakes/{metalake}/objects/{type}/{fullName}/domains`, with `details=true` for
snapshots and complete relations. Encode `fullName` as one path segment. Catalogs and schemas are
not valid leaf types for this endpoint. A leaf may belong to multiple domains and their parents;
no membership returns an empty list. Normal domain listings omit `relations`, while detailed
reverse lookups include them. SDK `relations()` is empty on ordinary snapshots.

Queries respect [Domain permissions](./security/access-control.md#domain-permissions) and ordinary
metadata access. They grant no right to read data. The read-only
[MCP Domain tools](./gravitino-mcp-server.md#domains-read-only) expose these same discovery semantics.
Domain UI and Domain-based data authorization are outside this feature.
