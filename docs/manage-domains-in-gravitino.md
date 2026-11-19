---
title: "Manage Domains"
slug: "/manage-domains-in-gravitino"
keyword: "domain, subdomain, membership, backing tag, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

A Domain groups metadata across catalogs in one metalake under a business name, such as
`Finance`. Domains provide discovery and organization; they do not grant access to the data.
A domain has a name, optional comment, string properties, status, optional parent, backing tag,
and audit information. See [Domains](./domains.md) for membership and hierarchy semantics,
[Access control](./security/access-control.md#domain-permissions) for authorization, and the
[Domain OpenAPI specification](./open-api/domains.yaml) for request and response schemas.

Use a server and Java/Python clients built from the same version that supports Domains.
Follow [Java client setup](./how-to-use-gravitino-client.md) or
[Python client setup](./how-to-use-python-client.md) first. The examples below use simple
authentication as `anonymous`, a configured service administrator, on a local server at
`http://127.0.0.1:8090`. They create an isolated metalake and use the built-in model catalog,
so they need no external catalog service. Use unique names on a shared server and configure
authentication for your deployment. See [Authentication](./security/how-to-authenticate.md).

## Deployment prerequisites

For an existing deployment, complete the database schema upgrade for the server version before
using Domain APIs. The 1.3-to-2.0 upgrade scripts for H2, MySQL and PostgreSQL add the Domain
metadata and hierarchy tables; installing a new client alone does not upgrade the server's store.
Follow [Upgrade Gravitino](./how-to-upgrade.md) for the supported backup and upgrade procedure and
[Relational backend storage](./how-to-use-relational-backend-storage.md) for backend setup.
Existing tags do not automatically create domains: bind them explicitly using the operations below.

## API examples

Each operation below provides REST, Java and Python examples. They assume an existing metalake
named `test`, with clients connected to that metalake. Java uses `lake` (`GravitinoMetalake`);
Python uses `lake` (`GravitinoClient`). `domain` refers to a loaded Finance domain. Import Java
Domain types from `org.apache.gravitino.domain`, `TagValue` from `org.apache.gravitino.tag`, and
`MetadataObject` from `org.apache.gravitino`. Python uses `gravitino.api.domain`,
`gravitino.api.tag` and `gravitino.api.metadata_object` respectively. Domain APIs take the backing
tag as a tag name.
REST examples use simple authentication as the configured administrator `anonymous`.
These are independent operation examples; the [complete examples](#complete-java-example) below
show a runnable sequence with setup and cleanup.

All loaded Domain objects are snapshots. Mutations return a fresh snapshot and leave old objects
unchanged. An operation on an old snapshot applies to current server state (last write wins), using
the snapshot's name. After rename it can fail with not-found; if another domain reuses that old name,
the operation targets that new domain. Reload by name after changes; snapshots are not revision locks.

## Domain Operations

### Create a Domain

Create a top-level draft with an explicit backing tag. Here a new valueless tag is created
atomically with the domain. See [Backing Tag Operations](#backing-tag-operations) for existing-tag
bindings and constraints.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X POST -H 'Content-Type: application/json' \
  -d '{"name":"Finance","comment":"Financial assets","properties":{},"backingTag":{"name":"finance","create":true}}' \
  "http://127.0.0.1:8090/api/metalakes/test/domains"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain domain = lake.createDomain(
    "Finance", "Financial assets", Collections.emptyMap(),
    "finance", true);
```

</TabItem>
<TabItem value="python" label="Python">

```python
domain = lake.create_domain("Finance", "Financial assets", {}, "finance", True)
```

</TabItem>
</Tabs>

Import `java.util.Collections` for the Java examples that use `Collections.emptyMap()`.

### Get a Domain

Load a snapshot by its literal name. The snapshot exposes name, comment, properties, status,
parent, backing tag and audit information. Ordinary snapshots have no object membership relations.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain domain = lake.getDomain("Finance");
```

</TabItem>
<TabItem value="python" label="Python">

```python
domain = lake.get_domain("Finance")
```

</TabItem>
</Tabs>

### List Domains

Lists include visible top-level domains and subdomains. Names-only and detailed variants are
shown below; an empty result contains an empty array.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy \
  "http://127.0.0.1:8090/api/metalakes/test/domains"
```

</TabItem>
<TabItem value="java" label="Java">

```java
String[] names = lake.listDomains();
```

</TabItem>
<TabItem value="python" label="Python">

```python
names = lake.list_domains()
```

</TabItem>
</Tabs>

### List Domains with Details

Set `details=true` to retrieve snapshots instead of names.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy \
  "http://127.0.0.1:8090/api/metalakes/test/domains?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain[] domains = lake.listDomainsInfo();
```

</TabItem>
<TabItem value="python" label="Python">

```python
domains = lake.list_domains_info()
```

</TabItem>
</Tabs>

### Alter a Domain

Rename, update/clear the comment, or set/remove properties. Renaming preserves tags and hierarchy.
Use `null` / `None` to clear the comment. Reload snapshots addressed by the former name.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X PUT -H 'Content-Type: application/json' \
  -d '{"updates":[{"@type":"rename","newName":"Corporate_Finance"},{"@type":"updateComment","newComment":"Finance metadata"},{"@type":"setProperty","property":"contact","value":"finance-team"},{"@type":"removeProperty","property":"obsolete"}]}' \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain renamed = lake.alterDomain("Finance",
    DomainChange.rename("Corporate_Finance"),
    DomainChange.updateComment("Finance metadata"),
    DomainChange.setProperty("contact", "finance-team"),
    DomainChange.removeProperty("obsolete"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
renamed = lake.alter_domain(
    "Finance",
    DomainChange.rename("Corporate_Finance"),
    DomainChange.update_comment("Finance metadata"),
    DomainChange.set_property("contact", "finance-team"),
    DomainChange.remove_property("obsolete"),
)
```

</TabItem>
</Tabs>

### Publish a Domain

Publish the parent before publishing children. Repeating the current status is a no-op.
Publication exposes the domain to users who can use it; it does not grant data access.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X PATCH -H 'Content-Type: application/json' \
  -d '{"status":"published"}' \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain published = domain.publish();
```

</TabItem>
<TabItem value="python" label="Python">

```python
published = domain.publish()
```

</TabItem>
</Tabs>

### Unpublish a Domain

Unpublish children before their parent. A draft is visible only to its managers and owners.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X PATCH -H 'Content-Type: application/json' \
  -d '{"status":"draft"}' \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain draft = domain.unpublish();
```

</TabItem>
<TabItem value="python" label="Python">

```python
draft = domain.unpublish()
```

</TabItem>
</Tabs>

### Delete a Domain

Only a draft domain can be deleted. Unpublish a published domain first; deleting it fails with
HTTP 400 and `IllegalArgumentException`. Delete or detach children before deleting their parent;
deleting a draft parent with children fails with HTTP 409 and `NonEmptyDomainException`. The status
is checked first, so a published parent with children fails with HTTP 400. Members alone do not
prevent deletion. The operation keeps backing tags and object assignments and returns false if
the domain is absent.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X DELETE \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance"
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean deleted = lake.deleteDomain("Finance");
```

</TabItem>
<TabItem value="python" label="Python">

```python
deleted = lake.delete_domain("Finance")
```

</TabItem>
</Tabs>

## Subdomain Operations

There is one level of subdomains. Names are unique across the metalake, including children.
A child is addressed by its own literal name, without a parent prefix.

### Create a Subdomain

Create a draft and its parent link atomically. `domain` is the top-level Finance parent.
The explicit backing-tag creation and binding rules are the same as for a top-level domain.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X POST -H 'Content-Type: application/json' \
  -d '{"name":"Planning_US","parent":"Finance","backingTag":{"name":"planning","create":true}}' \
  "http://127.0.0.1:8090/api/metalakes/test/domains"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain child = domain.createSubdomain(
    "Planning_US", null, null, "planning", true);
```

</TabItem>
<TabItem value="python" label="Python">

```python
child = domain.create_subdomain("Planning_US", None, None, "planning", True)
```

</TabItem>
</Tabs>

### List Subdomains

List the names of immediate children visible to the caller.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance/subdomains"
```

</TabItem>
<TabItem value="java" label="Java">

```java
String[] children = domain.listSubdomains();
```

</TabItem>
<TabItem value="python" label="Python">

```python
children = domain.list_subdomains()
```

</TabItem>
</Tabs>

### List Subdomains with Details

Retrieve visible child snapshots rather than names.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance/subdomains?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain[] children = domain.listSubdomainsInfo();
```

</TabItem>
<TabItem value="python" label="Python">

```python
children = domain.list_subdomains_info()
```

</TabItem>
</Tabs>

### Attach or Move a Domain

Attach Planning_US under Finance, replacing any current parent. The destination must be top-level
and cannot be the domain itself. A domain with children cannot become a child; a published child
requires a published destination. Tags and assignments remain intact; membership roll-ups and
inherited permissions follow the new parent immediately. Attaching to the current parent is a no-op.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X PUT -H 'Content-Type: application/json' \
  -d '{"parent":"Finance"}' \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Planning_US/parent"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain child = lake.getDomain("Planning_US");
Domain attached = child.attachToParent("Finance");
```

</TabItem>
<TabItem value="python" label="Python">

```python
child = lake.get_domain("Planning_US")
attached = child.attach_to_parent("Finance")
```

</TabItem>
</Tabs>

### Detach a Domain

Make the child top-level without retagging its members. Former parent permissions and membership
roll-ups stop applying. Detaching a top-level domain is a no-op.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X DELETE \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Planning_US/parent"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain child = lake.getDomain("Planning_US");
Domain detached = child.detachFromParent();
```

</TabItem>
<TabItem value="python" label="Python">

```python
child = lake.get_domain("Planning_US")
detached = child.detach_from_parent()
```

</TabItem>
</Tabs>

## Backing Tag Operations

Creation always requires an explicit backing tag:

| Intent | Java | Python | REST `backingTag` |
| --- | --- | --- | --- |
| Create a new valueless tag | `"finance", true` | `"finance", True` | `{"name":"finance","create":true}` |
| Bind an existing tag | `"finance_data", false` | `"finance_data", False` | `{"name":"finance_data","create":false}` |

New backing tags allow no values and are owned by the creator. Creation never implicitly adopts an
existing tag: `create=true` with an existing name conflicts. Any existing tag can be bound, whatever
values it allows. Each domain has its own backing tag: a tag backs at most one domain, so binding a
tag that already backs another domain conflicts, even between a parent and its child.

Membership depends only on the presence of the backing tag. An object that carries the tag, with or
without values, matches; any value on an assignment is ignored.

`domain.updateBackingTag("finance_data")` or Python `domain.update_backing_tag("finance_data")`
replaces the binding with an existing tag. Pass `null` / `None` to remove it. Repeating a binding is
a no-op. Removing a binding does not delete the tag, change assignments, or remove child membership
roll-ups. Replacing it does not migrate members: only objects carrying the new tag contribute the
domain's own members.

Bindings use tag identity (ID), not its name. Renaming a tag preserves membership and exposes the new
tag name on a freshly loaded domain. Deleting it leaves the domain without a backing tag. Recreating
a tag with the old name does not rebind the domain; explicitly update the binding if that is intended.
Domain rename, move, detach and deletion never rename or delete its backing tag.

### Create a Domain with an Existing Backing Tag

Assume the tag `finance_data` already exists. Bind it explicitly with `create=false`; the tag can
be bound whatever values it allows, and objects carrying it are members whatever values their
assignments have. Binding requires
ownership of the existing tag or metalake when authorization is enabled.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X POST -H 'Content-Type: application/json' \
  -d '{"name":"Finance","backingTag":{"name":"finance_data","create":false}}' \
  "http://127.0.0.1:8090/api/metalakes/test/domains"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain domain = lake.createDomain(
    "Finance", null, null, "finance_data", false);
```

</TabItem>
<TabItem value="python" label="Python">

```python
domain = lake.create_domain(
    "Finance", None, None, "finance_data", False
)
```

</TabItem>
</Tabs>

### Replace a Backing Tag

Bind an existing tag by name. Members are not migrated and previous assignments
remain unchanged. Repeating the current binding is a no-op.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X PUT -H 'Content-Type: application/json' \
  -d '{"backingTag":{"name":"finance_data"}}' \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance/backingTag"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain updated = domain.updateBackingTag("finance_data");
```

</TabItem>
<TabItem value="python" label="Python">

```python
updated = domain.update_backing_tag("finance_data")
```

</TabItem>
</Tabs>

### Remove a Backing Tag

Remove the binding without deleting the tag or assignments. Visible child membership still rolls
up to this domain. Removing a binding needs no tag privilege.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X PUT -H 'Content-Type: application/json' \
  -d '{"backingTag":null}' \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance/backingTag"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain unbound = domain.updateBackingTag(null);
```

</TabItem>
<TabItem value="python" label="Python">

```python
unbound = domain.update_backing_tag(None)
```

</TabItem>
</Tabs>

## Membership Operations

Members are managed through the existing tag API. There is no separate Domain add/remove-member
endpoint. In the assignment examples, `model` is a loaded model named `models.sales.forecast` and
`finance` is the backing tag of the Finance domain. See [tag operations](./manage-tags-in-gravitino.md)
for creating constrained tags and assigning them to catalogs or schemas.

### Assign a Backing Tag to an Object

Assign the Finance backing tag to the model using normal tag permissions. The assignment follows the
tag's own constraint: the valueless examples below work for a tag without an allowed-values list, such
as one created with the domain. A tag with an allowed-values list must be assigned one of its allowed
values, for example `{"name":"finance","value":"risk"}` or `TagValue.of("finance", "risk")`. Domain
membership uses only the presence of the tag and ignores the value. The same API works for leaf,
schema and catalog assignments. A tagged container and its directly tagged descendants
appear separately. Use `expand=true` to include recorded leaves that inherit the binding.
Tag-value requests use `application/vnd.gravitino.v2+json`; Domain endpoints use the v1 API.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X POST -H 'Accept: application/vnd.gravitino.v2+json' \
  -H 'Content-Type: application/vnd.gravitino.v2+json' \
  -d '{"tagsToAdd":[{"name":"finance"}],"tagsToRemove":[]}' \
  "http://127.0.0.1:8090/api/metalakes/test/objects/model/models.sales.forecast/tags"
```

</TabItem>
<TabItem value="java" label="Java">

```java
model.supportsTags().associateTags(
    new TagValue[] {TagValue.noValue("finance")}, new TagValue[0]);
```

</TabItem>
<TabItem value="python" label="Python">

```python
model.supports_tags().assign_tags(
    tags_to_add=["finance"]
)
```

</TabItem>
</Tabs>

### Remove a Backing Tag Assignment

Remove the backing tag assigned directly to the model. Removing the tag by name with the v1 tag API
clears all its direct assignments on the object, with or without values. The v2 tag-value API removes
only the given pair, so an object that keeps another value of the tag remains a member. An ancestor
assignment can then become effective again, so this does not necessarily remove inherited Domain
membership.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy -X POST -H 'Accept: application/vnd.gravitino.v1+json' \
  -H 'Content-Type: application/json' \
  -d '{"tagsToRemove":["finance"]}' \
  "http://127.0.0.1:8090/api/metalakes/test/objects/model/models.sales.forecast/tags"
```

</TabItem>
<TabItem value="java" label="Java">

```java
model.supportsTags().associateTags(new String[0], new String[] {"finance"});
```

</TabItem>
<TabItem value="python" label="Python">

```python
model.supports_tags().associate_tags([], ["finance"])
```

</TabItem>
</Tabs>

### List Domain Members

List the accessible objects that directly carry the backing tag of the domain or its visible
children. A directly tagged schema and a directly tagged table under it both appear. Each matching
object occurs once with all applicable direct and subdomain relations.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance/objects"
```

</TabItem>
<TabItem value="java" label="Java">

```java
DomainMember[] members = domain.listMembers(DomainMemberFilter.builder().build());
```

</TabItem>
<TabItem value="python" label="Python">

```python
members = domain.list_members(DomainMemberFilter())
```

</TabItem>
</Tabs>

### List Expanded Domain Members

Set `expand` to replace each tagged catalog or schema by the accessible recorded leaves under it
that inherit the backing tag. Only leaves are returned, with `inherited` relations where they apply.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance/objects?expand=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
DomainMember[] leaves = domain.listMembers(DomainMemberFilter.builder().withExpand(true).build());
```

</TabItem>
<TabItem value="python" label="Python">

```python
leaves = domain.list_members(DomainMemberFilter(expand=True))
```

</TabItem>
</Tabs>

### Filter Domain Members

Filters intersect. `domains` selects the union of scopes, using one REST parameter per domain name. Selecting a child filters members but retains their full relations relative
to the queried domain. See [membership filters](./domains.md#membership-filters) for exact semantics.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy --get \
  --data-urlencode 'types=MODEL,TABLE' \
  --data-urlencode 'nameContains=forecast' \
  --data-urlencode 'domains=Planning_US' --data-urlencode 'domains=Tax' \
  "http://127.0.0.1:8090/api/metalakes/test/domains/Finance/objects"
```

</TabItem>
<TabItem value="java" label="Java">

```java
DomainMember[] members = domain.listMembers(
    DomainMemberFilter.builder()
        .withTypes(MetadataObject.Type.MODEL, MetadataObject.Type.TABLE)
        .withNameContains("forecast")
        .withDomains("Planning_US", "Tax")
        .build());
```

</TabItem>
<TabItem value="python" label="Python">

```python
members = domain.list_members(
    DomainMemberFilter(
        types=[MetadataObject.Type.MODEL, MetadataObject.Type.TABLE],
        name_contains="forecast",
        domains=["Planning_US", "Tax"],
    )
)
```

</TabItem>
</Tabs>

### List Domains for an Object

Reverse lookup supports tables, views, filesets, topics, models and semantic models. It returns
visible matching domains and their visible parents. For Topic lookups from Python, use REST with
`topic` as the object type. Here `model` is the loaded forecast model.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy \
  "http://127.0.0.1:8090/api/metalakes/test/objects/model/models.sales.forecast/domains"
```

</TabItem>
<TabItem value="java" label="Java">

```java
String[] names = model.supportsDomains().listDomains();
```

</TabItem>
<TabItem value="python" label="Python">

```python
names = model.supports_domains().list_domains()
```

</TabItem>
</Tabs>

### List Domains for an Object with Details

Detailed reverse lookup includes every applicable relation on each domain snapshot. Ordinary
domain snapshots have empty SDK `relations()` and omit the REST `relations` field.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -u anonymous:dummy \
  "http://127.0.0.1:8090/api/metalakes/test/objects/model/models.sales.forecast/domains?details=true"
```

</TabItem>
<TabItem value="java" label="Java">

```java
Domain[] domains = model.supportsDomains().listDomainsInfo();
```

</TabItem>
<TabItem value="python" label="Python">

```python
domains = model.supports_domains().list_domains_info()
```

</TabItem>
</Tabs>

## Complete Java example

Save this as `DomainExample.java` in a Java application with the Gravitino Java client dependency.
Run its `main` method using the application's dependency classpath. Assertions below throw even
when JVM assertions are disabled. With the client and its dependencies in
`GRAVITINO_CLIENT_CLASSPATH`, compile with `javac -cp "$GRAVITINO_CLIENT_CLASSPATH" DomainExample.java`
and run with `java -cp ".:$GRAVITINO_CLIENT_CLASSPATH" DomainExample`. Add the server URI as the
last argument to use another port. The example creates metadata, assigns a tag, publishes a domain,
queries membership in both directions, and cleans up the metalake and its contents.

```java
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.model.Model;

/** Demonstrates Domain discovery and cleanup on a local Gravitino server. */
public class DomainExample {
  /** Runs the example, optionally using the server URI supplied as the first argument. */
  public static void main(String[] args) throws Exception {
    String uri = args.length == 0 ? "http://127.0.0.1:8090" : args[0];
    String lakeName = "domain_java_" + Long.toUnsignedString(System.nanoTime());
    try (GravitinoAdminClient admin =
        GravitinoAdminClient.builder(uri).withSimpleAuth("anonymous").build()) {
      GravitinoMetalake lake = admin.createMetalake(lakeName, "Domain example", Collections.emptyMap());
      try {
        Catalog catalog = lake.createCatalog(
            "models", Catalog.Type.MODEL, "model", "", Collections.emptyMap());
        catalog.asSchemas().createSchema("sales", "", Collections.emptyMap());
        Model model = catalog.asModelCatalog().registerModel(
            NameIdentifier.of("sales", "forecast"), "", Collections.emptyMap());
        Domain draft = lake.createDomain(
            "Finance", "Financial assets", Collections.emptyMap(), "finance", true);
        Domain child = draft.createSubdomain(
            "Planning_US", null, null, "planning", true);
        model.supportsTags().associateTags(new String[] {"planning"}, new String[0]);
        Domain published = draft.publish();
        child = child.publish();
        if (draft.status() != Domain.Status.DRAFT || published.status() != Domain.Status.PUBLISHED) {
          throw new IllegalStateException("Expected independent snapshots");
        }
        DomainMember[] members = published.listMembers(
            DomainMemberFilter.builder().withNameContains("FORECAST")
                .withDomains("Planning_US").build());
        if (members.length != 1
            || members[0].relations()[0].type() != DomainRelation.Type.VIA_SUBDOMAIN
            || !members[0].relations()[0].source().orElseThrow().fullName().equals("Planning_US")) {
          throw new IllegalStateException("Expected the child membership and its complete source");
        }
        if (!Arrays.asList(model.supportsDomains().listDomains()).contains("Finance")) {
          throw new IllegalStateException("Expected the parent in reverse lookup");
        }
        Domain[] domains = model.supportsDomains().listDomainsInfo();
        if (domains.length != 2) {
          throw new IllegalStateException("Expected parent and child");
        }
        child = child.detachFromParent();
        if (Arrays.asList(model.supportsDomains().listDomains()).contains("Finance")) {
          throw new IllegalStateException("Detached membership must stop rolling up");
        }
        // Only draft domains can be deleted.
        lake.deleteDomain(child.unpublish().name());
        lake.deleteDomain(published.unpublish().name());
        // Domain deletion keeps tags and assignments. Remove the assignment explicitly.
        model.supportsTags().associateTags(new String[0], new String[] {"planning"});
        lake.deleteTag("planning");
        lake.deleteTag("finance");
      } finally {
        admin.dropMetalake(lakeName, true);
      }
    }
  }
}
```

## Complete Python example

Save this as `domain_example.py` and run `python domain_example.py`, with the Python client installed.
An optional first argument supplies a different server URI.

```python
import os
import sys
import uuid

from gravitino import Catalog, GravitinoAdminClient, GravitinoClient, NameIdentifier
from gravitino.api.domain import Domain, DomainMemberFilter, DomainRelation
from gravitino.auth.simple_auth_provider import SimpleAuthProvider

os.environ["GRAVITINO_USER"] = "anonymous"
uri = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8090"
lake_name = "domain_python_" + uuid.uuid4().hex
admin = GravitinoAdminClient(uri, auth_data_provider=SimpleAuthProvider())
lake = None
created = False
try:
    admin.create_metalake(lake_name, "Domain example", {})
    created = True
    lake = GravitinoClient(uri, lake_name, auth_data_provider=SimpleAuthProvider())
    catalog = lake.create_catalog("models", Catalog.Type.MODEL, "model", "", {})
    catalog.as_schemas().create_schema("sales", "", {})
    model = catalog.as_model_catalog().register_model(
        NameIdentifier.of("sales", "forecast"), "", {}
    )
    draft = lake.create_domain("Finance", "Financial assets", {}, "finance", True)
    child = draft.create_subdomain("Planning_US", None, None, "planning", True)
    model.supports_tags().associate_tags(["planning"], [])
    published = draft.publish()
    child = child.publish()
    assert draft.status() == Domain.Status.DRAFT
    assert published.status() == Domain.Status.PUBLISHED
    members = published.list_members(
        DomainMemberFilter(name_contains="FORECAST", domains=["Planning_US"])
    )
    assert len(members) == 1
    assert members[0].relations()[0].type() == DomainRelation.Type.VIA_SUBDOMAIN
    assert members[0].relations()[0].source().full_name() == "Planning_US"
    assert "Finance" in model.supports_domains().list_domains()
    assert len(model.supports_domains().list_domains_info()) == 2
    child = child.detach_from_parent()
    assert "Finance" not in model.supports_domains().list_domains()
    # Only draft domains can be deleted.
    lake.delete_domain(child.unpublish().name())
    lake.delete_domain(published.unpublish().name())
    model.supports_tags().associate_tags([], ["planning"])
    lake.delete_tag("planning")
    lake.delete_tag("finance")
finally:
    if lake is not None:
        lake.close()
    if created:
        admin.drop_metalake(lake_name, True)
    admin.close()
```

## Path encoding and errors

Domain names must match `[A-Za-z0-9_][A-Za-z0-9_-]{0,63}`. The server reserves `*` and
`system` (in any letter case) and preserves the spelling of valid names. The same rules apply
to subdomains, rename targets, parent names and each name in the `domains` member filter.
Use the child's own name as one path segment. Percent-encoding a space, dot or slash does
not make it a valid domain name. Pass raw names to SDK methods. SDK path builders can reject
unsafe segments before sending a request; the server enforces the full naming rules.

Backing tags, metadata object names and `nameContains` search text have separate rules.
Encode metadata path parameters individually and supply query values with `--data-urlencode`.

| HTTP status | Typical Domain behavior                                                                                                                                                                            |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 200         | Success with `code: 0`; empty queries return empty arrays; deleting an absent domain, or a draft hidden from the caller, returns `dropped: false` and deletes nothing.                             |
| 400         | Invalid request: invalid domain names or filters, unsupported member types, illegal hierarchy/status transition, or deleting a published domain.                                                   |
| 403         | Insufficient permission on a published domain, a required backing tag, or a reverse-lookup object.                                                                                                 |
| 404         | Missing metalake/domain/parent/tag/object, or a draft hidden from the caller. `DELETE` on a domain returns 404 only for a missing metalake.                                                        |
| 409         | Duplicate domain/tag name, a tag that already backs another domain, deleting a draft parent with children, disabled metalake, or an optimistic lock conflict during domain alteration or deletion. |
| 415         | Unsupported request Content-Type; the error response uses `application/json`.                                                                                                                      |
| 5xx         | Server failure; inspect the error response and server log.                                                                                                                                         |

Authentication failures follow the configured authenticator. The JSON error response contains the
exception type and message; do not infer authorization from empty lists alone. Validation and
visibility checks can run before existence checks, so the first failing check determines the error.

## MCP access

The [existing read-only Domain MCP tools](./gravitino-mcp-server.md#domains-read-only) expose discovery,
subdomain listing, member queries and object-to-domain lookup with the same server-side visibility.
Use REST or the Java/Python clients for Domain mutations. Domain UI, pagination/counts and Domain-based
data authorization are not provided by this feature.
