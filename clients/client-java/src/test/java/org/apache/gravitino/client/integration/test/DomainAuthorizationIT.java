/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.client.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.exceptions.BackingTagAlreadyBoundException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.TagValue;
import org.junit.jupiter.api.Test;

/** SDK tests with simple authentication, real owners, role grants, denies and Casbin. */
public class DomainAuthorizationIT extends DomainIT {
  /** {@inheritDoc} */
  @Override
  protected boolean authorizationEnabled() {
    return true;
  }

  @Test
  void testVisibilityInheritedPermissionsAndDenyPrecedence() {
    Domain parent =
        metalake.createDomain("Parent/auth. scope", null, null, TagValue.noValue("parent"), true);
    Domain child =
        parent.createSubdomain("Child/auth. scope", null, null, TagValue.noValue("child"), true);
    for (String user : List.of("reader", "manager", "outsider", "owner", "lake_manager")) {
      metalake.addUser(user);
    }

    // Grant only parent scopes so the child must inherit its permissions.
    grant("reader", List.of(scope(parent.name(), Privileges.UseDomain.allow())));
    grant("manager", List.of(scope(parent.name(), Privileges.ManageDomain.allow())));
    try (GravitinoAdminClient readerClient = userClient("reader");
        GravitinoAdminClient managerClient = userClient("manager");
        GravitinoAdminClient outsiderClient = userClient("outsider");
        GravitinoAdminClient ownerClient = userClient("owner");
        GravitinoAdminClient inheritedClient = userClient("lake_manager")) {
      GravitinoMetalake reader = readerClient.loadMetalake(metalake.name());
      GravitinoMetalake manager = managerClient.loadMetalake(metalake.name());
      GravitinoMetalake outsider = outsiderClient.loadMetalake(metalake.name());

      // Drafts are hidden from readers but visible to inherited managers.
      assertEquals(0, reader.listDomains().length);
      assertThrows(NoSuchDomainException.class, () -> reader.getDomain(parent.name()));
      assertEquals(child.name(), manager.getDomain(child.name()).name());

      // Publication changes visibility independently for the parent and child.
      manager.getDomain(parent.name()).publish();
      assertEquals(Set.of(parent.name()), Set.of(reader.listDomains()));
      assertThrows(NoSuchDomainException.class, () -> reader.getDomain(child.name()));
      manager.getDomain(child.name()).publish();
      assertEquals(Set.of(parent.name(), child.name()), Set.of(reader.listDomains()));
      assertEquals(0, outsider.listDomains().length);
      assertThrows(ForbiddenException.class, () -> outsider.getDomain(parent.name()));
      assertThrows(ForbiddenException.class, () -> reader.getDomain(parent.name()).unpublish());

      // A child deny takes precedence over an allow inherited from the parent.
      grant("reader", List.of(scope(child.name(), Privileges.UseDomain.deny())));
      assertEquals(Set.of(parent.name()), Set.of(reader.listDomains()));
      assertThrows(ForbiddenException.class, () -> reader.getDomain(child.name()));

      // Owners bypass denies; metalake grants also apply throughout the domain hierarchy.
      metalake.setOwner(
          MetadataObjects.of(null, parent.name(), MetadataObject.Type.DOMAIN),
          "owner",
          Owner.Type.USER);
      grant(
          "owner",
          List.of(
              scope(child.name(), Privileges.UseDomain.deny(), Privileges.ManageDomain.deny())));
      GravitinoMetalake owner = ownerClient.loadMetalake(metalake.name());
      assertEquals(child.name(), owner.getDomain(child.name()).name());
      assertEquals(Domain.Status.DRAFT, owner.getDomain(child.name()).unpublish().status());
      grant(
          "lake_manager",
          List.of(
              SecurableObjects.ofMetalake(
                  metalake.name(), List.of(Privileges.ManageDomain.allow()))));
      assertEquals(
          child.name(),
          inheritedClient.loadMetalake(metalake.name()).getDomain(child.name()).name());
    }
  }

  @Test
  void testBackingTagOwnershipAndCreationPermissions() {
    metalake.addUser("creator");
    grant(
        "creator",
        List.of(
            SecurableObjects.ofMetalake(
                metalake.name(), List.of(Privileges.ManageDomain.allow()))));
    try (GravitinoAdminClient creatorClient = userClient("creator")) {
      GravitinoMetalake creator = creatorClient.loadMetalake(metalake.name());
      assertThrows(
          ForbiddenException.class,
          () -> creator.createDomain("denied", null, null, TagValue.noValue("new tag"), true));

      // Creating a backing tag needs CREATE_TAG in addition to domain management.
      grant(
          "creator",
          List.of(
              SecurableObjects.ofMetalake(metalake.name(), List.of(Privileges.CreateTag.allow()))));
      Domain created =
          creator.createDomain("created", null, null, TagValue.noValue("owned tag"), true);

      // Domain management does not grant ownership of an unrelated tag.
      metalake.createTag("foreign tag", null, Collections.emptyMap());
      assertThrows(
          ForbiddenException.class,
          () -> created.updateBackingTag(TagValue.noValue("foreign tag")));
      assertThrows(
          ForbiddenException.class,
          () ->
              creator.createDomain("foreign", null, null, TagValue.noValue("foreign tag"), false));

      metalake.setOwner(
          MetadataObjects.of(null, "foreign tag", MetadataObject.Type.TAG),
          "creator",
          Owner.Type.USER);
      Domain bound =
          creator.createDomain("bound", null, null, TagValue.noValue("foreign tag"), false);
      assertEquals(TagValue.noValue("foreign tag"), bound.backingTag().orElseThrow());
      assertTrue(bound.updateBackingTag(null).backingTag().isEmpty());
      created.updateBackingTag(TagValue.noValue("foreign tag"));
      assertThrows(
          BackingTagAlreadyBoundException.class,
          () ->
              creator.createDomain("conflict", null, null, TagValue.noValue("foreign tag"), false));
    }
  }

  @Test
  void testMemberAccessWithoutTagPrivilegesAndHiddenSubdomains() {
    Catalog catalog = createRelationalCatalog();
    Column[] columns = {Column.of("id", Types.LongType.get())};
    Table table =
        catalog
            .asTableCatalog()
            .createTable(
                NameIdentifier.of("schema", "orders"), columns, "", Collections.emptyMap());
    catalog
        .asTableCatalog()
        .createTable(NameIdentifier.of("schema", "hidden"), columns, "", Collections.emptyMap());
    Domain parent =
        metalake.createDomain("access parent", null, null, TagValue.noValue("parent tag"), true);
    Domain child =
        parent.createSubdomain("access child", null, null, TagValue.noValue("child tag"), true);
    parent.publish();
    catalog
        .asSchemas()
        .loadSchema("schema")
        .supportsTags()
        .associateTags(new String[] {"parent tag"}, new String[0]);
    table.supportsTags().associateTags(new String[] {"child tag"}, new String[0]);

    // Give access to one leaf object without granting any tag privileges.
    metalake.addUser("object_reader");
    SecurableObject catalogScope =
        SecurableObjects.ofCatalog("ice", List.of(Privileges.UseCatalog.allow()));
    SecurableObject schemaScope =
        SecurableObjects.ofSchema(
            SecurableObjects.ofCatalog("ice", Collections.emptyList()),
            "schema",
            List.of(Privileges.UseSchema.allow()));
    SecurableObject tableScope =
        SecurableObjects.ofTable(schemaScope, "orders", List.of(Privileges.SelectTable.allow()));
    grant(
        "object_reader",
        List.of(
            scope(parent.name(), Privileges.UseDomain.allow()),
            catalogScope,
            schemaScope,
            tableScope));
    try (GravitinoAdminClient readerClient = userClient("object_reader")) {
      GravitinoMetalake reader = readerClient.loadMetalake(metalake.name());
      DomainMember[] visible =
          reader.getDomain(parent.name()).listMembers(DomainMemberFilter.builder().build());
      assertEquals(1, visible.length);
      assertEquals("ice.schema.orders", visible[0].object().fullName());
      assertEquals(DomainRelation.Type.INHERITED, visible[0].relations()[0].type());
      assertEquals(1, visible[0].relations().length);
      assertEquals("ice.schema", visible[0].relations()[0].source().orElseThrow().fullName());
      assertEquals(0, reader.getDomain(parent.name()).listSubdomains().length);
      assertEquals(0, reader.getDomain(parent.name()).listSubdomainsInfo().length);
      assertEquals(1, reader.listDomainsInfo().length);
      assertEquals(
          0,
          reader
              .getDomain(parent.name())
              .listMembers(DomainMemberFilter.builder().withDomains(child.name()).build())
              .length);

      // The draft child must not contribute a roll-up relation before publication.
      child.publish();
      visible = reader.getDomain(parent.name()).listMembers(DomainMemberFilter.builder().build());
      assertEquals(
          Set.of(DomainRelation.Type.INHERITED, DomainRelation.Type.VIA_SUBDOMAIN),
          Arrays.stream(visible[0].relations())
              .map(DomainRelation::type)
              .collect(Collectors.toSet()));
      Table readerTable =
          reader
              .loadCatalog("ice")
              .asTableCatalog()
              .loadTable(NameIdentifier.of("schema", "orders"));
      assertEquals(
          Set.of(parent.name(), child.name()), Set.of(readerTable.supportsDomains().listDomains()));

      // A visible object's hidden child relation must disappear without hiding parent membership.
      grant("object_reader", List.of(scope(child.name(), Privileges.UseDomain.deny())));
      DomainMember[] parentOnly =
          reader.getDomain(parent.name()).listMembers(DomainMemberFilter.builder().build());
      assertEquals(1, parentOnly.length);
      assertEquals(1, parentOnly[0].relations().length);
      assertEquals(DomainRelation.Type.INHERITED, parentOnly[0].relations()[0].type());
      assertEquals(Set.of(parent.name()), Set.of(readerTable.supportsDomains().listDomains()));
      assertEquals(1, readerTable.supportsDomains().listDomainsInfo()[0].relations().length);

      // A table deny removes the member and blocks reverse lookup even with USE_DOMAIN.
      grant(
          "object_reader",
          List.of(
              SecurableObjects.ofTable(
                  schemaScope, "orders", List.of(Privileges.SelectTable.deny()))));
      assertEquals(
          0,
          reader.getDomain(parent.name()).listMembers(DomainMemberFilter.builder().build()).length);
      assertThrows(ForbiddenException.class, () -> readerTable.supportsDomains().listDomains());
    }
  }

  @Test
  void testMoveReevaluatesParentGrantsAndMetalakeOwnership() {
    Domain first =
        metalake.createDomain("first", null, null, TagValue.noValue("first tag"), true).publish();
    Domain second =
        metalake.createDomain("second", null, null, TagValue.noValue("second tag"), true).publish();
    Domain child =
        first
            .createSubdomain("moving/child. name", null, null, TagValue.noValue("moving tag"), true)
            .publish();
    metalake.addUser("moving_manager");
    metalake.addUser("lake_owner");
    grant("moving_manager", List.of(scope(first.name(), Privileges.ManageDomain.allow())));
    try (GravitinoAdminClient managerClient = userClient("moving_manager");
        GravitinoAdminClient ownerClient = userClient("lake_owner")) {
      GravitinoMetalake manager = managerClient.loadMetalake(metalake.name());
      Domain snapshot = manager.getDomain(child.name());
      assertThrows(ForbiddenException.class, () -> snapshot.attachToParent(second.name()));

      // A stale snapshot must use current parent grants, rather than its loaded parent.
      child.attachToParent(second.name());
      assertThrows(ForbiddenException.class, snapshot::unpublish);

      grant("moving_manager", List.of(scope(second.name(), Privileges.ManageDomain.allow())));
      assertEquals(Domain.Status.DRAFT, snapshot.unpublish().status());
      assertEquals(first.name(), snapshot.parent().orElseThrow());

      grant("moving_manager", List.of(scope(child.name(), Privileges.ManageDomain.deny())));
      assertThrows(NoSuchDomainException.class, () -> manager.getDomain(child.name()));
      grant(
          "lake_owner",
          List.of(
              scope(child.name(), Privileges.ManageDomain.deny(), Privileges.UseDomain.deny())));

      // Restore ownership on failure too, so teardown can still remove the metalake.
      MetadataObject root = MetadataObjects.of(null, metalake.name(), MetadataObject.Type.METALAKE);
      metalake.setOwner(root, "lake_owner", Owner.Type.USER);
      GravitinoMetalake owner = ownerClient.loadMetalake(metalake.name());
      try {
        assertEquals(child.name(), owner.getDomain(child.name()).name());
      } finally {
        owner.setOwner(root, "anonymous", Owner.Type.USER);
      }
    }
  }

  @Test
  void testWriteEndpointsRejectReadersAndExplicitManagementDenies() {
    Domain parent =
        metalake
            .createDomain("matrix-parent", null, null, TagValue.noValue("matrix-parent-tag"), true)
            .publish();
    Domain child =
        parent
            .createSubdomain("matrix-child", null, null, TagValue.noValue("matrix-child-tag"), true)
            .publish();
    Domain target =
        metalake
            .createDomain("matrix-target", null, null, TagValue.noValue("matrix-target-tag"), true)
            .publish();

    Domain creationParent =
        metalake
            .createDomain(
                "creation-parent", null, null, TagValue.noValue("creation-parent-tag"), true)
            .publish();

    for (String user : List.of("matrix_reader", "matrix_denied")) {
      metalake.addUser(user);
      grant(
          user,
          List.of(
              SecurableObjects.ofMetalake(
                  metalake.name(),
                  List.of(Privileges.UseDomain.allow(), Privileges.CreateTag.allow()))));
      if (user.equals("matrix_denied")) {
        grant(
            user,
            List.of(
                SecurableObjects.ofMetalake(
                    metalake.name(), List.of(Privileges.ManageDomain.allow())),
                scope(child.name(), Privileges.ManageDomain.deny()),
                scope(creationParent.name(), Privileges.ManageDomain.deny())));
      }

      try (GravitinoAdminClient userClient = userClient(user)) {
        GravitinoMetalake reader = userClient.loadMetalake(metalake.name());
        Domain snapshot = reader.getDomain(child.name());
        // USE_DOMAIN keeps reads available even when management is explicitly denied.
        assertEquals(child.name(), snapshot.name());
        assertEquals(0, snapshot.listMembers(DomainMemberFilter.builder().build()).length);
        assertEquals(0, snapshot.listSubdomains().length);
        assertEquals(0, snapshot.listSubdomainsInfo().length);
        assertThrows(
            ForbiddenException.class,
            () -> reader.alterDomain(child.name(), DomainChange.updateComment("denied")),
            user);
        assertThrows(ForbiddenException.class, () -> reader.deleteDomain(child.name()), user);
        assertThrows(ForbiddenException.class, snapshot::publish, user);
        assertThrows(ForbiddenException.class, snapshot::unpublish, user);
        assertThrows(ForbiddenException.class, () -> snapshot.attachToParent(target.name()), user);
        assertThrows(ForbiddenException.class, snapshot::detachFromParent, user);
        assertThrows(ForbiddenException.class, () -> snapshot.updateBackingTag(null), user);
        assertThrows(
            ForbiddenException.class,
            () ->
                reader
                    .getDomain(creationParent.name())
                    .createSubdomain(
                        "denied-subdomain", null, null, TagValue.noValue("denied-tag"), true),
            user);
        if (user.equals("matrix_reader")) {
          assertThrows(
              ForbiddenException.class,
              () ->
                  reader.createDomain(
                      "denied-domain", null, null, TagValue.noValue("denied-create-tag"), true));
        }
      }

      // Every rejected write leaves the original state intact, with cache enabled.
      Domain unchanged = metalake.getDomain(child.name());
      assertEquals(Domain.Status.PUBLISHED, unchanged.status());
      assertEquals(parent.name(), unchanged.parent().orElseThrow());
      assertEquals(child.backingTag(), unchanged.backingTag());
      assertEquals(child.comment(), unchanged.comment());
      assertEquals(Set.of(child.name()), Set.of(parent.listSubdomains()));
      assertEquals(0, creationParent.listSubdomains().length);
      for (String name : List.of("denied-subdomain", "denied-domain")) {
        assertThrows(NoSuchDomainException.class, () -> metalake.getDomain(name));
      }
      for (String name : List.of("denied-tag", "denied-create-tag")) {
        assertThrows(NoSuchTagException.class, () -> metalake.getTag(name));
      }
    }
  }

  @Test
  void testNonOwnerManagerCanUseWriteEndpoints() {
    Domain parent =
        metalake
            .createDomain(
                "allowed-parent", null, null, TagValue.noValue("allowed-parent-tag"), true)
            .publish();
    Domain target =
        metalake
            .createDomain(
                "allowed-target", null, null, TagValue.noValue("allowed-target-tag"), true)
            .publish();
    Domain child =
        parent
            .createSubdomain(
                "allowed-child", null, null, TagValue.noValue("allowed-child-tag"), true)
            .publish();
    Domain creationParent =
        metalake
            .createDomain(
                "allowed-creation-parent",
                null,
                null,
                TagValue.noValue("allowed-creation-parent-tag"),
                true)
            .publish();
    metalake.addUser("allowed_manager");
    grant(
        "allowed_manager",
        List.of(
            SecurableObjects.ofMetalake(
                metalake.name(),
                List.of(Privileges.ManageDomain.allow(), Privileges.CreateTag.allow()))));

    // The domain stays owned by the admin; only the replacement tag belongs to the manager.
    metalake.createTag("manager-binding-tag", null, Collections.emptyMap());
    metalake.setOwner(
        MetadataObjects.of(null, "manager-binding-tag", MetadataObject.Type.TAG),
        "allowed_manager",
        Owner.Type.USER);
    try (GravitinoAdminClient managerClient = userClient("allowed_manager")) {
      GravitinoMetalake manager = managerClient.loadMetalake(metalake.name());
      Domain managed = manager.getDomain(child.name());
      assertEquals(
          "managed",
          manager.alterDomain(child.name(), DomainChange.updateComment("managed")).comment());
      assertEquals(Domain.Status.DRAFT, managed.unpublish().status());
      assertEquals(Domain.Status.PUBLISHED, managed.publish().status());
      assertEquals(target.name(), managed.attachToParent(target.name()).parent().orElseThrow());
      assertTrue(managed.detachFromParent().parent().isEmpty());
      assertEquals(
          TagValue.noValue("manager-binding-tag"),
          managed
              .updateBackingTag(TagValue.noValue("manager-binding-tag"))
              .backingTag()
              .orElseThrow());
      assertTrue(managed.updateBackingTag(null).backingTag().isEmpty());

      // Legal creation requests also succeed when both management and tag creation are allowed.
      Domain created =
          manager.createDomain(
              "denied-domain", null, null, TagValue.noValue("denied-create-tag"), true);
      Domain subdomain =
          manager
              .getDomain(creationParent.name())
              .createSubdomain(
                  "denied-subdomain", null, null, TagValue.noValue("denied-tag"), true);
      assertEquals(creationParent.name(), subdomain.parent().orElseThrow());
      assertEquals(created.name(), manager.getDomain(created.name()).name());
      assertTrue(manager.deleteDomain(child.name()));
      assertThrows(NoSuchDomainException.class, () -> metalake.getDomain(child.name()));
    }
  }

  private GravitinoAdminClient userClient(String user) {
    return GravitinoAdminClient.builder(serverUri).withSimpleAuth(user).build();
  }

  private void grant(String user, List<SecurableObject> objects) {
    String role = "role_" + UUID.randomUUID().toString().replace("-", "");
    metalake.createRole(role, Collections.emptyMap(), objects);
    metalake.grantRolesToUser(List.of(role), user);
  }

  private static SecurableObject scope(String name, Privilege... privileges) {
    return SecurableObjects.parse(name, MetadataObject.Type.DOMAIN, List.of(privileges));
  }
}
