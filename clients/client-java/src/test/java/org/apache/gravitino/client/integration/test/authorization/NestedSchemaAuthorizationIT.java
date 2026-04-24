/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Integration tests for nested namespace (hierarchical schema) authorization.
 *
 * <p>Tests cover:
 *
 * <ol>
 *   <li>Admin can create nested schemas; auto-creates parent chain.
 *   <li>Normal user cannot create nested schemas without grants.
 *   <li>Granting {@code create_schema} on a parent schema allows creating direct children.
 *   <li>Granting {@code create_schema} on an ancestor schema inherits down to all descendants.
 *   <li>List schemas returns only top-level schemas by default.
 *   <li>{@code use_schema} on a nested schema allows loading and listing it.
 *   <li>Drop nested schema requires ownership or catalog ownership.
 * </ol>
 */
@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NestedSchemaAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG = "nested_catalog";
  private static final String ROLE = "nested_role";

  /** Top-level schemas created by admin at setup. */
  private static final String ROOT_A = "A";

  private static final String SCHEMA_AB = "A:B";
  private static final String SCHEMA_ABC = "A:B:C";
  private static final String SCHEMA_ABD = "A:B:D";

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // Enable authorization and configure the nested namespace separator.
    customConfigs.put(Configs.SCHEMA_NAMESPACE_SEPARATOR.getKey(), ":");
    super.startIntegrationTest();

    // Create an Iceberg catalog because ':' hierarchical schema names are only supported there.
    Map<String, String> catalogProperties = new HashMap<>();
    catalogProperties.put("catalog-backend", "jdbc");
    catalogProperties.put("warehouse", "/tmp/gravitino-it-nested-schema");
    catalogProperties.put("uri", "jdbc:sqlite::memory:");
    catalogProperties.put("jdbc-driver", "org.sqlite.JDBC");
    catalogProperties.put("jdbc-initialize", "true");
    client
        .loadMetalake(METALAKE)
        .createCatalog(
            CATALOG, Catalog.Type.RELATIONAL, "lakehouse-iceberg", "comment", catalogProperties);

    // Grant the normal user a role with USE_CATALOG so it can interact with the catalog.
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    List<SecurableObject> securableObjects = new ArrayList<>();
    List<Privilege> privileges = new ArrayList<>();
    privileges.add(Privileges.UseCatalog.allow());
    securableObjects.add(SecurableObjects.ofCatalog(CATALOG, privileges));
    metalake.createRole(ROLE, new HashMap<>(), securableObjects);
    metalake.grantRolesToUser(ImmutableList.of(ROLE), NORMAL_USER);
  }

  /**
   * Admin creates a nested schema {@code A:B:C}. The server automatically ensures the parent chain
   * {@code A} and {@code A:B} exists. Verify all three schemas are accessible by the admin.
   */
  @Test
  @Order(1)
  public void testAdminCreatesNestedSchemaAutoCreatesParentChain() {
    Catalog catalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG);

    // Creating "A:B:C" should auto-create "A" and "A:B".
    catalog.asSchemas().createSchema(SCHEMA_ABC, "nested schema", new HashMap<>());

    // Default listing only returns top-level schemas.
    String[] schemas = catalog.asSchemas().listSchemas();
    List<String> schemaList = Arrays.asList(schemas);

    assertTrue(schemaList.contains(ROOT_A), "Parent 'A' should be auto-created");
    assertTrue(
        !schemaList.contains(SCHEMA_AB),
        "Default listSchemas() should not include nested schema A:B");
    assertTrue(
        !schemaList.contains(SCHEMA_ABC),
        "Default listSchemas() should not include nested schema A:B:C");

    // Verify nested schemas exist via direct load.
    assertEquals(SCHEMA_AB, catalog.asSchemas().loadSchema(SCHEMA_AB).name());
    assertEquals(SCHEMA_ABC, catalog.asSchemas().loadSchema(SCHEMA_ABC).name());
  }

  /**
   * Normal user without {@code create_schema} on the parent schema cannot create a nested schema.
   */
  @Test
  @Order(2)
  public void testNormalUserCannotCreateNestedSchemaWithoutGrant() {
    Catalog catalogByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);

    // Creating "A:B:D" requires create_schema on parent "A:B" (or any ancestor / catalog).
    assertThrows(
        ForbiddenException.class,
        () -> catalogByNormalUser.asSchemas().createSchema(SCHEMA_ABD, "test", new HashMap<>()));
  }

  /**
   * Granting {@code create_schema} on the parent schema {@code A:B} allows the normal user to
   * create a direct child {@code A:B:D}.
   */
  @Test
  @Order(3)
  public void testCreateSchemaWithGrantOnParentSchema() {
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);

    // Grant create_schema on parent "A:B".
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(CATALOG, SCHEMA_AB, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow(), Privileges.CreateSchema.allow()));

    Catalog catalogByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    // Normal user can now create a child of "A:B".
    catalogByNormalUser.asSchemas().createSchema(SCHEMA_ABD, "child of A:B", new HashMap<>());

    // Verify "A:B:D" was created.
    Catalog catalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertEquals(SCHEMA_ABD, catalog.asSchemas().loadSchema(SCHEMA_ABD).name());
  }

  /**
   * Privilege inheritance: granting {@code create_schema} on ancestor {@code A} allows creating
   * schemas anywhere in the {@code A:*} subtree via the inheritance chain.
   */
  @Test
  @Order(4)
  public void testCreateSchemaInheritedFromAncestorGrant() {
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);

    // Grant create_schema on ancestor "A". Due to inheritance, this covers "A:B:*" as well.
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(CATALOG, ROOT_A, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow(), Privileges.CreateSchema.allow()));

    Catalog catalogByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);

    // Normal user can now create "A:B:E" via inheritance from "A".
    catalogByNormalUser.asSchemas().createSchema("A:B:E", "inherited from A", new HashMap<>());

    // Verify "A:B:E" was created.
    Catalog catalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertEquals("A:B:E", catalog.asSchemas().loadSchema("A:B:E").name());
  }

  /**
   * By default {@code listSchemas()} returns only top-level schemas (those without the separator).
   * Nested schemas are accessible through the parent-aware filter.
   */
  @Test
  @Order(5)
  public void testListSchemasReturnsTopLevelByDefault() {
    Catalog catalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG);
    String[] schemas = catalog.asSchemas().listSchemas();

    // Top-level schemas should include "A" (and any others created by prior tests).
    List<String> schemaList = Arrays.asList(schemas);
    assertTrue(schemaList.contains(ROOT_A));

    // Nested schemas should NOT appear in the default listing.
    schemaList.forEach(
        name ->
            assertTrue(
                !name.contains(":"),
                "Default listSchemas() should return only top-level schemas, got: " + name));
  }

  /**
   * Granting {@code use_schema} on a nested schema allows the normal user to load it, even without
   * grants on intermediate parent schemas.
   */
  @Test
  @Order(6)
  public void testUseSchemaPrivilegeOnNestedSchema() {
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);

    // Grant use_schema specifically on "A:B:C".
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(CATALOG, SCHEMA_ABC, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.UseSchema.allow()));

    Catalog catalogByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);

    // Normal user can now load "A:B:C".
    org.apache.gravitino.Schema loaded = catalogByNormalUser.asSchemas().loadSchema(SCHEMA_ABC);
    assertEquals(SCHEMA_ABC, loaded.name());
  }

  /**
   * Dropping a nested schema requires ownership or catalog ownership. Normal user without ownership
   * cannot drop; after becoming owner, they can.
   */
  @Test
  @Order(7)
  public void testDropNestedSchemaRequiresOwnership() {
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);
    Catalog catalogByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);

    // Normal user cannot drop "A:B:C" without ownership.
    assertThrows(
        ForbiddenException.class,
        () -> catalogByNormalUser.asSchemas().dropSchema(SCHEMA_ABC, false));

    // Set normal user as owner of "A:B:C".
    metalake.setOwner(
        MetadataObjects.of(CATALOG, SCHEMA_ABC, MetadataObject.Type.SCHEMA),
        NORMAL_USER,
        Owner.Type.USER);

    // Normal user can now drop it.
    catalogByNormalUser.asSchemas().dropSchema(SCHEMA_ABC, false);

    // Verify "A:B:C" is gone; parent "A:B" should still exist.
    Catalog catalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertThrows(NoSuchSchemaException.class, () -> catalog.asSchemas().loadSchema(SCHEMA_ABC));
    assertEquals(ROOT_A, catalog.asSchemas().loadSchema(ROOT_A).name());
  }

  /**
   * Tests that {@code create_schema} cannot be bound to a SCHEMA-level securable object before the
   * canBindTo change (this is now allowed). Grants {@code CreateSchema.allow()} on a schema object
   * and verifies no exception is thrown.
   */
  @Test
  @Order(8)
  public void testCreateSchemaPrivilegeCanBindToSchema() {
    GravitinoMetalake metalake = client.loadMetalake(METALAKE);

    // This should NOT throw — CreateSchema.canBindTo(SCHEMA) is now true.
    metalake.grantPrivilegesToRole(
        ROLE,
        MetadataObjects.of(CATALOG, ROOT_A, MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.CreateSchema.allow()));

    // Verify the grant was applied by checking normal user can create under "A".
    Catalog catalogByNormalUser = normalUserClient.loadMetalake(METALAKE).loadCatalog(CATALOG);
    catalogByNormalUser.asSchemas().createSchema("A:F", "via schema-level grant", new HashMap<>());

    Catalog catalog = client.loadMetalake(METALAKE).loadCatalog(CATALOG);
    assertEquals("A:F", catalog.asSchemas().loadSchema("A:F").name());
  }
}
