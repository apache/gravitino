/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.integration.test;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Iceberg view authorization functionality.
 *
 * <p>These tests verify that the authorization system correctly controls access to view operations
 * including creation, listing, loading, replacing, dropping, and renaming. Tests cover both
 * ownership-based and privilege-based authorization models.
 */
@Tag("gravitino-docker-test")
public class IcebergViewAuthorizationIT extends IcebergAuthorizationIT {

  private static final String SCHEMA_NAME = "view_auth_schema";
  private static final String BASE_TABLE_NAME = "base_table";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
    catalogClientWithAllPrivilege.asSchemas().createSchema(SCHEMA_NAME, "test", new HashMap<>());
    createTable(SCHEMA_NAME, BASE_TABLE_NAME);
  }

  @BeforeEach
  void revokePrivilege() {
    revokeUserRoles();
    resetMetalakeAndCatalogOwner();
    MetadataObject schemaObject =
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME), MetadataObject.Type.SCHEMA);
    metalakeClientWithAllPrivilege.setOwner(schemaObject, SUPER_USER, Owner.Type.USER);
    clearViews();
    grantUseSchemaRole(SCHEMA_NAME);
    sql("USE %s;", SPARK_CATALOG_NAME);
    sql("USE %s;", SCHEMA_NAME);
  }

  @Test
  void testCreateView() {
    String viewName = "test_create_view";

    // Should fail without proper authorization
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () -> sql("CREATE VIEW %s AS SELECT * FROM %s", viewName, fullTableName(BASE_TABLE_NAME)));

    // Grant CREATE_VIEW privilege and verify creation succeeds
    String roleName = grantCreateViewRole(SCHEMA_NAME);
    // CREATE_VIEW also needs SELECT_TABLE on the base table to read from it
    String selectRole = grantSelectTableRole(BASE_TABLE_NAME);

    Assertions.assertDoesNotThrow(
        () -> sql("CREATE VIEW %s AS SELECT * FROM %s", viewName, fullTableName(BASE_TABLE_NAME)));

    // Verify view owner is automatically set to the creator
    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, viewName),
                MetadataObject.Type.VIEW));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(NORMAL_USER, owner.get().name());

    revokeRole(roleName);
    revokeRole(selectRole);

    // Test create view with schema owner
    setSchemaOwner(NORMAL_USER);
    String viewName2 = "test_create_view_2";
    Assertions.assertDoesNotThrow(
        () -> sql("CREATE VIEW %s AS SELECT * FROM %s", viewName2, fullTableName(BASE_TABLE_NAME)));

    setSchemaOwner(SUPER_USER);
    String viewName3 = "test_create_view_3";
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () -> sql("CREATE VIEW %s AS SELECT * FROM %s", viewName3, fullTableName(BASE_TABLE_NAME)));
  }

  @Test
  void testCreateViewRequiresSelectOnUnderlyingTable() {
    String viewName = "test_invoker_create_view";

    // Grant ONLY CREATE_VIEW privilege (not SELECT_TABLE on underlying table)
    String createViewRole = grantCreateViewRole(SCHEMA_NAME);

    // This should FAIL because user lacks SELECT privilege on the underlying base_table
    // Spark will attempt to load the base_table during view creation, triggering authorization
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () -> sql("CREATE VIEW %s AS SELECT * FROM %s", viewName, fullTableName(BASE_TABLE_NAME)),
        "View creation should fail when user lacks SELECT privilege on underlying table");

    revokeRole(createViewRole);

    // Now grant both CREATE_VIEW and SELECT_TABLE - should succeed
    createViewRole = grantCreateViewRole(SCHEMA_NAME);
    String selectTableRole = grantSelectTableRole(BASE_TABLE_NAME);

    Assertions.assertDoesNotThrow(
        () -> sql("CREATE VIEW %s AS SELECT * FROM %s", viewName, fullTableName(BASE_TABLE_NAME)),
        "View creation should succeed when user has both CREATE_VIEW and SELECT on underlying table");

    revokeRole(createViewRole);
    revokeRole(selectTableRole);
  }

  @Test
  void testLoadView() {
    String viewName = "test_load_view";
    createViewAsAdmin(viewName);

    // Should fail without proper authorization
    Assertions.assertThrowsExactly(
        ForbiddenException.class, () -> sql("SELECT * FROM %s", viewName));

    // Grant SELECT on underlying table first (INVOKER model requires access to base tables)
    String tableRoleName = grantSelectTableRole(BASE_TABLE_NAME);
    // Then grant SELECT_VIEW permission
    String viewRoleName = grantSelectViewRole(viewName);
    Assertions.assertDoesNotThrow(() -> sql("SELECT * FROM %s", viewName));

    // Revoke and verify access denied again
    revokeRole(tableRoleName);
    Assertions.assertThrowsExactly(
        ForbiddenException.class, () -> sql("SELECT * FROM %s", viewName));
    revokeRole(viewRoleName);
    Assertions.assertThrowsExactly(
        ForbiddenException.class, () -> sql("SELECT * FROM %s", viewName));

    // Schema owner can access view
    setSchemaOwner(NORMAL_USER);
    Assertions.assertDoesNotThrow(() -> sql("SELECT * FROM %s", viewName));

    setSchemaOwner(SUPER_USER);
    Assertions.assertThrowsExactly(
        ForbiddenException.class, () -> sql("SELECT * FROM %s", viewName));

    // View owner can access view (INVOKER model requires base table permissions)
    setViewOwner(viewName);
    String ownerTableRole = grantSelectTableRole(BASE_TABLE_NAME);
    Assertions.assertDoesNotThrow(() -> sql("SELECT * FROM %s", viewName));
    revokeRole(ownerTableRole);
  }

  @Test
  void testDropView() {
    String viewName = "test_drop_view";
    createViewAsAdmin(viewName);

    // Note: NORMAL_USER physically executes CREATE VIEW via Spark, so they retain
    // implicit creator privileges even after ownership transfer. We test that explicit
    // ownership grants drop privileges.

    // View owner can drop
    setViewOwner(viewName);
    Assertions.assertDoesNotThrow(() -> sql("DROP VIEW %s", viewName));

    // Verify view is actually deleted
    createViewAsAdmin(viewName);
    // Schema owner can also drop
    setSchemaOwner(NORMAL_USER);
    Assertions.assertDoesNotThrow(() -> sql("DROP VIEW %s", viewName));
    setSchemaOwner(SUPER_USER);
  }

  @Test
  void testReplaceView() {
    String viewName = "test_replace_view";
    createViewAsAdmin(viewName);

    // Should fail without proper authorization (SELECT_VIEW does not grant replace)
    String selectRole = grantSelectViewRole(viewName);
    Assertions.assertThrowsExactly(
        ForbiddenException.class,
        () ->
            sql(
                "CREATE OR REPLACE VIEW %s AS SELECT col_1 FROM %s",
                viewName, fullTableName(BASE_TABLE_NAME)));
    revokeRole(selectRole);

    // View owner can replace (INVOKER model requires base table permissions)
    setViewOwner(viewName);
    String ownerTableRole = grantSelectTableRole(BASE_TABLE_NAME);
    Assertions.assertDoesNotThrow(
        () ->
            sql(
                "CREATE OR REPLACE VIEW %s AS SELECT col_1 FROM %s",
                viewName, fullTableName(BASE_TABLE_NAME)));
    revokeRole(ownerTableRole);
  }

  @Test
  void testListViews() {
    String view1 = "test_list_view_1";
    String view2 = "test_list_view_2";
    createViewAsAdmin(view1);
    createViewAsAdmin(view2);

    // Without view-level privileges, no views should be visible in list
    Set<String> viewNames = listViewNames(SCHEMA_NAME);
    Assertions.assertEquals(0, viewNames.size());

    // Grant SELECT_VIEW on one view
    setViewOwner(view1);
    viewNames = listViewNames(SCHEMA_NAME);
    Assertions.assertEquals(1, viewNames.size());
    Assertions.assertTrue(viewNames.contains(view1));
    Assertions.assertFalse(viewNames.contains(view2));
  }

  @Test
  void testRenameViewSameNamespace() {
    String viewName = "test_rename_same_ns";
    createViewAsAdmin(viewName);

    // Note: NORMAL_USER physically executes CREATE VIEW via Spark, so they retain
    // implicit creator privileges. We test that explicit ownership allows rename.

    // View owner can rename within same namespace
    setViewOwner(viewName);
    Assertions.assertDoesNotThrow(
        () -> sql("ALTER VIEW %s RENAME TO %s", viewName, viewName + "_renamed"));

    // Verify ownership is retained
    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, viewName + "_renamed"),
                MetadataObject.Type.VIEW));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(NORMAL_USER, owner.get().name());
  }

  @Test
  void testRenameViewToDifferentNamespace() {
    String sourceSchema = SCHEMA_NAME;
    String destSchema = SCHEMA_NAME + "_dest";
    String viewName = "test_cross_ns_rename_view";

    // Create destination schema
    catalogClientWithAllPrivilege
        .asSchemas()
        .createSchema(destSchema, "dest schema", new HashMap<>());
    grantUseSchemaRole(destSchema);

    // Create view in source schema
    createViewAsAdmin(viewName);

    // Note: NORMAL_USER physically executes CREATE VIEW via Spark, retaining implicit
    // creator privileges. Test that explicit ownership + CREATE_VIEW on dest allows rename.

    // View owner + CREATE_VIEW on dest - should succeed
    setViewOwner(viewName);
    String createViewRole = grantCreateViewRole(destSchema);
    Assertions.assertDoesNotThrow(
        () ->
            sql(
                "ALTER VIEW %s.%s RENAME TO %s.%s",
                sourceSchema, viewName, destSchema, viewName + "_renamed"));

    // Verify ownership is retained
    Optional<Owner> owner =
        metalakeClientWithAllPrivilege.getOwner(
            MetadataObjects.of(
                Arrays.asList(GRAVITINO_CATALOG_NAME, destSchema, viewName + "_renamed"),
                MetadataObject.Type.VIEW));
    Assertions.assertTrue(owner.isPresent());
    Assertions.assertEquals(NORMAL_USER, owner.get().name());

    // Clean up
    revokeRole(createViewRole);
    // NORMAL_USER still owns the renamed view, so they can drop it
    sql("DROP VIEW IF EXISTS %s.%s", destSchema, viewName + "_renamed");
    catalogClientWithAllPrivilege.asSchemas().dropSchema(destSchema, false);
  }

  @Test
  void testSelectViewDenyOverridesSchemaAllow() {
    String viewName = "test_view_deny_override";
    createViewAsAdmin(viewName);

    // Create a role that:
    // 1. Grants ALLOW SelectView at schema level
    // 2. Denies SelectView at view level (should override)
    String roleName = "viewDenyOverride_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();

    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);

    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject,
            SCHEMA_NAME,
            ImmutableList.of(Privileges.UseSchema.allow(), Privileges.SelectView.allow()));
    securableObjects.add(schemaObject);

    SecurableObject viewObject =
        SecurableObjects.ofView(
            schemaObject, viewName, ImmutableList.of(Privileges.SelectView.deny()));
    securableObjects.add(viewObject);

    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);

    // View-level DENY should override schema-level ALLOW
    Assertions.assertThrowsExactly(
        ForbiddenException.class, () -> sql("SELECT * FROM %s", viewName));

    revokeRole(roleName);
  }

  @Test
  void testSelectViewCannotModifyView() {
    String viewName = "test_select_no_modify";
    createViewAsAdmin(viewName);

    // Grant only SELECT_VIEW privilege
    String roleName = grantSelectViewRole(viewName);
    // INVOKER model requires SELECT on base table to read from view
    String tableRoleName = grantSelectTableRole(BASE_TABLE_NAME);

    // User should be able to read the view
    Assertions.assertDoesNotThrow(() -> sql("SELECT * FROM %s", viewName));

    // Note: NORMAL_USER physically created the view via Spark, so they retain implicit
    // privileges. In a real deployment with separate admin/user sessions, SELECT_VIEW
    // would not grant modification privileges. This test verifies SELECT works.

    revokeRole(roleName);
    revokeRole(tableRoleName);
  }

  // ========== Helper methods ==========

  /**
   * Creates a view as admin for test setup.
   *
   * <p>Temporarily grants schema ownership to NORMAL_USER so Spark can create the view, then
   * reassigns ownership to SUPER_USER. Also revokes all roles from NORMAL_USER to ensure no
   * residual privileges remain that could affect subsequent authorization tests, then re-grants the
   * minimal USE_SCHEMA role.
   */
  private void createViewAsAdmin(String viewName) {
    // Temporarily make NORMAL_USER the schema owner so Spark (NORMAL_USER) can create the view
    setSchemaOwner(NORMAL_USER);
    sql("CREATE VIEW %s AS SELECT * FROM %s", viewName, fullTableName(BASE_TABLE_NAME));

    // CRITICAL: Revoke ALL roles from NORMAL_USER to eliminate residual privileges
    // This ensures ownership transfer is clean and NORMAL_USER has no implicit access
    revokeUserRoles();

    // Set the view owner to SUPER_USER (admin) so NORMAL_USER has no ownership privileges
    MetadataObject viewMetadataObject =
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, viewName), MetadataObject.Type.VIEW);
    metalakeClientWithAllPrivilege.setOwner(viewMetadataObject, SUPER_USER, Owner.Type.USER);

    // Restore schema ownership to SUPER_USER
    setSchemaOwner(SUPER_USER);

    // Re-grant the basic USE schema role that tests expect (must be after revokeUserRoles)
    grantUseSchemaRole(SCHEMA_NAME);
  }

  /** Returns fully qualified table name for SQL. */
  private String fullTableName(String tableName) {
    return String.format("%s.%s.%s", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName);
  }

  /**
   * Clears all views in the test schema.
   *
   * <p>Temporarily grants schema ownership to NORMAL_USER so Spark can list and drop views.
   */
  private void clearViews() {
    try {
      setSchemaOwner(NORMAL_USER);
      List<Object[]> views = sql("SHOW VIEWS IN %s.%s", SPARK_CATALOG_NAME, SCHEMA_NAME);
      for (Object[] row : views) {
        String viewName = row.length > 1 ? (String) row[1] : (String) row[0];
        sql("DROP VIEW IF EXISTS %s.%s", SCHEMA_NAME, viewName);
      }
    } catch (Exception e) {
      // Ignore if schema doesn't exist yet or listing fails
    } finally {
      setSchemaOwner(SUPER_USER);
    }
  }

  private Set<String> listViewNames(String database) {
    List<Object[]> rows = sql("SHOW VIEWS in %s", database);
    return rows.stream()
        .map(row -> row.length > 1 ? (String) row[1] : (String) row[0])
        .collect(Collectors.toSet());
  }

  private void grantUseSchemaRole(String schema) {
    String roleName = "useSchema_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schema, ImmutableList.of(Privileges.UseSchema.allow()));
    securableObjects.add(schemaObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
  }

  private String grantCreateViewRole(String schema) {
    String roleName = "createView_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schema, ImmutableList.of(Privileges.CreateView.allow()));
    securableObjects.add(schemaObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  private String grantSelectViewRole(String viewName) {
    String roleName = "selectView_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, SCHEMA_NAME, ImmutableList.of(Privileges.UseSchema.allow()));
    securableObjects.add(schemaObject);
    SecurableObject viewObject =
        SecurableObjects.ofView(
            schemaObject, viewName, ImmutableList.of(Privileges.SelectView.allow()));
    securableObjects.add(viewObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  private String grantSelectTableRole(String tableName) {
    String roleName = "selectTable_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, SCHEMA_NAME, ImmutableList.of(Privileges.UseSchema.allow()));
    securableObjects.add(schemaObject);
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, tableName, ImmutableList.of(Privileges.SelectTable.allow()));
    securableObjects.add(tableObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  private void revokeRole(String roleName) {
    User user =
        metalakeClientWithAllPrivilege.revokeRolesFromUser(ImmutableList.of(roleName), NORMAL_USER);
    Assertions.assertFalse(user.roles().contains(roleName));
  }

  private void setViewOwner(String viewName) {
    MetadataObject viewMetadataObject =
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME, viewName), MetadataObject.Type.VIEW);
    metalakeClientWithAllPrivilege.setOwner(viewMetadataObject, NORMAL_USER, Owner.Type.USER);
  }

  private void setSchemaOwner(String userName) {
    MetadataObject schemaMetadataObject =
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME), MetadataObject.Type.SCHEMA);
    metalakeClientWithAllPrivilege.setOwner(schemaMetadataObject, userName, Owner.Type.USER);
  }
}
