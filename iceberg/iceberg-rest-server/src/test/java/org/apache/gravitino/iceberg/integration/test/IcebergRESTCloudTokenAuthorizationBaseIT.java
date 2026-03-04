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
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Abstract base class for Iceberg REST credential vending integration tests with cloud storage
 * providers (S3, GCS, Azure, etc.).
 *
 * <p>This class contains all common test logic and helper methods for testing privilege-based
 * credential vending. Subclasses implement cloud-specific configuration and setup.
 */
@SuppressWarnings("FormatStringAnnotation")
public abstract class IcebergRESTCloudTokenAuthorizationBaseIT extends IcebergAuthorizationIT {

  protected static final String SCHEMA_NAME = "schema";

  @BeforeEach
  void revokePrivilege() {
    revokeUserRoles();
    resetMetalakeAndCatalogOwner();
    MetadataObject schemaObject =
        MetadataObjects.of(
            Arrays.asList(GRAVITINO_CATALOG_NAME, SCHEMA_NAME), MetadataObject.Type.SCHEMA);
    metalakeClientWithAllPrivilege.setOwner(schemaObject, SUPER_USER, Owner.Type.USER);
    clearTable();
    // Grant user the privilege to use the catalog and schema
    grantUseSchemaRole(SCHEMA_NAME);
    sql("USE %s;", SPARK_CATALOG_NAME);
    sql("USE %s;", SCHEMA_NAME);
  }

  @Override
  protected boolean supportsCredentialVending() {
    return true;
  }

  /**
   * Returns the cloud-specific configuration properties. Subclasses implement this to provide
   * cloud-specific settings like credentials, warehouse location, IO implementation, etc.
   *
   * @return Map of cloud storage configuration properties
   */
  @Override
  public abstract Map<String, String> getCustomProperties();

  /**
   * Downloads cloud-specific bundle JARs (e.g., iceberg-aws-bundle, iceberg-gcp-bundle). Subclasses
   * implement this to download the appropriate bundle for their cloud provider.
   *
   * @throws Exception if download fails
   */
  protected abstract void downloadCloudBundleJar() throws Exception;

  /**
   * Copies cloud-specific bundle JARs to the Iceberg REST server libs directory. Subclasses
   * implement this to copy the appropriate bundle (e.g., "aws", "gcp", "azure").
   */
  protected abstract void copyCloudBundleJar();

  /**
   * Returns the cloud provider name for table naming (e.g., "s3", "gcs", "azure"). Used to generate
   * unique table names like "test_owner_s3", "test_select_gcs".
   *
   * @return Cloud provider suffix for table names
   */
  protected abstract String getCloudProviderName();

  /**
   * Sets up cloud-specific bundle JARs by downloading and copying them. This method should be
   * called from subclass {@code startIntegrationTest()} methods after calling {@code
   * super.startIntegrationTest()}.
   *
   * <p>Skips setup if running in embedded mode.
   */
  protected void setupCloudBundles() {
    if (ITUtils.isEmbedded()) {
      return;
    }
    try {
      downloadCloudBundleJar();
    } catch (Exception e) {
      throw new RuntimeException("Failed to download cloud bundle JAR", e);
    }
    copyCloudBundleJar();
  }

  @Test
  void testIcebergOwnerCloudToken() {
    String tableName = "test_owner_" + getCloudProviderName();
    grantCreateTableRole(SCHEMA_NAME);
    sql("CREATE TABLE %s(a int, b int) PARTITIONED BY (a)", tableName);
    sql("INSERT INTO %s VALUES (1,1),(2,2)", tableName);
    List<Object[]> rows = sql("SELECT * FROM %s", tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT * FROM %s.%s.%s", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT * FROM %s.%s", SCHEMA_NAME, tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT * FROM %s.%s.%s.partitions", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT *,_file FROM %s", tableName);
    Assertions.assertEquals(2, rows.size());
  }

  @Test
  void testIcebergSelectTableCloudToken() {
    String tableName = "test_select_" + getCloudProviderName();
    createTable(SCHEMA_NAME, tableName);

    // No privileges
    Assertions.assertThrows(
        ForbiddenException.class, () -> sql("INSERT INTO %s VALUES (1,1),(2,2)", tableName));
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> sql("SELECT * FROM %s.%s.%s", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName));

    grantSelectTableRole(tableName);
    Assertions.assertThrows(
        SparkException.class, () -> sql("INSERT INTO %s VALUES (1,1),(2,2)", tableName));
    List<Object[]> rows = sql("SELECT * FROM %s", tableName);
    Assertions.assertEquals(0, rows.size());

    rows = sql("SELECT * FROM %s.%s.%s", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName);
    Assertions.assertEquals(0, rows.size());

    rows = sql("SELECT * FROM %s.%s", SCHEMA_NAME, tableName);
    Assertions.assertEquals(0, rows.size());

    rows = sql("SELECT * FROM %s.%s.%s.partitions", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName);
    Assertions.assertEquals(0, rows.size());

    rows = sql("SELECT *,_file FROM %s", tableName);
    Assertions.assertEquals(0, rows.size());
  }

  @Test
  void testIcebergModifyTableCloudToken() {
    String tableName = "test_modify_" + getCloudProviderName();
    createTable(SCHEMA_NAME, tableName);

    // No privileges
    Assertions.assertThrows(
        ForbiddenException.class, () -> sql("INSERT INTO %s VALUES (1,1),(2,2)", tableName));
    Assertions.assertThrows(
        ForbiddenException.class,
        () -> sql("SELECT * FROM %s.%s.%s", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName));

    grantModifyTableRole(tableName);
    sql("INSERT INTO %s VALUES (1,1),(2,2)", tableName);
    List<Object[]> rows = sql("SELECT * FROM %s", tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT * FROM %s.%s.%s", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT * FROM %s.%s", SCHEMA_NAME, tableName);
    Assertions.assertEquals(2, rows.size());

    rows = sql("SELECT * FROM %s.%s.%s.partitions", SPARK_CATALOG_NAME, SCHEMA_NAME, tableName);
    Assertions.assertEquals(1, rows.size());

    rows = sql("SELECT *,_file FROM %s", tableName);
    Assertions.assertEquals(2, rows.size());
  }

  protected void grantUseSchemaRole(String schema) {
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

  protected String grantCreateTableRole(String schema) {
    String roleName = "createTable_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schema, ImmutableList.of(Privileges.CreateTable.allow()));
    securableObjects.add(schemaObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  protected String grantSelectTableRole(String tableName) {
    String roleName = "selectTable_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, SCHEMA_NAME, ImmutableList.of(Privileges.UseSchema.allow()));
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, tableName, ImmutableList.of(Privileges.SelectTable.allow()));
    securableObjects.add(tableObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  protected String grantModifyTableRole(String tableName) {
    String roleName = "modifyTable_" + UUID.randomUUID();
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(
            GRAVITINO_CATALOG_NAME, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, SCHEMA_NAME, ImmutableList.of(Privileges.UseSchema.allow()));
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, tableName, ImmutableList.of(Privileges.ModifyTable.allow()));
    securableObjects.add(tableObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  protected void clearTable() {
    Arrays.stream(
            catalogClientWithAllPrivilege.asTableCatalog().listTables(Namespace.of(SCHEMA_NAME)))
        .forEach(
            table -> {
              catalogClientWithAllPrivilege
                  .asTableCatalog()
                  .dropTable(NameIdentifier.of(SCHEMA_NAME, table.name()));
            });
    NameIdentifier[] nameIdentifiers =
        catalogClientWithAllPrivilege.asTableCatalog().listTables(Namespace.of(SCHEMA_NAME));
    Assertions.assertEquals(0, nameIdentifiers.length);
  }
}
