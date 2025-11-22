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
import java.io.IOException;
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
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.DownloaderUtils;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.commons.util.StringUtils;

@SuppressWarnings("FormatStringAnnotation")
@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class IcebergRESTS3TokenAuthorizationIT extends IcebergAuthorizationIT {

  private static final String SCHEMA_NAME = "schema";
  private String s3Warehouse;
  private String accessKey;
  private String secretKey;
  private String region;
  private String roleArn;
  private String externalId;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    this.s3Warehouse =
        String.format(
            "s3://%s/test1", System.getenv().getOrDefault("GRAVITINO_S3_BUCKET", "{BUCKET_NAME}"));
    this.accessKey = System.getenv().getOrDefault("GRAVITINO_S3_ACCESS_KEY", "{ACCESS_KEY}");
    this.secretKey = System.getenv().getOrDefault("GRAVITINO_S3_SECRET_KEY", "{SECRET_KEY}");
    this.region = System.getenv().getOrDefault("GRAVITINO_S3_REGION", "ap-southeast-2");
    this.roleArn = System.getenv().getOrDefault("GRAVITINO_S3_ROLE_ARN", "{ROLE_ARN}");
    this.externalId = System.getenv().getOrDefault("GRAVITINO_S3_EXTERNAL_ID", "");

    super.startIntegrationTest();

    catalogClientWithAllPrivilege.asSchemas().createSchema(SCHEMA_NAME, "test", new HashMap<>());

    if (ITUtils.isEmbedded()) {
      return;
    }
    try {
      downloadIcebergAwsBundleJar();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    copyS3BundleJar();
  }

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
  public Map<String, String> getCustomProperties() {
    HashMap<String, String> m = new HashMap<>();
    m.putAll(getS3Config());
    return m;
  }

  @Override
  protected boolean supportsCredentialVending() {
    return true;
  }

  @Test
  void testIcebergOwnerS3Token() {
    String tableName = "test_owner_s3";
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
  void testIcebergSelectTableS3Token() {
    String tableName = "test_select_s3";
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
  void testIcebergModifyTableS3Token() {
    String tableName = "test_modify_s3";
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

  private String grantCreateTableRole(String schema) {
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
    SecurableObject tableObject =
        SecurableObjects.ofTable(
            schemaObject, tableName, ImmutableList.of(Privileges.SelectTable.allow()));
    securableObjects.add(tableObject);
    metalakeClientWithAllPrivilege.createRole(roleName, new HashMap<>(), securableObjects);
    metalakeClientWithAllPrivilege.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);
    return roleName;
  }

  private String grantModifyTableRole(String tableName) {
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

  private void clearTable() {
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

  private void downloadIcebergAwsBundleJar() throws IOException {
    String icebergBundleJarUri =
        String.format(
            "https://repo1.maven.org/maven2/org/apache/iceberg/"
                + "iceberg-aws-bundle/%s/iceberg-aws-bundle-%s.jar",
            ITUtils.icebergVersion(), ITUtils.icebergVersion());
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    DownloaderUtils.downloadFile(icebergBundleJarUri, targetDir);
  }

  private void copyS3BundleJar() {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String targetDir = String.format("%s/iceberg-rest-server/libs/", gravitinoHome);
    BaseIT.copyBundleJarsToDirectory("aws", targetDir);
  }

  private Map<String, String> getS3Config() {
    Map configMap = new HashMap<String, String>();

    configMap.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE);
    configMap.put(S3Properties.GRAVITINO_S3_REGION, region);
    configMap.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, accessKey);
    configMap.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, secretKey);
    configMap.put(S3Properties.GRAVITINO_S3_ROLE_ARN, roleArn);
    if (StringUtils.isNotBlank(externalId)) {
      configMap.put(S3Properties.GRAVITINO_S3_EXTERNAL_ID, externalId);
    }

    configMap.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    configMap.put(IcebergConstants.WAREHOUSE, s3Warehouse);

    return configMap;
  }
}
