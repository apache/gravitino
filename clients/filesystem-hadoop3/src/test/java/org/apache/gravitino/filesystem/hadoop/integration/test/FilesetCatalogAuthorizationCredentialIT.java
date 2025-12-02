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

package org.apache.gravitino.filesystem.hadoop.integration.test;

import static org.apache.gravitino.catalog.fileset.FilesetCatalogPropertiesMetadata.FILESYSTEM_PROVIDERS;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for Fileset Catalog Credential with Authorization enabled. Tests that only users
 * with proper privileges can retrieve credentials for filesets.
 */
@EnabledIfEnvironmentVariable(named = "GRAVITINO_TEST_CLOUD_IT", matches = "true")
public class FilesetCatalogAuthorizationCredentialIT extends BaseIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(FilesetCatalogAuthorizationCredentialIT.class);

  public static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME");
  public static final String S3_ACCESS_KEY = System.getenv("S3_ACCESS_KEY_ID");
  public static final String S3_SECRET_KEY = System.getenv("S3_SECRET_ACCESS_KEY");
  public static final String S3_ROLE_ARN = System.getenv("S3_ROLE_ARN");

  private static final String SUPER_USER = "gravitino_admin";
  private static final String NORMAL_USER = "normal_user";
  private static final String ROLE_NAME = "fileset_credential_test_role";

  private static String metalakeName = GravitinoITUtils.genRandomName("gvfs_authz_cred_metalake");
  private static String catalogName = GravitinoITUtils.genRandomName("catalog");
  private static String schemaName = GravitinoITUtils.genRandomName("schema");

  private static GravitinoMetalake metalake;
  private static GravitinoAdminClient adminClient;
  private static GravitinoAdminClient normalUserClient;

  @BeforeAll
  public void startIntegrationTest() {
    // Intentionally override to prevent parent class's startIntegrationTest() from executing.
  }

  @BeforeAll
  public void startUp() throws Exception {
    copyBundleJarsToHadoop("aws-bundle");

    // Enable authorization
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    configs.put(Configs.SERVICE_ADMINS.getKey(), SUPER_USER);
    configs.put(Configs.AUTHENTICATORS.getKey(), "simple");
    registerCustomConfigs(configs);

    // Start the server with authorization enabled
    super.startIntegrationTest();

    // Create admin client
    adminClient = GravitinoAdminClient.builder(serverUri).withSimpleAuth(SUPER_USER).build();

    // Create normal user client
    normalUserClient = GravitinoAdminClient.builder(serverUri).withSimpleAuth(NORMAL_USER).build();

    // Create metalake as admin
    metalake = adminClient.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(adminClient.metalakeExists(metalakeName));

    // Add normal user to the metalake
    metalake.addUser(NORMAL_USER);

    // Create catalog with S3 credential providers
    Map<String, String> properties = Maps.newHashMap();
    properties.put(FILESYSTEM_PROVIDERS, "s3");
    properties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS,
        S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE
            + ","
            + S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE);
    properties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3_ACCESS_KEY);
    properties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, S3_SECRET_KEY);
    properties.put(S3Properties.GRAVITINO_S3_ENDPOINT, "s3.ap-southeast-2.amazonaws.com");
    properties.put(S3Properties.GRAVITINO_S3_REGION, "ap-southeast-2");
    properties.put(S3Properties.GRAVITINO_S3_ROLE_ARN, S3_ROLE_ARN);

    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Create schema
    catalog.asSchemas().createSchema(schemaName, "schema comment", properties);
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaName));

    // Create a role for normal user with basic privileges
    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(catalogName, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schemaName, ImmutableList.of(Privileges.UseSchema.allow()));
    securableObjects.add(schemaObject);

    metalake.createRole(ROLE_NAME, new HashMap<>(), securableObjects);
    metalake.grantRolesToUser(ImmutableList.of(ROLE_NAME), NORMAL_USER);
  }

  @AfterAll
  public void tearDown() throws IOException {
    try {
      if (adminClient != null) {
        Catalog catalog = metalake.loadCatalog(catalogName);
        catalog.asSchemas().dropSchema(schemaName, true);
        metalake.dropCatalog(catalogName, true);
        adminClient.dropMetalake(metalakeName, true);
        adminClient.close();
        adminClient = null;
      }

      if (normalUserClient != null) {
        normalUserClient.close();
        normalUserClient = null;
      }

      if (client != null) {
        client.close();
        client = null;
      }
    } finally {
      try {
        closer.close();
      } catch (Exception e) {
        LOG.error("Exception in closing CloseableGroup", e);
      }
    }
  }

  protected String genStorageLocation(String fileset) {
    return String.format("s3a://%s/%s", BUCKET_NAME, fileset);
  }

  @Test
  void testGetCatalogCredentialWithPrivilege() {
    GravitinoMetalake normalUserMetalake = normalUserClient.loadMetalake(metalakeName);
    Catalog catalog = normalUserMetalake.loadCatalog(catalogName);
    catalog.supportsCredentials().getCredentials();

    metalake.revokePrivilegesFromRole(
        ROLE_NAME,
        MetadataObjects.of(null, catalogName, MetadataObject.Type.CATALOG),
        ImmutableSet.of(Privileges.UseCatalog.allow()));

    // Normal user without specific catalog privileges cannot get catalog credentials
    assertThrows(
        ForbiddenException.class,
        () -> {
          catalog.supportsCredentials().getCredentials();
        });

    metalake.grantPrivilegesToRole(
        ROLE_NAME,
        MetadataObjects.of(null, catalogName, MetadataObject.Type.CATALOG),
        ImmutableSet.of(Privileges.UseCatalog.allow()));
  }

  @Test
  void testGetFilesetCredentialWithoutPrivilege() {
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_no_priv");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    String storageLocation = genStorageLocation(filesetName);

    // Admin creates a fileset with S3 token credential
    Catalog adminCatalog = metalake.loadCatalog(catalogName);
    adminCatalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of(
                CredentialConstants.CREDENTIAL_PROVIDERS,
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE));

    // Normal user cannot get credentials without privilege
    GravitinoMetalake normalUserMetalake = normalUserClient.loadMetalake(metalakeName);
    Catalog normalUserCatalog = normalUserMetalake.loadCatalog(catalogName);

    assertThrows(
        ForbiddenException.class,
        () -> {
          Fileset fileset = normalUserCatalog.asFilesetCatalog().loadFileset(filesetIdent);
          fileset.supportsCredentials().getCredentials();
        });
  }

  @Test
  void testGetFilesetCredentialWithReadPrivilege() {
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_read_priv");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    String storageLocation = genStorageLocation(filesetName);

    // Admin creates a fileset with S3 token credential
    Catalog adminCatalog = metalake.loadCatalog(catalogName);
    adminCatalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of(
                CredentialConstants.CREDENTIAL_PROVIDERS,
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE));

    // Grant ReadFileset privilege to normal user
    metalake.grantPrivilegesToRole(
        ROLE_NAME,
        MetadataObjects.of(
            ImmutableList.of(catalogName, schemaName, filesetName), MetadataObject.Type.FILESET),
        ImmutableList.of(Privileges.ReadFileset.allow()));

    // Normal user can now get credentials with ReadFileset privilege
    GravitinoMetalake normalUserMetalake = normalUserClient.loadMetalake(metalakeName);
    Catalog normalUserCatalog = normalUserMetalake.loadCatalog(catalogName);
    Fileset fileset = normalUserCatalog.asFilesetCatalog().loadFileset(filesetIdent);
    Credential[] credentials = fileset.supportsCredentials().getCredentials();

    Assertions.assertEquals(1, credentials.length);
    Assertions.assertInstanceOf(S3TokenCredential.class, credentials[0]);

    // Cleanup
    metalake.revokePrivilegesFromRole(
        ROLE_NAME,
        MetadataObjects.of(
            ImmutableList.of(catalogName, schemaName, filesetName), MetadataObject.Type.FILESET),
        ImmutableSet.of(Privileges.ReadFileset.allow()));

    Assertions.assertThrows(
        ForbiddenException.class, () -> fileset.supportsCredentials().getCredentials());
  }

  @Test
  void testGetFilesetCredentialWithWritePrivilege() {
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_write_priv");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    String storageLocation = genStorageLocation(filesetName);

    // Admin creates a fileset with S3 token credential
    Catalog adminCatalog = metalake.loadCatalog(catalogName);
    adminCatalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of(
                CredentialConstants.CREDENTIAL_PROVIDERS,
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE));

    // Grant WriteFileset privilege to normal user
    metalake.grantPrivilegesToRole(
        ROLE_NAME,
        MetadataObjects.of(
            ImmutableList.of(catalogName, schemaName, filesetName), MetadataObject.Type.FILESET),
        ImmutableList.of(Privileges.WriteFileset.allow()));

    // Normal user can get credentials with WriteFileset privilege
    GravitinoMetalake normalUserMetalake = normalUserClient.loadMetalake(metalakeName);
    Catalog normalUserCatalog = normalUserMetalake.loadCatalog(catalogName);
    Fileset fileset = normalUserCatalog.asFilesetCatalog().loadFileset(filesetIdent);
    Credential[] credentials = fileset.supportsCredentials().getCredentials();

    Assertions.assertEquals(1, credentials.length);
    Assertions.assertInstanceOf(S3TokenCredential.class, credentials[0]);

    // Cleanup
    metalake.revokePrivilegesFromRole(
        ROLE_NAME,
        MetadataObjects.of(
            ImmutableList.of(catalogName, schemaName, filesetName), MetadataObject.Type.FILESET),
        ImmutableSet.of(Privileges.WriteFileset.allow()));

    Assertions.assertThrows(
        ForbiddenException.class, () -> fileset.supportsCredentials().getCredentials());
  }

  @Test
  void testGetFilesetCredentialAsOwner() {
    String filesetName = GravitinoITUtils.genRandomName("test_fileset_owner");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    String storageLocation = genStorageLocation(filesetName);

    // Grant CreateFileset privilege to normal user
    metalake.grantPrivilegesToRole(
        ROLE_NAME,
        MetadataObjects.of(ImmutableList.of(catalogName, schemaName), MetadataObject.Type.SCHEMA),
        ImmutableList.of(Privileges.CreateFileset.allow()));

    // Normal user creates a fileset (becomes owner)
    GravitinoMetalake normalUserMetalake = normalUserClient.loadMetalake(metalakeName);
    Catalog normalUserCatalog = normalUserMetalake.loadCatalog(catalogName);
    normalUserCatalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            ImmutableMap.of(
                CredentialConstants.CREDENTIAL_PROVIDERS,
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE));

    // Owner can get credentials without explicit privilege
    Fileset fileset = normalUserCatalog.asFilesetCatalog().loadFileset(filesetIdent);
    Credential[] credentials = fileset.supportsCredentials().getCredentials();

    Assertions.assertEquals(1, credentials.length);
    Assertions.assertInstanceOf(S3TokenCredential.class, credentials[0]);

    // Cleanup
    metalake.setOwner(
        MetadataObjects.of(
            Lists.newArrayList(catalogName, schemaName, filesetName), MetadataObject.Type.FILESET),
        SUPER_USER,
        Owner.Type.USER);

    Assertions.assertThrows(
        ForbiddenException.class, () -> fileset.supportsCredentials().getCredentials());
  }
}
