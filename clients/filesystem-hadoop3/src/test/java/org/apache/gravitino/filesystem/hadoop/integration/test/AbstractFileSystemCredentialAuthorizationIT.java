/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for GVFS credential vending authorization tests across cloud providers. Subclasses
 * supply provider-specific configuration and environment guards.
 */
abstract class AbstractFileSystemCredentialAuthorizationIT extends BaseIT {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractFileSystemCredentialAuthorizationIT.class);

  protected static final String SUPER_USER = "gravitino_admin";
  protected static final String NORMAL_USER = "normal_user";

  protected GravitinoMetalake metalake;
  protected GravitinoAdminClient adminClient;
  protected GravitinoAdminClient normalUserClient;
  protected Configuration gvfsConf;

  protected String metalakeName;
  protected String catalogName;
  protected String schemaName;
  protected String roleName;

  // Public lifecycle
  @BeforeAll
  public void startIntegrationTest() {
    // Prevent BaseIT from starting before we inject auth configs.
  }

  @BeforeAll
  public void startUp() throws Exception {
    copyBundleJarsToHadoop(providerBundleName());

    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    configs.put(Configs.SERVICE_ADMINS.getKey(), SUPER_USER);
    configs.put(Configs.AUTHENTICATORS.getKey(), "simple");
    registerCustomConfigs(configs);

    super.startIntegrationTest();

    adminClient = GravitinoAdminClient.builder(serverUri).withSimpleAuth(SUPER_USER).build();
    normalUserClient = GravitinoAdminClient.builder(serverUri).withSimpleAuth(NORMAL_USER).build();

    metalakeName = GravitinoITUtils.genRandomName("gvfs_cloud_auth_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");
    roleName = providerRoleName();

    metalake = adminClient.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(adminClient.metalakeExists(metalakeName));
    metalake.addUser(NORMAL_USER);

    Map<String, String> properties = new HashMap<>(catalogBaseProperties());
    properties.put(FILESYSTEM_PROVIDERS, providerName());
    properties.put(CredentialConstants.CREDENTIAL_PROVIDERS, credentialProviderType());
    metalake.createCatalog(
        catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    metalake
        .loadCatalog(catalogName)
        .asSchemas()
        .createSchema(schemaName, "schema comment", properties);
    Assertions.assertTrue(metalake.loadCatalog(catalogName).asSchemas().schemaExists(schemaName));

    List<SecurableObject> securableObjects = new ArrayList<>();
    SecurableObject catalogObject =
        SecurableObjects.ofCatalog(catalogName, ImmutableList.of(Privileges.UseCatalog.allow()));
    securableObjects.add(catalogObject);
    SecurableObject schemaObject =
        SecurableObjects.ofSchema(
            catalogObject, schemaName, ImmutableList.of(Privileges.UseSchema.allow()));
    securableObjects.add(schemaObject);
    metalake.createRole(roleName, new HashMap<>(), securableObjects);
    metalake.grantRolesToUser(ImmutableList.of(roleName), NORMAL_USER);

    gvfsConf = new Configuration();
    gvfsConf.set(
        "fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    gvfsConf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
    gvfsConf.set("fs.gvfs.impl.disable.cache", "true");
    gvfsConf.set("fs.gravitino.server.uri", serverUri);
    gvfsConf.set("fs.gravitino.client.metalake", metalakeName);
    gvfsConf.set("fs.gravitino.enableCredentialVending", "true");
  }

  @AfterAll
  public void tearDown() throws IOException {
    try {
      if (metalake != null) {
        Catalog catalog = metalake.loadCatalog(catalogName);
        catalog.asSchemas().dropSchema(schemaName, true);
        metalake.dropCatalog(catalogName, true);
      }
      if (adminClient != null) {
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

  @Test
  void testCredentialVendingWithReadPrivilegeAllowsReadOnly() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName(providerPrefix() + "_auth_read");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    String storageLocation = genStorageLocation(filesetName);

    Catalog catalog = metalake.loadCatalog(catalogName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            filesetProperties());

    seedFile(storageLocation, "seed.txt");

    metalake.grantPrivilegesToRole(
        roleName,
        MetadataObjects.of(
            ImmutableList.of(catalogName, schemaName, filesetName), MetadataObject.Type.FILESET),
        ImmutableList.of(Privileges.ReadFileset.allow()));

    String originalUser = System.getProperty("user.name");
    try {
      System.setProperty("user.name", NORMAL_USER);
      Path gvfsPath = genGvfsPath(filesetName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(new Configuration(gvfsConf))) {
        Assertions.assertTrue(gvfs.listStatus(gvfsPath).length >= 1);
        try (FSDataInputStream in = gvfs.open(new Path(gvfsPath, "seed.txt"))) {
          Assertions.assertEquals('s', in.read());
        }
        Path denyWrite = new Path(gvfsPath, "write-denied.txt");
        IOException ioe =
            Assertions.assertThrows(IOException.class, () -> gvfs.create(denyWrite, true).close());
        String msg = ioe.getMessage() == null ? "" : ioe.getMessage().toLowerCase();
        Assertions.assertTrue(
            msg.contains("forbidden") || msg.contains("accessdenied") || msg.contains("permission"),
            "Expected auth failure (forbidden/access denied) due to missing WRITE_FILESET privilege, but got: "
                + ioe.getMessage());
      }
    } finally {
      revokeRolePrivilege(filesetIdent, Privileges.ReadFileset.allow());
      if (originalUser != null) {
        System.setProperty("user.name", originalUser);
      }
      metalake.loadCatalog(catalogName).asFilesetCatalog().dropFileset(filesetIdent);
    }
  }

  @Test
  void testCredentialVendingWithWritePrivilegeAllowsReadAndWrite() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName(providerPrefix() + "_auth_write");
    NameIdentifier filesetIdent = NameIdentifier.of(schemaName, filesetName);
    String storageLocation = genStorageLocation(filesetName);

    Catalog catalog = metalake.loadCatalog(catalogName);
    catalog
        .asFilesetCatalog()
        .createFileset(
            filesetIdent,
            "fileset comment",
            Fileset.Type.MANAGED,
            storageLocation,
            filesetProperties());

    metalake.grantPrivilegesToRole(
        roleName,
        MetadataObjects.of(
            ImmutableList.of(catalogName, schemaName, filesetName), MetadataObject.Type.FILESET),
        ImmutableList.of(Privileges.WriteFileset.allow()));

    String originalUser = System.getProperty("user.name");
    try {
      System.setProperty("user.name", NORMAL_USER);
      Path gvfsPath = genGvfsPath(filesetName);
      try (FileSystem gvfs = gvfsPath.getFileSystem(new Configuration(gvfsConf))) {
        Path writable = new Path(gvfsPath, "write-allowed.txt");
        try (FSDataOutputStream out = gvfs.create(writable, true)) {
          out.write("ok".getBytes(StandardCharsets.UTF_8));
        }
        Assertions.assertTrue(gvfs.exists(writable));
        try (FSDataInputStream in = gvfs.open(writable)) {
          Assertions.assertEquals('o', in.read());
        }
      }
    } finally {
      revokeRolePrivilege(filesetIdent, Privileges.WriteFileset.allow());
      if (originalUser != null) {
        System.setProperty("user.name", originalUser);
      }
      metalake.loadCatalog(catalogName).asFilesetCatalog().dropFileset(filesetIdent);
    }
  }

  protected void seedFile(String storageLocation, String fileName) throws IOException {
    Path realPath = new Path(storageLocation);
    try (FileSystem realFs =
        realPath.getFileSystem(
            convertGvfsConfigToRealFileSystemConfig(new Configuration(gvfsConf)))) {
      realFs.mkdirs(realPath);
      try (FSDataOutputStream out = realFs.create(new Path(realPath, fileName), true)) {
        out.write("seed".getBytes(StandardCharsets.UTF_8));
      }
    }
  }

  protected void revokeRolePrivilege(NameIdentifier filesetIdent, Privilege privilege) {
    metalake.revokePrivilegesFromRole(
        roleName,
        MetadataObjects.of(
            ImmutableList.of(catalogName, schemaName, filesetIdent.name()),
            MetadataObject.Type.FILESET),
        ImmutableSet.of(privilege));
  }

  protected Map<String, String> filesetProperties() {
    return ImmutableMap.of(CredentialConstants.CREDENTIAL_PROVIDERS, credentialProviderType());
  }

  protected abstract String providerName();

  protected abstract String providerBundleName();

  protected abstract String credentialProviderType();

  protected abstract Map<String, String> catalogBaseProperties();

  protected abstract String genStorageLocation(String fileset);

  protected abstract Path genGvfsPath(String fileset);

  protected abstract Configuration convertGvfsConfigToRealFileSystemConfig(Configuration gvfsConf);

  protected abstract String providerPrefix();

  protected abstract String providerRoleName();
}
