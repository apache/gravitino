/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
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
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.hadoop.fs.FileSystemUtils;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.s3.fs.S3FileSystemProvider;
import org.apache.gravitino.storage.S3Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GVFS integration test that verifies credential vending with authorization enabled on S3. -
 * READ_FILESET: can read via GVFS but cannot write. - WRITE_FILESET: can read and write via GVFS.
 */
@EnabledIf(value = "s3IsConfigured", disabledReason = "s3 with credential is not prepared")
public class GravitinoVirtualFileSystemS3CredentialAuthorizationIT extends BaseIT {
  private static final Logger LOG =
      LoggerFactory.getLogger(GravitinoVirtualFileSystemS3CredentialAuthorizationIT.class);

  public static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME_FOR_CREDENTIAL");
  public static final String S3_ACCESS_KEY = System.getenv("S3_ACCESS_KEY_ID_FOR_CREDENTIAL");
  public static final String S3_SECRET_KEY = System.getenv("S3_SECRET_ACCESS_KEY_FOR_CREDENTIAL");
  public static final String S3_ENDPOINT = System.getenv("S3_ENDPOINT_FOR_CREDENTIAL");
  public static final String S3_REGION = System.getenv("S3_REGION_FOR_CREDENTIAL");
  public static final String S3_ROLE_ARN = System.getenv("S3_ROLE_ARN_FOR_CREDENTIAL");

  private static final String SUPER_USER = "gravitino_admin";
  private static final String NORMAL_USER = "normal_user";
  private static final String ROLE_NAME = "gvfs_s3_credential_auth_role";

  private GravitinoMetalake metalake;
  private GravitinoAdminClient adminClient;
  private GravitinoAdminClient normalUserClient;
  private Configuration gvfsConf;

  private String metalakeName;
  private String catalogName;
  private String schemaName;

  @BeforeAll
  public void startIntegrationTest() {
    // Prevent BaseIT from starting before we inject auth configs.
  }

  @BeforeAll
  public void startUp() throws Exception {
    copyBundleJarsToHadoop("aws-bundle");

    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    configs.put(Configs.SERVICE_ADMINS.getKey(), SUPER_USER);
    configs.put(Configs.AUTHENTICATORS.getKey(), "simple");
    registerCustomConfigs(configs);

    super.startIntegrationTest();

    adminClient = GravitinoAdminClient.builder(serverUri).withSimpleAuth(SUPER_USER).build();
    normalUserClient = GravitinoAdminClient.builder(serverUri).withSimpleAuth(NORMAL_USER).build();

    metalakeName = GravitinoITUtils.genRandomName("gvfs_auth_metalake");
    catalogName = GravitinoITUtils.genRandomName("catalog");
    schemaName = GravitinoITUtils.genRandomName("schema");

    metalake = adminClient.createMetalake(metalakeName, "metalake comment", Collections.emptyMap());
    Assertions.assertTrue(adminClient.metalakeExists(metalakeName));
    metalake.addUser(NORMAL_USER);

    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3_ACCESS_KEY);
    properties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, S3_SECRET_KEY);
    properties.put(S3Properties.GRAVITINO_S3_ENDPOINT, S3_ENDPOINT);
    properties.put(
        "gravitino.bypass.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
    properties.put(FILESYSTEM_PROVIDERS, "s3");
    properties.put(S3Properties.GRAVITINO_S3_REGION, S3_REGION);
    properties.put(S3Properties.GRAVITINO_S3_ROLE_ARN, S3_ROLE_ARN);
    properties.put(
        CredentialConstants.CREDENTIAL_PROVIDERS, S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE);

    Catalog catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "catalog comment", properties);
    Assertions.assertTrue(metalake.catalogExists(catalogName));
    catalog.asSchemas().createSchema(schemaName, "schema comment", properties);
    Assertions.assertTrue(catalog.asSchemas().schemaExists(schemaName));

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

    gvfsConf = new Configuration();
    gvfsConf.set(
        "fs.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.GravitinoVirtualFileSystem");
    gvfsConf.set("fs.AbstractFileSystem.gvfs.impl", "org.apache.gravitino.filesystem.hadoop.Gvfs");
    gvfsConf.set("fs.gvfs.impl.disable.cache", "true");
    gvfsConf.set("fs.gravitino.server.uri", serverUri);
    gvfsConf.set("fs.gravitino.client.metalake", metalakeName);
    gvfsConf.set("fs.gravitino.enableCredentialVending", "true");
    gvfsConf.set(S3Properties.GRAVITINO_S3_ENDPOINT, S3_ENDPOINT);
    gvfsConf.set(S3Properties.GRAVITINO_S3_REGION, S3_REGION);
    gvfsConf.set(S3Properties.GRAVITINO_S3_ROLE_ARN, S3_ROLE_ARN);
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

  private Configuration convertGvfsConfigToRealFileSystemConfig(Configuration gvfsConf) {
    Configuration s3Conf = new Configuration();
    Map<String, String> map = Maps.newHashMap();
    gvfsConf.forEach(entry -> map.put(entry.getKey(), entry.getValue()));
    map.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, S3_ACCESS_KEY);
    map.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, S3_SECRET_KEY);
    Map<String, String> hadoopConfMap =
        FileSystemUtils.toHadoopConfigMap(map, S3FileSystemProvider.GRAVITINO_KEY_TO_S3_HADOOP_KEY);
    hadoopConfMap.forEach(s3Conf::set);
    return s3Conf;
  }

  private String genStorageLocation(String fileset) {
    return String.format("s3a://%s/%s", BUCKET_NAME, fileset);
  }

  private Path genGvfsPath(String fileset) {
    return new Path(String.format("gvfs://fileset/%s/%s/%s", catalogName, schemaName, fileset));
  }

  private static boolean s3IsConfigured() {
    return StringUtils.isNotBlank(System.getenv("S3_ACCESS_KEY_ID_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_SECRET_ACCESS_KEY_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_ENDPOINT_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_BUCKET_NAME_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_REGION_FOR_CREDENTIAL"))
        && StringUtils.isNotBlank(System.getenv("S3_ROLE_ARN_FOR_CREDENTIAL"));
  }

  @Test
  void testCredentialVendingWithReadPrivilegeAllowsReadOnly() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("gvfs_auth_read");
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
            ImmutableMap.of(
                CredentialConstants.CREDENTIAL_PROVIDERS,
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE));

    // Seed a file so list/open works.
    Path realPath = new Path(storageLocation);
    try (FileSystem realFs =
        realPath.getFileSystem(convertGvfsConfigToRealFileSystemConfig(gvfsConf))) {
      realFs.mkdirs(realPath);
      try (FSDataOutputStream out = realFs.create(new Path(realPath, "seed.txt"), true)) {
        out.write("seed".getBytes(StandardCharsets.UTF_8));
      }
    }

    metalake.grantPrivilegesToRole(
        ROLE_NAME,
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
            msg.contains("accessdenied"),
            "Expected auth failure access denied due to missing WRITE_FILESET privilege, but got: "
                + ioe.getMessage());
      }
    } finally {
      metalake.revokePrivilegesFromRole(
          ROLE_NAME,
          MetadataObjects.of(
              ImmutableList.of(catalogName, schemaName, filesetName), MetadataObject.Type.FILESET),
          ImmutableSet.of(Privileges.ReadFileset.allow()));
      if (originalUser != null) {
        System.setProperty("user.name", originalUser);
      }
      metalake.loadCatalog(catalogName).asFilesetCatalog().dropFileset(filesetIdent);
    }
  }

  @Test
  void testCredentialVendingWithWritePrivilegeAllowsReadAndWrite() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("gvfs_auth_write");
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
            ImmutableMap.of(
                CredentialConstants.CREDENTIAL_PROVIDERS,
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE));

    metalake.grantPrivilegesToRole(
        ROLE_NAME,
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
      metalake.revokePrivilegesFromRole(
          ROLE_NAME,
          MetadataObjects.of(
              ImmutableList.of(catalogName, schemaName, filesetName), MetadataObject.Type.FILESET),
          ImmutableSet.of(Privileges.WriteFileset.allow()));
      if (originalUser != null) {
        System.setProperty("user.name", originalUser);
      }
      metalake.loadCatalog(catalogName).asFilesetCatalog().dropFileset(filesetIdent);
    }
  }
}
