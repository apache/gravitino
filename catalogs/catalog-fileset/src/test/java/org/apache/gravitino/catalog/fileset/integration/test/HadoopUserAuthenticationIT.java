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

package org.apache.gravitino.catalog.fileset.integration.test;

import static org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig.AUTH_TYPE_KEY;
import static org.apache.gravitino.catalog.fileset.authentication.kerberos.KerberosConfig.IMPERSONATION_ENABLE_KEY;
import static org.apache.gravitino.catalog.fileset.authentication.kerberos.KerberosConfig.KEY_TAB_URI_KEY;
import static org.apache.gravitino.catalog.fileset.authentication.kerberos.KerberosConfig.PRINCIPAL_KEY;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.MountableFile;

@Tag("gravitino-docker-test")
public class HadoopUserAuthenticationIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopUserAuthenticationIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String SDK_KERBEROS_PRINCIPAL_KEY = "client.kerberos.principal";
  private static final String SDK_KERBEROS_KEYTAB_KEY = "client.kerberos.keytab";

  private static final String GRAVITINO_CLIENT_PRINCIPAL = "gravitino_client@HADOOPKRB";
  private static final String GRAVITINO_CLIENT_KEYTAB = "/gravitino_client.keytab";

  private static final String GRAVITINO_SERVER_PRINCIPAL = "HTTP/localhost@HADOOPKRB";
  private static final String GRAVITINO_SERVER_KEYTAB = "/gravitino_server.keytab";

  private static final String HADOOP_CLIENT_PRINCIPAL = "cli@HADOOPKRB";
  private static final String HADOOP_CLIENT_KEYTAB = "/client.keytab";

  private static final String HADOOP_SCHEMA_PRINCIPAL = "cli_schema";
  private static final String HADOOP_FILESET_PRINCIPAL = "cli_fileset";

  private static final String HADOOP_SCHEMA_KEYTAB = "/cli_schema.keytab";
  private static final String HADOOP_FILESET_KEYTAB = "/cli_fileset.keytab";

  private static final String REALM = "HADOOPKRB";
  private static final String ADMIN_PASSWORD = "Admin12!";

  private static String TMP_DIR;

  private static String HDFS_URL;

  private static GravitinoAdminClient adminClient;

  private static HiveContainer kerberosHiveContainer;

  private static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("CatalogHadoop_metalake");
  private static final String CATALOG_NAME =
      GravitinoITUtils.genRandomName("CatalogHadoop_catalog");
  private static final String SCHEMA_NAME = GravitinoITUtils.genRandomName("CatalogHadoop_schema");

  @SuppressWarnings("unused")
  private static final String TABLE_NAME = "test_table";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startKerberosHiveContainer();
    kerberosHiveContainer = containerSuite.getKerberosHiveContainer();

    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    TMP_DIR = file.getAbsolutePath();

    HDFS_URL = String.format("hdfs://%s:9000", kerberosHiveContainer.getContainerIpAddress());

    // Prepare kerberos related-config;
    prepareKerberosConfig();

    // Config kerberos configuration for Gravitino server
    addKerberosConfig();

    // Start Gravitino server
    super.startIntegrationTest();
  }

  @AfterAll
  public static void stop() {
    // Reset the UGI
    UserGroupInformation.reset();

    // Clean up the kerberos configuration
    System.clearProperty("java.security.krb5.conf");
    System.clearProperty("sun.security.krb5.debug");
  }

  private static void prepareKerberosConfig() throws IOException {
    // Keytab of the Gravitino SDK client
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/gravitino_client.keytab", TMP_DIR + GRAVITINO_CLIENT_KEYTAB);

    createKeyTableForSchemaAndFileset();

    // Keytab of the Gravitino server
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/gravitino_server.keytab", TMP_DIR + GRAVITINO_SERVER_KEYTAB);

    // Keytab of Gravitino server to connector to HDFS
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/etc/admin.keytab", TMP_DIR + HADOOP_CLIENT_KEYTAB);

    String tmpKrb5Path = TMP_DIR + "krb5.conf_tmp";
    String krb5Path = TMP_DIR + "krb5.conf";
    kerberosHiveContainer.getContainer().copyFileFromContainer("/etc/krb5.conf", tmpKrb5Path);

    // Modify the krb5.conf and change the kdc and admin_server to the container IP
    String ip = containerSuite.getKerberosHiveContainer().getContainerIpAddress();
    String content = FileUtils.readFileToString(new File(tmpKrb5Path), StandardCharsets.UTF_8);
    content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
    content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
    FileUtils.write(new File(krb5Path), content, StandardCharsets.UTF_8);

    LOG.info("Kerberos kdc config:\n{}", content);
    System.setProperty("java.security.krb5.conf", krb5Path);
    System.setProperty("sun.security.krb5.debug", "true");
  }

  private static void createKeyTableForSchemaAndFileset() throws IOException {
    String shellContent =
        "echo -e \"%s\\n%s\" | kadmin.local -q \"addprinc %s@%s\""
            + "\n"
            + "kadmin.local -q \"xst -k /%s.keytab -norandkey %s@%s\"";

    String createSchemaShellFile = String.format("/%s.sh", HADOOP_SCHEMA_PRINCIPAL);
    String createFileSetShellFile = String.format("/%s.sh", HADOOP_FILESET_PRINCIPAL);

    FileUtils.writeStringToFile(
        Paths.get(TMP_DIR + createSchemaShellFile).toFile(),
        String.format(
            shellContent,
            ADMIN_PASSWORD,
            ADMIN_PASSWORD,
            HADOOP_SCHEMA_PRINCIPAL,
            REALM,
            HADOOP_SCHEMA_PRINCIPAL,
            HADOOP_SCHEMA_PRINCIPAL,
            REALM),
        StandardCharsets.UTF_8);
    kerberosHiveContainer
        .getContainer()
        .copyFileToContainer(
            MountableFile.forHostPath(TMP_DIR + createSchemaShellFile), createSchemaShellFile);
    kerberosHiveContainer.executeInContainer("bash", createSchemaShellFile);

    FileUtils.writeStringToFile(
        Paths.get(TMP_DIR + createFileSetShellFile).toFile(),
        String.format(
            shellContent,
            ADMIN_PASSWORD,
            ADMIN_PASSWORD,
            HADOOP_FILESET_PRINCIPAL,
            REALM,
            HADOOP_FILESET_PRINCIPAL,
            HADOOP_FILESET_PRINCIPAL,
            REALM),
        StandardCharsets.UTF_8);
    kerberosHiveContainer
        .getContainer()
        .copyFileToContainer(
            MountableFile.forHostPath(TMP_DIR + createFileSetShellFile), createFileSetShellFile);
    kerberosHiveContainer.executeInContainer("bash", createFileSetShellFile);

    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer(HADOOP_SCHEMA_KEYTAB, TMP_DIR + HADOOP_SCHEMA_KEYTAB);
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer(HADOOP_FILESET_KEYTAB, TMP_DIR + HADOOP_FILESET_KEYTAB);
  }

  private void addKerberosConfig() {
    customConfigs.put(Configs.AUTHENTICATORS.getKey(), "kerberos");
    customConfigs.put("gravitino.authenticator.kerberos.principal", GRAVITINO_SERVER_PRINCIPAL);
    customConfigs.put("gravitino.authenticator.kerberos.keytab", TMP_DIR + GRAVITINO_SERVER_KEYTAB);
    customConfigs.put(SDK_KERBEROS_KEYTAB_KEY, TMP_DIR + GRAVITINO_CLIENT_KEYTAB);
    customConfigs.put(SDK_KERBEROS_PRINCIPAL_KEY, GRAVITINO_CLIENT_PRINCIPAL);
  }

  @Test
  public void testUserAuthentication() {
    KerberosTokenProvider provider =
        KerberosTokenProvider.builder()
            .withClientPrincipal(GRAVITINO_CLIENT_PRINCIPAL)
            .withKeyTabFile(new File(TMP_DIR + GRAVITINO_CLIENT_KEYTAB))
            .build();
    adminClient = GravitinoAdminClient.builder(serverUri).withKerberosAuth(provider).build();

    GravitinoMetalake[] metalakes = adminClient.listMetalakes();
    Assertions.assertEquals(0, metalakes.length);

    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(METALAKE_NAME, null, ImmutableMap.of());

    // Create a catalog
    Map<String, String> properties = Maps.newHashMap();

    properties.put(AUTH_TYPE_KEY, "kerberos");
    properties.put(IMPERSONATION_ENABLE_KEY, "true");
    properties.put(KEY_TAB_URI_KEY, TMP_DIR + HADOOP_CLIENT_KEYTAB);
    properties.put(PRINCIPAL_KEY, HADOOP_CLIENT_PRINCIPAL);
    properties.put("location", HDFS_URL + "/user/hadoop/");

    kerberosHiveContainer.executeInContainer("hadoop", "fs", "-mkdir", "/user/hadoop");

    Catalog catalog =
        gravitinoMetalake.createCatalog(
            CATALOG_NAME, Catalog.Type.FILESET, "hadoop", "comment", properties);

    // Test create schema
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));
    String exceptionMessage = Throwables.getStackTraceAsString(exception);
    // Make sure real user is 'gravitino_client'
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=gravitino_client, access=WRITE"));

    // Make the property wrong by changing the principal
    gravitinoMetalake.alterCatalog(
        CATALOG_NAME, CatalogChange.setProperty(PRINCIPAL_KEY, HADOOP_CLIENT_PRINCIPAL + "wrong"));
    exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));
    exceptionMessage = Throwables.getStackTraceAsString(exception);
    Assertions.assertTrue(exceptionMessage.contains("Failed to login with Kerberos"));

    // Restore the property, everything goes okay.
    gravitinoMetalake.alterCatalog(
        CATALOG_NAME, CatalogChange.setProperty(PRINCIPAL_KEY, HADOOP_CLIENT_PRINCIPAL));

    // Now try to give the user the permission to create schema again
    kerberosHiveContainer.executeInContainer("hadoop", "fs", "-chmod", "-R", "777", "/user/hadoop");
    Assertions.assertDoesNotThrow(
        () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));

    catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(SCHEMA_NAME, TABLE_NAME),
            "comment",
            Fileset.Type.MANAGED,
            null,
            ImmutableMap.of());

    catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(SCHEMA_NAME, TABLE_NAME));

    catalog.asSchemas().alterSchema(SCHEMA_NAME, SchemaChange.setProperty("k1", "value1"));

    catalog.asSchemas().dropSchema(SCHEMA_NAME, true);
  }

  @Test
  void testCreateSchemaWithKerberos() {
    KerberosTokenProvider provider =
        KerberosTokenProvider.builder()
            .withClientPrincipal(GRAVITINO_CLIENT_PRINCIPAL)
            .withKeyTabFile(new File(TMP_DIR + GRAVITINO_CLIENT_KEYTAB))
            .build();
    adminClient = GravitinoAdminClient.builder(serverUri).withKerberosAuth(provider).build();

    String metalakeName = GravitinoITUtils.genRandomName("metalake");
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(metalakeName, null, ImmutableMap.of());

    // Create a catalog
    Map<String, String> properties = Maps.newHashMap();
    String location = HDFS_URL + "/user/hadoop/" + catalogName;

    properties.put(AUTH_TYPE_KEY, "kerberos");
    properties.put(IMPERSONATION_ENABLE_KEY, "true");
    properties.put(KEY_TAB_URI_KEY, TMP_DIR + HADOOP_CLIENT_KEYTAB);
    properties.put(PRINCIPAL_KEY, HADOOP_CLIENT_PRINCIPAL);
    properties.put("location", location);

    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-mkdir", "-p", "/user/hadoop/" + catalogName);

    Catalog catalog =
        gravitinoMetalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "comment", properties);

    // Test create schema
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));
    String exceptionMessage = Throwables.getStackTraceAsString(exception);
    // Make sure real user is 'gravitino_client'
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=gravitino_client, access=WRITE"));

    Map<String, String> schemaProperty = new HashMap<>();
    schemaProperty.put(AUTH_TYPE_KEY, "kerberos");
    // Disable impersonation here, so the user is the same as the principal
    schemaProperty.put(IMPERSONATION_ENABLE_KEY, "false");
    schemaProperty.put(KEY_TAB_URI_KEY, TMP_DIR + HADOOP_SCHEMA_KEYTAB);
    schemaProperty.put(PRINCIPAL_KEY, HADOOP_SCHEMA_PRINCIPAL + "@" + REALM);

    exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", schemaProperty));
    exceptionMessage = Throwables.getStackTraceAsString(exception);
    // Make sure real user is 'cli_schema'
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=cli_schema, access=WRITE"));

    // enable user impersonation, so the real user is gravitino_client
    schemaProperty.put(IMPERSONATION_ENABLE_KEY, "true");
    exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", schemaProperty));
    exceptionMessage = Throwables.getStackTraceAsString(exception);
    // Make sure real user is 'gravitino_client' if user impersonation enabled.
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=gravitino_client, access=WRITE"));

    // Now try to give the user the permission to create schema again
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chown", "gravitino_client", "/user/hadoop/" + catalogName);
    Assertions.assertDoesNotThrow(
        () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", schemaProperty));

    // Disable impersonation here, so the user is the same as the principal 'cli_schema'
    schemaProperty.put(IMPERSONATION_ENABLE_KEY, "false");
    exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                catalog.asSchemas().createSchema(SCHEMA_NAME + "_new", "comment", schemaProperty));
    exceptionMessage = Throwables.getStackTraceAsString(exception);
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=cli_schema, access=WRITE"));

    // END of test schema creation
    Assertions.assertDoesNotThrow(() -> catalog.asSchemas().dropSchema(SCHEMA_NAME, true));

    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chown", "cli_schema", "/user/hadoop/" + catalogName);
    Assertions.assertDoesNotThrow(
        () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", schemaProperty));

    catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(SCHEMA_NAME, TABLE_NAME),
            "comment",
            Fileset.Type.MANAGED,
            null,
            ImmutableMap.of());
  }

  @Test
  void createFilesetWithKerberos() {
    KerberosTokenProvider provider =
        KerberosTokenProvider.builder()
            .withClientPrincipal(GRAVITINO_CLIENT_PRINCIPAL)
            .withKeyTabFile(new File(TMP_DIR + GRAVITINO_CLIENT_KEYTAB))
            .build();
    adminClient = GravitinoAdminClient.builder(serverUri).withKerberosAuth(provider).build();

    String metalakeName = GravitinoITUtils.genRandomName("metalake");
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(metalakeName, null, ImmutableMap.of());

    // Create a catalog
    Map<String, String> properties = Maps.newHashMap();
    String location = HDFS_URL + "/user/hadoop/" + catalogName;

    properties.put(AUTH_TYPE_KEY, "kerberos");
    properties.put(IMPERSONATION_ENABLE_KEY, "true");
    properties.put(KEY_TAB_URI_KEY, TMP_DIR + HADOOP_CLIENT_KEYTAB);
    properties.put(PRINCIPAL_KEY, HADOOP_CLIENT_PRINCIPAL);
    properties.put("location", location);

    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-mkdir", "-p", "/user/hadoop/" + catalogName);

    Catalog catalog =
        gravitinoMetalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "comment", properties);

    Map<String, String> schemaProperty = new HashMap<>();
    schemaProperty.put(AUTH_TYPE_KEY, "kerberos");
    // Disable impersonation here, so the user is the same as the principal as 'cli_schema'
    schemaProperty.put(IMPERSONATION_ENABLE_KEY, "false");
    schemaProperty.put(KEY_TAB_URI_KEY, TMP_DIR + HADOOP_SCHEMA_KEYTAB);
    schemaProperty.put(PRINCIPAL_KEY, HADOOP_SCHEMA_PRINCIPAL + "@" + REALM);

    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chown", "-R", "cli_schema", "/user/hadoop/" + catalogName);
    Assertions.assertDoesNotThrow(
        () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", schemaProperty));

    catalog
        .asFilesetCatalog()
        .createFileset(
            NameIdentifier.of(SCHEMA_NAME, TABLE_NAME),
            "comment",
            Fileset.Type.MANAGED,
            null,
            ImmutableMap.of());

    Map<String, String> tableProperty = Maps.newHashMap();
    tableProperty.put(AUTH_TYPE_KEY, "kerberos");
    // Disable impersonation here, so the user is the same as the principal as 'cli_schema'
    tableProperty.put(IMPERSONATION_ENABLE_KEY, "false");
    tableProperty.put(KEY_TAB_URI_KEY, TMP_DIR + HADOOP_FILESET_KEYTAB);
    tableProperty.put(PRINCIPAL_KEY, HADOOP_FILESET_PRINCIPAL + "@" + REALM);

    String fileset1 = GravitinoITUtils.genRandomName("fileset1");
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                catalog
                    .asFilesetCatalog()
                    .createFileset(
                        NameIdentifier.of(SCHEMA_NAME, fileset1),
                        "comment",
                        Fileset.Type.MANAGED,
                        null,
                        tableProperty));
    String exceptionMessage = Throwables.getStackTraceAsString(exception);
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=cli_fileset, access=WRITE"));

    // Now change the owner of schema directory to 'cli_fileset'
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chown", "-R", "cli_fileset", "/user/hadoop/" + catalogName);
    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asFilesetCatalog()
                .createFileset(
                    NameIdentifier.of(SCHEMA_NAME, fileset1),
                    "comment",
                    Fileset.Type.MANAGED,
                    null,
                    tableProperty));

    // enable user impersonation, so the real user is gravitino_client
    tableProperty.put(IMPERSONATION_ENABLE_KEY, "true");
    String fileset2 = GravitinoITUtils.genRandomName("fileset2");
    exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                catalog
                    .asFilesetCatalog()
                    .createFileset(
                        NameIdentifier.of(SCHEMA_NAME, fileset2),
                        "comment",
                        Fileset.Type.MANAGED,
                        null,
                        tableProperty));
    exceptionMessage = Throwables.getStackTraceAsString(exception);
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=gravitino_client, access=WRITE"));

    // Now change the owner of schema directory to 'gravitino_client'
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chown", "-R", "gravitino_client", "/user/hadoop/" + catalogName);
    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asFilesetCatalog()
                .createFileset(
                    NameIdentifier.of(SCHEMA_NAME, fileset2),
                    "comment",
                    Fileset.Type.MANAGED,
                    null,
                    tableProperty));

    // As the owner of the current schema directory is 'gravitino_client', the current user is
    // cli_fileset.
    Assertions.assertThrows(
        Exception.class,
        () -> catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(SCHEMA_NAME, fileset1)));
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chown", "-R", "cli_fileset", "/user/hadoop/" + catalogName);
    // Now try to drop fileset
    Assertions.assertDoesNotThrow(
        () -> catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(SCHEMA_NAME, fileset1)));

    Assertions.assertThrows(
        Exception.class,
        () -> catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(SCHEMA_NAME, fileset2)));
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chown", "-R", "gravitino_client", "/user/hadoop/" + catalogName);
    Assertions.assertDoesNotThrow(
        () -> catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(SCHEMA_NAME, fileset2)));

    Assertions.assertDoesNotThrow(() -> catalog.asSchemas().dropSchema(SCHEMA_NAME, true));
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chown", "-R", "cli_schema", "/user/hadoop/" + catalogName);
    Assertions.assertDoesNotThrow(() -> catalog.asSchemas().dropSchema(SCHEMA_NAME, true));
  }

  @Test
  void testUserImpersonation() {
    KerberosTokenProvider provider =
        KerberosTokenProvider.builder()
            .withClientPrincipal(GRAVITINO_CLIENT_PRINCIPAL)
            .withKeyTabFile(new File(TMP_DIR + GRAVITINO_CLIENT_KEYTAB))
            .build();
    adminClient = GravitinoAdminClient.builder(serverUri).withKerberosAuth(provider).build();

    String metalakeName = GravitinoITUtils.genRandomName("metalake");
    String catalogName = GravitinoITUtils.genRandomName("catalog");
    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(metalakeName, null, ImmutableMap.of());

    // Create a catalog
    Map<String, String> properties = Maps.newHashMap();
    String localtion = HDFS_URL + "/user/hadoop/" + catalogName;

    properties.put(IMPERSONATION_ENABLE_KEY, "true");
    properties.put(KEY_TAB_URI_KEY, TMP_DIR + HADOOP_CLIENT_KEYTAB);
    properties.put(PRINCIPAL_KEY, HADOOP_CLIENT_PRINCIPAL);
    properties.put(AUTH_TYPE_KEY, "kerberos");
    properties.put("location", localtion);

    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-mkdir", "-p", "/user/hadoop/" + catalogName);

    Catalog catalog =
        gravitinoMetalake.createCatalog(
            catalogName, Catalog.Type.FILESET, "hadoop", "comment", properties);

    Map<String, String> schemaProperty = new HashMap<>();

    // Test set schema IMPERSONATION_ENABLE_KEY to true, the final result is:
    // IMPERSONATION_ENABLE_KEY is true
    // so the user access HDFS is user 'gravitino_client'
    schemaProperty.put(IMPERSONATION_ENABLE_KEY, "true");
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", schemaProperty));
    String exceptionMessage = Throwables.getStackTraceAsString(exception);
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=gravitino_client, access=WRITE"));

    // Test set schema IMPERSONATION_ENABLE_KEY to false, the final result is:
    // IMPERSONATION_ENABLE_KEY is false
    // so the user access HDFS is user 'cli'
    schemaProperty.put(IMPERSONATION_ENABLE_KEY, "false");
    exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", schemaProperty));
    exceptionMessage = Throwables.getStackTraceAsString(exception);
    Assertions.assertTrue(exceptionMessage.contains("Permission denied: user=cli, access=WRITE"));

    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chown", "-R", "cli", "/user/hadoop/" + catalogName);
    Assertions.assertDoesNotThrow(
        () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", schemaProperty));

    String filesetName = GravitinoITUtils.genRandomName("fileset");
    Map<String, String> filesetProperty = new HashMap<>();
    filesetProperty.put(IMPERSONATION_ENABLE_KEY, "true");
    exception =
        Assertions.assertThrows(
            Exception.class,
            () ->
                catalog
                    .asFilesetCatalog()
                    .createFileset(
                        NameIdentifier.of(SCHEMA_NAME, filesetName),
                        "comment",
                        Fileset.Type.MANAGED,
                        null,
                        filesetProperty));
    exceptionMessage = Throwables.getStackTraceAsString(exception);
    Assertions.assertTrue(
        exceptionMessage.contains("Permission denied: user=gravitino_client, access=WRITE"));

    // Line 602 has set the owner of the schema directory to 'cli', if the IMPERSONATION_ENABLE_KEY
    // is false, the user is 'cli'
    filesetProperty.put(IMPERSONATION_ENABLE_KEY, "false");
    Assertions.assertDoesNotThrow(
        () ->
            catalog
                .asFilesetCatalog()
                .createFileset(
                    NameIdentifier.of(SCHEMA_NAME, filesetName),
                    "comment",
                    Fileset.Type.MANAGED,
                    null,
                    filesetProperty));

    catalog.asFilesetCatalog().dropFileset(NameIdentifier.of(SCHEMA_NAME, filesetName));
    catalog.asSchemas().dropSchema(SCHEMA_NAME, true);
    gravitinoMetalake.dropCatalog(catalogName, true);
    adminClient.dropMetalake(metalakeName, true);
  }
}
