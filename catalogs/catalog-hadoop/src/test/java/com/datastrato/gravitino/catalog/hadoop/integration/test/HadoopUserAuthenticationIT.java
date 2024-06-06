/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.hadoop.integration.test;

import static com.datastrato.gravitino.catalog.hadoop.authentication.AuthenticationConfig.AUTH_TYPE_KEY;
import static com.datastrato.gravitino.catalog.hadoop.authentication.kerberos.KerberosConfig.IMPERSONATION_ENABLE_KEY;
import static com.datastrato.gravitino.catalog.hadoop.authentication.kerberos.KerberosConfig.KEY_TAB_URI_KEY;
import static com.datastrato.gravitino.catalog.hadoop.authentication.kerberos.KerberosConfig.PRINCIPAL_KEY;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.client.GravitinoAdminClient;
import com.datastrato.gravitino.client.GravitinoMetalake;
import com.datastrato.gravitino.client.KerberosTokenProvider;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

@Tag("gravitino-docker-it")
public class HadoopUserAuthenticationIT extends AbstractIT {
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
  public static void startIntegrationTest() throws Exception {
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
    AbstractIT.startIntegrationTest();
  }

  @AfterAll
  public static void stop() {
    // Reset the UGI
    UserGroupInformation.reset();

    // Clean up the kerberos configuration
    System.clearProperty("java.security.krb5.conf");
    System.clearProperty("sun.security.krb5.debug");
  }

  private static void prepareKerberosConfig() throws IOException, KrbException {
    // Keytab of the Gravitino SDK client
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/gravitino_client.keytab", TMP_DIR + GRAVITINO_CLIENT_KEYTAB);

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

  private static void addKerberosConfig() {
    AbstractIT.customConfigs.put("gravitino.authenticator", "kerberos");
    AbstractIT.customConfigs.put(
        "gravitino.authenticator.kerberos.principal", GRAVITINO_SERVER_PRINCIPAL);
    AbstractIT.customConfigs.put(
        "gravitino.authenticator.kerberos.keytab", TMP_DIR + GRAVITINO_SERVER_KEYTAB);
    AbstractIT.customConfigs.put(SDK_KERBEROS_KEYTAB_KEY, TMP_DIR + GRAVITINO_CLIENT_KEYTAB);
    AbstractIT.customConfigs.put(SDK_KERBEROS_PRINCIPAL_KEY, GRAVITINO_CLIENT_PRINCIPAL);
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
}
