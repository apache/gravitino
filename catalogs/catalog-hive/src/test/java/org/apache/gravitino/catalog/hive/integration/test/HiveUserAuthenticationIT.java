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

package org.apache.gravitino.catalog.hive.integration.test;

import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMeta.IMPERSONATION_ENABLE;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMeta.KEY_TAB_URI;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;
import static org.apache.gravitino.catalog.hive.HiveCatalogPropertiesMeta.PRINCIPAL;
import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-test")
public class HiveUserAuthenticationIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(HiveUserAuthenticationIT.class);

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String SDK_KERBEROS_PRINCIPAL_KEY = "client.kerberos.principal";
  private static final String SDK_KERBEROS_KEYTAB_KEY = "client.kerberos.keytab";

  private static final String GRAVITINO_CLIENT_PRINCIPAL = "gravitino_client@HADOOPKRB";
  private static final String GRAVITINO_CLIENT_KEYTAB = "/gravitino_client.keytab";

  private static final String GRAVITINO_SERVER_PRINCIPAL = "HTTP/localhost@HADOOPKRB";
  private static final String GRAVITINO_SERVER_KEYTAB = "/gravitino_server.keytab";

  private static final String HIVE_METASTORE_CLIENT_PRINCIPAL = "cli@HADOOPKRB";
  private static final String HIVE_METASTORE_CLIENT_KEYTAB = "/client.keytab";

  private static String TMP_DIR;

  protected static String HIVE_METASTORE_URI;

  private static GravitinoAdminClient adminClient;

  protected static HiveContainer kerberosHiveContainer;

  static String METALAKE_NAME = GravitinoITUtils.genRandomName("test_metalake");
  static String CATALOG_NAME = GravitinoITUtils.genRandomName("test_catalog");
  static String SCHEMA_NAME = GravitinoITUtils.genRandomName("test_schema");
  static String TABLE_NAME = GravitinoITUtils.genRandomName("test_table");

  private static final String HIVE_COL_NAME1 = "col1";
  private static final String HIVE_COL_NAME2 = "col2";
  private static final String HIVE_COL_NAME3 = "col3";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startKerberosHiveContainer();
    kerberosHiveContainer = containerSuite.getKerberosHiveContainer();

    File baseDir = new File(System.getProperty("java.io.tmpdir"));
    File file = Files.createTempDirectory(baseDir.toPath(), "test").toFile();
    file.deleteOnExit();
    TMP_DIR = file.getAbsolutePath();

    HIVE_METASTORE_URI =
        String.format(
            "thrift://%s:%d",
            kerberosHiveContainer.getContainerIpAddress(), HiveContainer.HIVE_METASTORE_PORT);

    // Prepare kerberos related-config;
    prepareKerberosConfig();

    // Config kerberos configuration for Gravitino server
    addKerberosConfig();

    // Start Gravitino server
    super.startIntegrationTest();
  }

  @AfterAll
  public void stop() {
    // Reset the UGI
    UserGroupInformation.reset();

    LOG.info("krb5 path: {}", System.getProperty("java.security.krb5.conf"));
    // Clean up the kerberos configuration
    System.clearProperty("java.security.krb5.conf");
    System.clearProperty("sun.security.krb5.debug");

    client = null;
  }

  private static void prepareKerberosConfig() throws Exception {
    // Keytab of the Gravitino SDK client
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/gravitino_client.keytab", TMP_DIR + GRAVITINO_CLIENT_KEYTAB);

    // Keytab of the Gravitino server
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/gravitino_server.keytab", TMP_DIR + GRAVITINO_SERVER_KEYTAB);

    // Keytab of Gravitino server to connector to Hive
    kerberosHiveContainer
        .getContainer()
        .copyFileFromContainer("/etc/admin.keytab", TMP_DIR + HIVE_METASTORE_CLIENT_KEYTAB);

    String tmpKrb5Path = TMP_DIR + "/krb5.conf_tmp";
    String krb5Path = TMP_DIR + "/krb5.conf";
    kerberosHiveContainer.getContainer().copyFileFromContainer("/etc/krb5.conf", tmpKrb5Path);

    // Modify the krb5.conf and change the kdc and admin_server to the container IP
    String ip = containerSuite.getKerberosHiveContainer().getContainerIpAddress();
    String content = FileUtils.readFileToString(new File(tmpKrb5Path), StandardCharsets.UTF_8);
    content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
    content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
    FileUtils.write(new File(krb5Path), content, StandardCharsets.UTF_8);

    LOG.info("Kerberos kdc config:\n{}, path: {}", content, krb5Path);
    System.setProperty("java.security.krb5.conf", krb5Path);
    System.setProperty("sun.security.krb5.debug", "true");

    refreshKerberosConfig();
    KerberosName.resetDefaultRealm();

    LOG.info("Kerberos default realm: {}", KerberosUtil.getDefaultRealm());
  }

  private static void refreshKerberosConfig() {
    Class<?> classRef;
    try {
      if (System.getProperty("java.vendor").contains("IBM")) {
        classRef = Class.forName("com.ibm.security.krb5.internal.Config");
      } else {
        classRef = Class.forName("sun.security.krb5.Config");
      }

      Method refershMethod = classRef.getMethod("refresh");
      refershMethod.invoke(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(METALAKE_NAME, null, ImmutableMap.of());

    // Create a catalog
    Map<String, String> properties = Maps.newHashMap();
    properties.put(METASTORE_URIS, HIVE_METASTORE_URI);
    properties.put(IMPERSONATION_ENABLE, "true");
    properties.put(KEY_TAB_URI, TMP_DIR + HIVE_METASTORE_CLIENT_KEYTAB);
    properties.put(PRINCIPAL, HIVE_METASTORE_CLIENT_PRINCIPAL);
    properties.put(CATALOG_BYPASS_PREFIX + HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    properties.put(
        CATALOG_BYPASS_PREFIX + "hive.metastore.kerberos.principal",
        "hive/_HOST@HADOOPKRB"
            .replace("_HOST", containerSuite.getKerberosHiveContainer().getHostName()));
    properties.put(CATALOG_BYPASS_PREFIX + "hive.metastore.sasl.enabled", "true");
    Catalog catalog =
        gravitinoMetalake.createCatalog(
            CATALOG_NAME, Catalog.Type.RELATIONAL, "hive", "comment", properties);

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
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chmod", "-R", "777", "/user/hive/warehouse");
    Assertions.assertDoesNotThrow(
        () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));

    // Create table
    NameIdentifier tableNameIdentifier = NameIdentifier.of(SCHEMA_NAME, TABLE_NAME);
    catalog
        .asTableCatalog()
        .createTable(
            tableNameIdentifier,
            createColumns(),
            "",
            ImmutableMap.of(),
            Transforms.EMPTY_TRANSFORM,
            Distributions.NONE,
            SortOrders.NONE);

    // Now try to alter the table
    catalog.asTableCatalog().alterTable(tableNameIdentifier, TableChange.rename("new_table"));
    NameIdentifier newTableIdentifier = NameIdentifier.of(SCHEMA_NAME, "new_table");

    // Old table name should not exist
    Assertions.assertFalse(catalog.asTableCatalog().tableExists(tableNameIdentifier));
    Assertions.assertTrue(catalog.asTableCatalog().tableExists(newTableIdentifier));

    // Drop table
    catalog.asTableCatalog().dropTable(newTableIdentifier);
    Assertions.assertFalse(catalog.asTableCatalog().tableExists(newTableIdentifier));

    // Drop schema
    catalog.asSchemas().dropSchema(SCHEMA_NAME, true);
    Assertions.assertFalse(catalog.asSchemas().schemaExists(SCHEMA_NAME));

    // Drop catalog
    Assertions.assertTrue(gravitinoMetalake.dropCatalog(CATALOG_NAME, true));
  }

  @AfterAll
  static void restoreFilePermission() {
    kerberosHiveContainer.executeInContainer(
        "hadoop", "fs", "-chmod", "-R", "755", "/user/hive/warehouse");
  }

  private static Column[] createColumns() {
    Column col1 = Column.of(HIVE_COL_NAME1, Types.ByteType.get(), "col_1_comment");
    Column col2 = Column.of(HIVE_COL_NAME2, Types.DateType.get(), "col_2_comment");
    Column col3 = Column.of(HIVE_COL_NAME3, Types.StringType.get(), "col_3_comment");
    return new Column[] {col1, col2, col3};
  }
}
