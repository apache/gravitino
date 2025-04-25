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
package org.apache.gravitino.catalog.lakehouse.hudi.integration.test;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.catalog.lakehouse.hudi.HudiCatalogPropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.hudi.backend.hms.kerberos.AuthenticationConfig;
import org.apache.gravitino.catalog.lakehouse.hudi.backend.hms.kerberos.KerberosConfig;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.client.KerberosTokenProvider;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.TableCatalog;
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
public class HudiCatalogKerberosHiveIT extends BaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(HudiCatalogKerberosHiveIT.class);

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
  static String SCHEMA_NAME = "default";
  static String TABLE_NAME = GravitinoITUtils.genRandomName("test_table");

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

    // Create hive tables
    createHudiTables();

    // Start Gravitino server
    super.startIntegrationTest();
  }

  @AfterAll
  public void stop() {
    // Reset the UGI
    org.apache.hadoop.security.UserGroupInformation.reset();

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
    String content =
        FileUtils.readFileToString(new File(tmpKrb5Path), java.nio.charset.StandardCharsets.UTF_8);
    content = content.replace("kdc = localhost:88", "kdc = " + ip + ":88");
    content = content.replace("admin_server = localhost", "admin_server = " + ip + ":749");
    FileUtils.write(new File(krb5Path), content, java.nio.charset.StandardCharsets.UTF_8);

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

      java.lang.reflect.Method refershMethod = classRef.getMethod("refresh");
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
  public void testHudiCatalogWithKerberos() {
    KerberosTokenProvider provider =
        KerberosTokenProvider.builder()
            .withClientPrincipal(GRAVITINO_CLIENT_PRINCIPAL)
            .withKeyTabFile(new File(TMP_DIR + GRAVITINO_CLIENT_KEYTAB))
            .build();
    adminClient = GravitinoAdminClient.builder(serverUri).withKerberosAuth(provider).build();
    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(METALAKE_NAME, null, ImmutableMap.of());

    // Create a catalog with kerberos
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AuthenticationConfig.IMPERSONATION_ENABLE_KEY, "true");
    properties.put(AuthenticationConfig.AUTH_TYPE_KEY, "kerberos");
    properties.put(KerberosConfig.KEY_TAB_URI_KEY, TMP_DIR + HIVE_METASTORE_CLIENT_KEYTAB);
    properties.put(KerberosConfig.PRINCIPAL_KEY, HIVE_METASTORE_CLIENT_PRINCIPAL);
    properties.put("list-all-tables", "false");
    properties.put(
        CATALOG_BYPASS_PREFIX + "hive.metastore.kerberos.principal",
        "hive/_HOST@HADOOPKRB".replace("_HOST", kerberosHiveContainer.getHostName()));
    properties.put(CATALOG_BYPASS_PREFIX + "hive.metastore.sasl.enabled", "true");
    properties.put(CATALOG_BYPASS_PREFIX + "hadoop.security.authentication", "kerberos");

    properties.put(HudiCatalogPropertiesMetadata.CATALOG_BACKEND, "hms");
    properties.put(HudiCatalogPropertiesMetadata.URI, HIVE_METASTORE_URI);
    properties.put(
        "warehouse",
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-hudi",
            kerberosHiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT));
    Catalog catalog =
        gravitinoMetalake.createCatalog(
            CATALOG_NAME, Catalog.Type.RELATIONAL, "lakehouse-hudi", "comment", properties);
    LOG.info("create catalog: {} sucess", catalog.name());
    Assertions.assertEquals(CATALOG_NAME, catalog.name());

    Schema schema = catalog.asSchemas().loadSchema(SCHEMA_NAME);
    Assertions.assertEquals(SCHEMA_NAME, schema.name());

    TableCatalog tableOps = catalog.asTableCatalog();
    NoSuchTableException exception =
        Assertions.assertThrows(
            NoSuchTableException.class,
            () -> tableOps.loadTable(NameIdentifier.of(SCHEMA_NAME, TABLE_NAME)));

    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("Hudi table does not exist: " + TABLE_NAME + " in Hive Metastore"),
        "Unexpected exception message: " + exception.getMessage());
  }

  private static void createHudiTables() {
    String createTableSql =
        String.format(
            "CREATE TABLE %s.%s (\n"
                + "  id STRING,\n"
                + "  name STRING,\n"
                + "  age INT\n"
                + ") \n"
                + "LOCATION 'hdfs://localhost:9000/user/hive/warehouse-catalog-hudi' ",
            SCHEMA_NAME, TABLE_NAME);
    kerberosHiveContainer.executeInContainer("hive", "-e", "'" + createTableSql + ";'");
    LOG.info("create table {} success[{}]", TABLE_NAME, createTableSql);

    String insertTableSql =
        String.format("INSERT INTO %s.%s" + " VALUES " + "(1,'Cyber',26)", SCHEMA_NAME, TABLE_NAME);
    kerberosHiveContainer.executeInContainer("hive", "-e", "'" + insertTableSql + ";'");
    LOG.info("insert table {} data success[{}]", TABLE_NAME, insertTableSql);
  }
}
