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
package org.apache.gravitino.flink.connector.integration.test.hive;

import static org.apache.gravitino.server.authentication.KerberosConfig.KEYTAB;
import static org.apache.gravitino.server.authentication.KerberosConfig.PRINCIPAL;
import static org.apache.hadoop.minikdc.MiniKdc.MAX_TICKET_LIFETIME;

import com.google.common.collect.Maps;
import java.io.File;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalog;
import org.apache.gravitino.flink.connector.hive.GravitinoHiveCatalogFactoryOptions;
import org.apache.gravitino.flink.connector.integration.test.FlinkEnvIT;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for creating Gravitino Hive catalogs with Kerberos authentication. This test
 * verifies that catalog creation works correctly when the Gravitino server is configured with
 * Kerberos authentication. The test extends FlinkEnvIT directly to keep the test scope focused on
 * Kerberos-specific scenarios.
 */
@Tag("gravitino-docker-test")
public class FlinkHiveKerberosClientIT extends FlinkEnvIT {

  private static final KerberosSecurityTestcase kdc =
      new KerberosSecurityTestcase() {
        @Override
        public void createMiniKdcConf() {
          super.createMiniKdcConf();
          // Use a very short ticket lifetime to speed up Kerberos expiration in tests.
          // The test operations are executed immediately after the MiniKdc is started and
          // are not long-running, so a 5-second lifetime is sufficient and keeps the test
          // fast. If this test ever becomes flaky on slower environments or CI systems,
          // consider increasing this value to a more forgiving lifetime (for example, 60s).
          getConf().setProperty(MAX_TICKET_LIFETIME, "5");
        }
      };

  private static final String keytabFile =
      new File(System.getProperty("test.dir", "target"), UUID.randomUUID().toString())
          .getAbsolutePath();

  // Server principals for Gravitino server
  private static final String serverPrincipal = "HTTP/localhost@EXAMPLE.COM";
  private static final String serverPrincipalWithAll = "HTTP/0.0.0.0@EXAMPLE.COM";

  // Client principal for authentication
  private static final String clientPrincipal = "client@EXAMPLE.COM";

  @Override
  protected String getProvider() {
    return "hive";
  }

  @Override
  @BeforeAll
  public void startIntegrationTest() throws Exception {
    // Set up Kerberos before starting integration test
    kdc.startMiniKdc();
    initKeyTab();

    // Configure a Gravitino server with Kerberos
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.KERBEROS.name().toLowerCase());
    configs.put(PRINCIPAL.getKey(), serverPrincipal);
    configs.put(KEYTAB.getKey(), keytabFile);
    configs.put("client.kerberos.principal", clientPrincipal);
    configs.put("client.kerberos.keytab", keytabFile);

    registerCustomConfigs(configs);

    // Start the integration test (starts Gravitino server)
    super.startIntegrationTest();
  }

  @Override
  @AfterAll
  public void stopIntegrationTest() {
    try {
      super.stopIntegrationTest();
      UserGroupInformation.setConfiguration(new org.apache.hadoop.conf.Configuration(false));
      kdc.stopMiniKdc();
    } catch (Exception e) {
      throw new RuntimeException("Failed to stop Kerberos test", e);
    }
  }

  @Override
  protected void initFlinkEnv() {
    // Initialize Kerberos authentication for Hadoop UGI.
    // This approach follows Flink's HadoopLoginModule pattern for Kerberos authentication,
    // which performs a login from keytab to establish a secure context before creating
    // Flink table environment. See org.apache.flink.runtime.security.modules.HadoopLoginModule
    // in Flink source code for reference.
    org.apache.hadoop.conf.Configuration hadoopConfig = new org.apache.hadoop.conf.Configuration();

    hadoopConfig.set("hadoop.security.authentication", "kerberos");
    hadoopConfig.set("hadoop.security.auth_to_local", "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//");

    UserGroupInformation.setConfiguration(hadoopConfig);
    UserGroupInformation proxyUser = null;
    try {
      proxyUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientPrincipal, keytabFile);
    } catch (Exception e) {
      throw new RuntimeException("Failed to obtain UGI for Kerberos user", e);
    }

    final Configuration configuration = new Configuration();
    configuration.setString(
        "table.catalog-store.kind", GravitinoCatalogStoreFactoryOptions.GRAVITINO);
    configuration.setString("table.catalog-store.gravitino.gravitino.metalake", GRAVITINO_METALAKE);
    configuration.setString("table.catalog-store.gravitino.gravitino.uri", gravitinoUri);
    EnvironmentSettings.Builder builder =
        EnvironmentSettings.newInstance().withConfiguration(configuration);
    tableEnv =
        proxyUser.doAs(
            (PrivilegedAction<TableEnvironment>)
                () -> TableEnvironment.create(builder.inBatchMode().build()));
  }

  @Test
  public void testCreateGravitinoHiveCatalogWithKerberosAuth() {
    tableEnv.useCatalog(DEFAULT_CATALOG);
    int numCatalogs = tableEnv.listCatalogs().length;

    // Create a new catalog with Kerberos authentication using SQL
    String catalogName = "gravitino_hive_kerberos";
    tableEnv.executeSql(
        String.format(
            "CREATE CATALOG %s WITH ("
                + "'type'='gravitino-hive', "
                + "'hive-conf-dir'='src/test/resources/flink-tests',"
                + "'hive.metastore.uris'='%s'"
                + ")",
            catalogName, hiveMetastoreUri));

    // Verify catalog exists in Gravitino
    Assertions.assertTrue(metalake.catalogExists(catalogName));

    // Check the catalog properties
    org.apache.gravitino.Catalog gravitinoCatalog = metalake.loadCatalog(catalogName);
    Assertions.assertNotNull(gravitinoCatalog);
    Assertions.assertEquals(catalogName, gravitinoCatalog.name());

    // Assert creator is the Kerberos-authenticated client principal
    Assertions.assertEquals("client", gravitinoCatalog.auditInfo().creator());

    Map<String, String> properties = gravitinoCatalog.properties();
    Assertions.assertEquals(hiveMetastoreUri, properties.get(HiveConstants.METASTORE_URIS));

    // Verify Flink-specific properties are stored correctly
    Map<String, String> flinkProperties =
        properties.entrySet().stream()
            .filter(e -> e.getKey().startsWith(PropertiesConverter.FLINK_PROPERTY_PREFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Assertions.assertEquals(2, flinkProperties.size());
    Assertions.assertEquals(
        "src/test/resources/flink-tests",
        flinkProperties.get(flinkByPass(HiveCatalogFactoryOptions.HIVE_CONF_DIR.key())));
    Assertions.assertEquals(
        GravitinoHiveCatalogFactoryOptions.IDENTIFIER,
        flinkProperties.get(flinkByPass(CommonCatalogOptions.CATALOG_TYPE.key())));

    // Get the created catalog
    Optional<Catalog> catalog = tableEnv.getCatalog(catalogName);
    Assertions.assertTrue(catalog.isPresent());
    Assertions.assertInstanceOf(GravitinoHiveCatalog.class, catalog.get());

    // List catalogs
    String[] catalogs = tableEnv.listCatalogs();
    Assertions.assertEquals(numCatalogs + 1, catalogs.length, "Should create a new catalog");
    Assertions.assertTrue(
        Arrays.asList(catalogs).contains(catalogName), "Should create the correct catalog.");

    // Use the catalog
    tableEnv.useCatalog(catalogName);
    Assertions.assertEquals(
        catalogName,
        tableEnv.getCurrentCatalog(),
        "Current catalog should be the Kerberos-authenticated catalog.");

    // Drop the catalog
    tableEnv.useCatalog(DEFAULT_CATALOG);
    tableEnv.executeSql("DROP CATALOG " + catalogName);
    Assertions.assertFalse(metalake.catalogExists(catalogName));

    Optional<Catalog> droppedCatalog = tableEnv.getCatalog(catalogName);
    Assertions.assertFalse(droppedCatalog.isPresent(), "Catalog should be dropped");
  }

  private static void initKeyTab() throws Exception {
    File newKeytabFile = new File(keytabFile);
    String newClientPrincipal = removeRealm(clientPrincipal);
    String newServerPrincipal = removeRealm(serverPrincipal);
    String newServerPrincipalAll = removeRealm(serverPrincipalWithAll);
    kdc.getKdc()
        .createPrincipal(
            newKeytabFile, newClientPrincipal, newServerPrincipal, newServerPrincipalAll);
  }

  private static String removeRealm(String principal) {
    return principal.substring(0, principal.lastIndexOf("@"));
  }
}
