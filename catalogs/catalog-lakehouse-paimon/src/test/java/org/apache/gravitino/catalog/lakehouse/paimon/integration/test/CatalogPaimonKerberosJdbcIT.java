package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonKerberosJdbcIT extends CatalogPaimonKerberosBaseIT {

  @Override
  protected void initCatalogProperties() {
    containerSuite.startMySQLContainer(TEST_DB_NAME);
    mysqlContainer = containerSuite.getMySQLContainer();

    catalogProperties = new HashMap<>();
    String type = "jdbc";
    String warehouse =
        String.format(
            "hdfs://%s:%d/user/hive/paimon_catalog_warehouse_with_kerberos/",
            kerberosHiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT);
    String uri = mysqlContainer.getJdbcUrl(TEST_DB_NAME);
    String jdbcUser = mysqlContainer.getUsername();
    String jdbcPassword = mysqlContainer.getPassword();

    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, type);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, warehouse);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.URI, uri);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.JDBC_USER, jdbcUser);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.JDBC_PASSWORD, jdbcPassword);
  }

  @Test
  void testPaimonWithoutClientKerberosLogin() {
    adminClient = GravitinoAdminClient.builder(serverUri).build();

    String metalakeName = GravitinoITUtils.genRandomName("test_metalake");
    GravitinoMetalake gravitinoMetalake =
        adminClient.createMetalake(metalakeName, null, ImmutableMap.of());

    // Create a catalog
    Map<String, String> properties = Maps.newHashMap();
    properties.putAll(catalogProperties);
    Catalog catalog =
        gravitinoMetalake.createCatalog(
            CATALOG_NAME, Catalog.Type.RELATIONAL, "lakehouse-paimon", "comment", properties);

    // Test create schema
    Exception exception =
        Assertions.assertThrows(
            Exception.class,
            () -> catalog.asSchemas().createSchema(SCHEMA_NAME, "comment", ImmutableMap.of()));
    String exceptionMessage = Throwables.getStackTraceAsString(exception);

    // Make sure the real user is 'cli' because no impersonation here.
    Assertions.assertTrue(exceptionMessage.contains("Permission denied: user=cli, access=WRITE"));
  }
}
