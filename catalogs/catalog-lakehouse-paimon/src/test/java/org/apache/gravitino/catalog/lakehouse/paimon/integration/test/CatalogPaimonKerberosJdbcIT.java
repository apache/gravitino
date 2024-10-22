package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import java.util.HashMap;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.Tag;
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
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_USER, jdbcUser);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_JDBC_PASSWORD, jdbcPassword);
  }
}
