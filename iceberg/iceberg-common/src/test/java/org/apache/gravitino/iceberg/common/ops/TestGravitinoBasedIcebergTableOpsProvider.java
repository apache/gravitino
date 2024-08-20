package org.apache.gravitino.iceberg.common.ops;

import java.util.HashMap;
import java.util.UUID;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestGravitinoBasedIcebergTableOpsProvider {
  private static final String STORE_PATH =
      "/tmp/gravitino_test_iceberg_jdbc_backend_" + UUID.randomUUID().toString().replace("-", "");

  @Test
  public void testValidIcebergTableOps() {
    String hiveCatalogName = "hive_backend";
    String jdbcCatalogName = "jdbc_backend";

    Catalog hiveMockCatalog = Mockito.mock(Catalog.class);
    Catalog jdbcMockCatalog = Mockito.mock(Catalog.class);

    GravitinoMetalake gravitinoMetalake = Mockito.mock(GravitinoMetalake.class);
    Mockito.when(gravitinoMetalake.loadCatalog(hiveCatalogName)).thenReturn(hiveMockCatalog);
    Mockito.when(gravitinoMetalake.loadCatalog(jdbcCatalogName)).thenReturn(jdbcMockCatalog);

    Mockito.when(hiveMockCatalog.provider()).thenReturn("lakehouse-iceberg");
    Mockito.when(jdbcMockCatalog.provider()).thenReturn("lakehouse-iceberg");

    Mockito.when(hiveMockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "hive");
                put(IcebergConstants.URI, "thrift://127.0.0.1:7004");
                put(IcebergConstants.WAREHOUSE, "/tmp/usr/hive/warehouse");
                put(IcebergConstants.CATALOG_BACKEND_NAME, hiveCatalogName);
              }
            });
    Mockito.when(jdbcMockCatalog.properties())
        .thenReturn(
            new HashMap<String, String>() {
              {
                put(IcebergConstants.CATALOG_BACKEND, "jdbc");
                put(
                    IcebergConstants.URI,
                    String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", STORE_PATH));
                put(IcebergConstants.WAREHOUSE, "/tmp/user/hive/warehouse-jdbc");
                put(IcebergConstants.GRAVITINO_JDBC_USER, "gravitino");
                put(IcebergConstants.GRAVITINO_JDBC_PASSWORD, "gravitino");
                put(IcebergConstants.GRAVITINO_JDBC_DRIVER, "org.h2.Driver");
                put(IcebergConstants.ICEBERG_JDBC_INITIALIZE, "true");
                put(IcebergConstants.CATALOG_BACKEND_NAME, jdbcCatalogName);
              }
            });

    GravitinoBasedIcebergTableOpsProvider provider = new GravitinoBasedIcebergTableOpsProvider();
    provider.setGravitinoMetalake(gravitinoMetalake);

    IcebergTableOps hiveOps = provider.getIcebergTableOps(hiveCatalogName);
    IcebergTableOps jdbcOps = provider.getIcebergTableOps(jdbcCatalogName);

    Assertions.assertEquals(hiveCatalogName, hiveOps.catalog.name());
    Assertions.assertEquals(jdbcCatalogName, jdbcOps.catalog.name());

    Assertions.assertTrue(hiveOps.catalog instanceof HiveCatalog);
    Assertions.assertTrue(jdbcOps.catalog instanceof JdbcCatalog);
  }

  @Test
  public void testInvalidIcebergTableOps() {
    String invalidCatalogName = "invalid_catalog";

    Catalog invalidCatalog = Mockito.mock(Catalog.class);

    GravitinoMetalake gravitinoMetalake = Mockito.mock(GravitinoMetalake.class);
    Mockito.when(gravitinoMetalake.loadCatalog(invalidCatalogName)).thenReturn(invalidCatalog);

    Mockito.when(invalidCatalog.provider()).thenReturn("hive");

    GravitinoBasedIcebergTableOpsProvider provider = new GravitinoBasedIcebergTableOpsProvider();
    provider.setGravitinoMetalake(gravitinoMetalake);

    Assertions.assertThrowsExactly(
            RuntimeException.class, () -> provider.getIcebergTableOps(invalidCatalogName));
  }
}
