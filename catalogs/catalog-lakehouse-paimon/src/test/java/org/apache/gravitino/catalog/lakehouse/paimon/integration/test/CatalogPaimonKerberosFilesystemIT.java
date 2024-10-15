package org.apache.gravitino.catalog.lakehouse.paimon.integration.test;

import java.util.HashMap;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonKerberosFilesystemIT extends CatalogPaimonKerberosBaseIT {

  @Override
  protected void initCatalogProperties() {
    catalogProperties = new HashMap<>();
    String type = "filesystem";
    String warehouse =
        String.format(
            "hdfs://%s:%d/user/hive/paimon_catalog_warehouse/",
            kerberosHiveContainer.getContainerIpAddress(), HiveContainer.HDFS_DEFAULTFS_PORT);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, type);
    catalogProperties.put(PaimonCatalogPropertiesMetadata.WAREHOUSE, warehouse);
  }
}
