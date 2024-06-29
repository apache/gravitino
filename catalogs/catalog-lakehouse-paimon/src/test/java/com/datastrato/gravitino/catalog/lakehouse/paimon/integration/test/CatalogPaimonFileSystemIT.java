/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon.integration.test;

import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import com.datastrato.gravitino.integration.test.container.HiveContainer;
import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogPaimonFileSystemIT extends CatalogPaimonBaseIT {

  @Override
  protected Map<String, String> initPaimonCatalogProperties() {

    Map<String, String> catalogProperties = Maps.newHashMap();
    catalogProperties.put("key1", "val1");
    catalogProperties.put("key2", "val2");

    catalogProperties.put(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, "filesystem");
    catalogProperties.put(
        PaimonCatalogPropertiesMetadata.WAREHOUSE,
        String.format(
            "hdfs://%s:%d/user/hive/warehouse-catalog-paimon/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT));

    return catalogProperties;
  }
}
