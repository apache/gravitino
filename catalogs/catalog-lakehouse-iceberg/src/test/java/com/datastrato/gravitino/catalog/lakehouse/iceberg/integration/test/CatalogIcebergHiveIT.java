/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.integration.test;

import com.datastrato.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CatalogIcebergHiveIT extends CatalogIcebergBaseIT {

  @Override
  protected void initIcebergCatalogProperties() {
    URIS =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);
    TYPE = "hive";
    WAREHOUSE =
        String.format(
            "hdfs://%s:%d/user/hive/iceberg-hive-warehouse/",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HDFS_DEFAULTFS_PORT);
  }
}
