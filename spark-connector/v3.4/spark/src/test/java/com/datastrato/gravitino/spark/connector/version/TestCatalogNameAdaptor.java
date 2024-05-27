/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.version;

import com.datastrato.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark34;
import com.datastrato.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark34;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogNameAdaptor {
  @Test
  void testSpark34() {
    String hiveCatalogName = CatalogNameAdaptor.getCatalogName("hive");
    Assertions.assertEquals(GravitinoHiveCatalogSpark34.class.getName(), hiveCatalogName);

    String icebergCatalogName = CatalogNameAdaptor.getCatalogName("lakehouse-iceberg");
    Assertions.assertEquals(GravitinoIcebergCatalogSpark34.class.getName(), icebergCatalogName);
  }
}
