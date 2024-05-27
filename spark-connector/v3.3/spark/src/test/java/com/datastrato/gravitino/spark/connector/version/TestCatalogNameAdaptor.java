/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.version;

import com.datastrato.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark33;
import com.datastrato.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark33;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogNameAdaptor {
  @Test
  void testSpark33() {
    String hiveCatalogName = CatalogNameAdaptor.getCatalogName("hive");
    Assertions.assertEquals(GravitinoHiveCatalogSpark33.class.getName(), hiveCatalogName);

    String icebergCatalogName = CatalogNameAdaptor.getCatalogName("lakehouse-iceberg");
    Assertions.assertEquals(GravitinoIcebergCatalogSpark33.class.getName(), icebergCatalogName);
  }
}
