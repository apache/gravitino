/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.integration.test.iceberg;

import com.datastrato.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark33;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkIcebergCatalogHiveBackendIT33 extends SparkIcebergCatalogHiveBackendIT {
  @Test
  void testCatalogClassName() {
    String catalogClass =
        getSparkSession()
            .sessionState()
            .conf()
            .getConfString("spark.sql.catalog." + getCatalogName());
    Assertions.assertEquals(GravitinoIcebergCatalogSpark33.class.getName(), catalogClass);
  }
}
