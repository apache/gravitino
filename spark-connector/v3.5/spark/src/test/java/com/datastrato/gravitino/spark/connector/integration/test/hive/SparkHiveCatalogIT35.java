/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.integration.test.hive;

import com.datastrato.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark35;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkHiveCatalogIT35 extends SparkHiveCatalogIT {
  @Test
  void testCatalogClassName() {
    String catalogClass =
        getSparkSession()
            .sessionState()
            .conf()
            .getConfString("spark.sql.catalog." + getCatalogName());
    Assertions.assertEquals(GravitinoHiveCatalogSpark35.class.getName(), catalogClass);
  }
}
