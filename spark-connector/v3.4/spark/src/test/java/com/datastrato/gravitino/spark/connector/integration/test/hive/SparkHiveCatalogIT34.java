/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.integration.test.hive;

import com.datastrato.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark34;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SparkHiveCatalogIT34 extends SparkHiveCatalogIT {
  @Test
  void testCatalogClassName() {
    String catalogClass =
        getSparkSession()
            .sessionState()
            .conf()
            .getConfString("spark.sql.catalog." + getCatalogName());
    Assertions.assertEquals(GravitinoHiveCatalogSpark34.class.getName(), catalogClass);
  }
}
