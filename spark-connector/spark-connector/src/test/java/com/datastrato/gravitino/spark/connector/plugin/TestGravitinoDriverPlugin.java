/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.plugin;

import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoDriverPlugin {

  @Test
  void testIcebergExtensionName() {
    Assertions.assertEquals(
        IcebergSparkSessionExtensions.class.getName(),
        GravitinoDriverPlugin.ICEBERG_SPARK_EXTENSIONS);
  }
}
