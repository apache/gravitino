/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.spark.hive;

import com.datastrato.gravitino.integration.test.spark.SparkCommonIT;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@Tag("gravitino-docker-it")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkHiveCatalogIT extends SparkCommonIT {

  @Override
  protected String getCatalogName() {
    return "hive";
  }

  @Override
  protected String getProvider() {
    return "hive";
  }

  @Override
  protected String getUsingClause() {
    return "USING PARQUET";
  }
}
