/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.iceberg;

import com.datastrato.gravitino.spark.connector.SparkTableChangeConverter;
import com.datastrato.gravitino.spark.connector.SparkTableChangeConverter34;
import com.datastrato.gravitino.spark.connector.SparkTypeConverter;
import com.datastrato.gravitino.spark.connector.SparkTypeConverter34;

public class GravitinoIcebergCatalogSpark34 extends GravitinoIcebergCatalog {
  @Override
  protected SparkTypeConverter getSparkTypeConverter() {
    return new SparkTypeConverter34();
  }

  @Override
  protected SparkTableChangeConverter getSparkTableChangeConverter(
      SparkTypeConverter sparkTypeConverter) {
    return new SparkTableChangeConverter34(sparkTypeConverter);
  }
}
