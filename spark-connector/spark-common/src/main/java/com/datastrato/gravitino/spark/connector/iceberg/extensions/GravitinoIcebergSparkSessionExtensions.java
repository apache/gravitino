/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector.iceberg.extensions;

import org.apache.spark.sql.SparkSessionExtensions;
import scala.Function1;

public class GravitinoIcebergSparkSessionExtensions
    implements Function1<SparkSessionExtensions, Void> {

  @Override
  public Void apply(SparkSessionExtensions extensions) {

    // planner extensions
    extensions.injectPlannerStrategy(IcebergExtendedDataSourceV2Strategy::new);

    // There must be a return value, and Void only supports returning null, not other types.
    return null;
  }
}
