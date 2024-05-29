/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.spark.connector.plugin;

import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

/** The entrypoint for Gravitino Spark connector. */
public class GravitinoSparkPlugin implements SparkPlugin {

  @Override
  public DriverPlugin driverPlugin() {
    return new GravitinoDriverPlugin();
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return null;
  }
}
