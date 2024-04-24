/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test.util.spark;

import org.apache.spark.sql.connector.catalog.MetadataColumn;
import org.apache.spark.sql.types.DataType;

public class SparkMetadataColumnInfo implements MetadataColumn {
  private final String name;
  private final DataType dataType;
  private final boolean isNullable;

  public SparkMetadataColumnInfo(String name, DataType dataType, boolean isNullable) {
    this.name = name;
    this.dataType = dataType;
    this.isNullable = isNullable;
  }

  public String name() {
    return this.name;
  }

  public DataType dataType() {
    return this.dataType;
  }

  public boolean isNullable() {
    return this.isNullable;
  }
}
