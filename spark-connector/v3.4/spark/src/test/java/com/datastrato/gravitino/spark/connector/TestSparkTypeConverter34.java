/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.spark.connector;

import com.datastrato.gravitino.rel.types.Types.TimestampType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestSparkTypeConverter34 {
  private SparkTypeConverter sparkTypeConverter = new SparkTypeConverter34();

  @Test
  void testTimestampNTZ() {
    Assertions.assertEquals(
        TimestampType.withoutTimeZone(),
        sparkTypeConverter.toGravitinoType(DataTypes.TimestampNTZType));
    Assertions.assertEquals(
        DataTypes.TimestampNTZType,
        sparkTypeConverter.toSparkType(TimestampType.withoutTimeZone()));
  }
}
