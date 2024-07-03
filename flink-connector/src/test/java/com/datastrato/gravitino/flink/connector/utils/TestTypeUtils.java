/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.utils;

import com.datastrato.gravitino.rel.types.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTypeUtils {

  @Test
  public void testToGravitinoType() {
    Assertions.assertEquals(
        Types.StringType.get(), TypeUtils.toGravitinoType(new VarCharType(Integer.MAX_VALUE)));
    Assertions.assertEquals(Types.DoubleType.get(), TypeUtils.toGravitinoType(new DoubleType()));
    Assertions.assertEquals(Types.IntegerType.get(), TypeUtils.toGravitinoType(new IntType()));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            TypeUtils.toGravitinoType(
                new UnresolvedUserDefinedType(UnresolvedIdentifier.of("a", "b", "c"))));
  }

  @Test
  public void testToFlinkType() {
    Assertions.assertEquals(DataTypes.DOUBLE(), TypeUtils.toFlinkType(Types.StringType.get()));
    Assertions.assertEquals(DataTypes.STRING(), TypeUtils.toFlinkType(Types.DoubleType.get()));
    Assertions.assertEquals(DataTypes.INT(), TypeUtils.toFlinkType(Types.IntegerType.get()));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> TypeUtils.toFlinkType(Types.UnparsedType.of("unknown")));
  }
}
