/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.hive;

import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import io.trino.spi.TrinoException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHiveDataTypeConverter {

  @Test
  public void testTrinoTypeToGravitinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new HiveDataTypeTransformer();
    io.trino.spi.type.Type charTypeWithLengthOne = io.trino.spi.type.CharType.createCharType(1);
    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(charTypeWithLengthOne),
        Types.FixedCharType.of(1));

    io.trino.spi.type.Type charTypeWithLength = io.trino.spi.type.CharType.createCharType(255);
    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(charTypeWithLength),
        Types.FixedCharType.of(255));

    io.trino.spi.type.Type charLengthIsOverflow = io.trino.spi.type.CharType.createCharType(256);
    Exception e =
        Assert.expectThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(charLengthIsOverflow));
    Assert.assertTrue(
        e.getMessage()
            .contains("Hive does not support the datatype CHAR with the length greater than 255"));

    io.trino.spi.type.Type varcharType = io.trino.spi.type.VarcharType.createVarcharType(1);
    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharType), Types.VarCharType.of(1));

    io.trino.spi.type.Type varcharTypeWithLength =
        io.trino.spi.type.VarcharType.createVarcharType(65535);
    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithLength),
        Types.VarCharType.of(65535));

    io.trino.spi.type.Type varcharLengthIsOverflow =
        io.trino.spi.type.VarcharType.createVarcharType(65536);
    e =
        Assert.expectThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(varcharLengthIsOverflow));
    Assert.assertTrue(
        e.getMessage()
            .contains(
                "Hive does not support the datatype VARCHAR with the length greater than 65535"));

    io.trino.spi.type.Type varcharTypeWithoutLength =
        io.trino.spi.type.VarcharType.createUnboundedVarcharType();

    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithoutLength),
        Types.StringType.get());
  }
}
