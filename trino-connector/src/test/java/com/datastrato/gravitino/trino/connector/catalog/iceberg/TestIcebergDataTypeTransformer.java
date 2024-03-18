/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.iceberg;

import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import io.trino.spi.TrinoException;
import io.trino.spi.type.VarcharType;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestIcebergDataTypeTransformer {

  @Test
  public void testTrinoTypeToGravitinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new IcebergDataTypeTransformer();
    io.trino.spi.type.Type charTypeWithLengthOne = io.trino.spi.type.CharType.createCharType(1);

    Exception e =
        Assert.expectThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(charTypeWithLengthOne));
    Assert.assertTrue(e.getMessage().contains("Gravitino does not support the datatype CHAR"));

    io.trino.spi.type.Type varcharType = io.trino.spi.type.VarcharType.createVarcharType(1);
    e =
        Assert.expectThrows(
            TrinoException.class, () -> generalDataTypeTransformer.getGravitinoType(varcharType));
    Assert.assertTrue(
        e.getMessage().contains("Gravitino does not support the datatype VARCHAR with length"));

    io.trino.spi.type.Type varcharTypeWithoutLength = VarcharType.VARCHAR;

    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithoutLength),
        Types.StringType.get());
  }
}
