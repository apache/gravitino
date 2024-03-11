/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPostgreSQLDataTypeTransformer {

  @Test
  public void testTrinoTypeToGravitinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new PostgreSQLDataTypeTransformer();
    io.trino.spi.type.Type charTypeWithLengthOne = io.trino.spi.type.CharType.createCharType(1);
    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(charTypeWithLengthOne),
        Types.FixedCharType.of(1));

    io.trino.spi.type.Type charTypeWithLength = io.trino.spi.type.CharType.createCharType(65536);
    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(charTypeWithLength),
        Types.FixedCharType.of(65536));

    io.trino.spi.type.Type varcharType = io.trino.spi.type.VarcharType.createVarcharType(1);
    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharType), Types.VarCharType.of(1));

    io.trino.spi.type.Type varcharTypeWithLength =
        io.trino.spi.type.VarcharType.createVarcharType(65536);

    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithLength),
        Types.VarCharType.of(65536));

    io.trino.spi.type.Type varcharTypeWithLength2 =
        io.trino.spi.type.VarcharType.createUnboundedVarcharType();
    Assert.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithLength2),
        Types.StringType.get());
  }

  @Test
  public void testGravitinoCharToTrinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new PostgreSQLDataTypeTransformer();
    Type stringType = Types.StringType.get();
    Assert.assertEquals(
        generalDataTypeTransformer.getTrinoType(stringType),
        io.trino.spi.type.VarcharType.createUnboundedVarcharType());
  }
}
