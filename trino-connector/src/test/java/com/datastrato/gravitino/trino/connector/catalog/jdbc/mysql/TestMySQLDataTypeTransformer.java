/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import com.datastrato.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMySQLDataTypeTransformer {

  @Test
  public void testTrinoTypeToGravitinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new MySQLDataTypeTransformer();
    io.trino.spi.type.Type charTypeWithLengthOne = io.trino.spi.type.CharType.createCharType(1);
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(charTypeWithLengthOne),
        Types.FixedCharType.of(1));

    io.trino.spi.type.Type charTypeWithLength = io.trino.spi.type.CharType.createCharType(256);
    Exception e =
        Assertions.assertThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(charTypeWithLength));
    Assertions.assertTrue(
        e.getMessage()
            .contains("MySQL does not support the datatype CHAR with the length greater than 255"));

    io.trino.spi.type.Type varcharType = io.trino.spi.type.VarcharType.createVarcharType(1);
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharType), Types.VarCharType.of(1));

    io.trino.spi.type.Type varcharTypeWithLength =
        io.trino.spi.type.VarcharType.createVarcharType(16384);
    e =
        Assertions.assertThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(varcharTypeWithLength));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "MySQL does not support the datatype VARCHAR with the length greater than 16383"));

    io.trino.spi.type.Type varcharTypeWithLength2 =
        io.trino.spi.type.VarcharType.createUnboundedVarcharType();
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithLength2),
        Types.StringType.get());
  }

  @Test
  public void testGravitinoCharToTrinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new MySQLDataTypeTransformer();

    Type stringType = Types.StringType.get();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(stringType),
        io.trino.spi.type.VarcharType.createUnboundedVarcharType());
  }
}
