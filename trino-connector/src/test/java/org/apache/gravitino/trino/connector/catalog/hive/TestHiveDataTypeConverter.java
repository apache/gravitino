/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.trino.connector.catalog.hive;

import io.trino.spi.TrinoException;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveDataTypeConverter {

  @Test
  public void testTrinoTypeToGravitinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new HiveDataTypeTransformer();
    io.trino.spi.type.Type charTypeWithLengthOne = io.trino.spi.type.CharType.createCharType(1);
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(charTypeWithLengthOne),
        Types.FixedCharType.of(1));

    io.trino.spi.type.Type charTypeWithLength = io.trino.spi.type.CharType.createCharType(255);
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(charTypeWithLength),
        Types.FixedCharType.of(255));

    io.trino.spi.type.Type charLengthIsOverflow = io.trino.spi.type.CharType.createCharType(256);
    Exception e =
        Assert.assertThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(charLengthIsOverflow));
    Assertions.assertTrue(
        e.getMessage()
            .contains("Hive does not support the datatype CHAR with the length greater than 255"));

    io.trino.spi.type.Type varcharType = io.trino.spi.type.VarcharType.createVarcharType(1);
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharType), Types.VarCharType.of(1));

    io.trino.spi.type.Type varcharTypeWithLength =
        io.trino.spi.type.VarcharType.createVarcharType(65535);
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithLength),
        Types.VarCharType.of(65535));

    io.trino.spi.type.Type varcharLengthIsOverflow =
        io.trino.spi.type.VarcharType.createVarcharType(65536);
    e =
        Assert.assertThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(varcharLengthIsOverflow));
    Assertions.assertTrue(
        e.getMessage()
            .contains(
                "Hive does not support the datatype VARCHAR with the length greater than 65535"));

    io.trino.spi.type.Type varcharTypeWithoutLength =
        io.trino.spi.type.VarcharType.createUnboundedVarcharType();

    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithoutLength),
        Types.StringType.get());
  }
}
