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

package org.apache.gravitino.trino.connector.catalog.jdbc.postgresql;

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

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
