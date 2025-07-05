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

package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import static org.apache.gravitino.trino.connector.catalog.jdbc.mysql.MySQLDataTypeTransformer.JSON_TYPE;

import io.trino.spi.TrinoException;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
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

  @Test
  public void testGravitinoIntegerToTrinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new MySQLDataTypeTransformer();

    Type integerType = Types.IntegerType.get();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(integerType),
        io.trino.spi.type.IntegerType.INTEGER);

    Type unsignedIntegerType = Types.IntegerType.unsigned();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(unsignedIntegerType),
        io.trino.spi.type.BigintType.BIGINT);

    Type bigintType = Types.LongType.get();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(bigintType), io.trino.spi.type.BigintType.BIGINT);

    Type unsignBigintType = Types.LongType.unsigned();
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(unsignBigintType),
        io.trino.spi.type.DecimalType.createDecimalType(20, 0));
  }

  @Test
  public void testGravitinoExternalTypeToTrinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new MySQLDataTypeTransformer();

    Type mediumintType = Types.ExternalType.of("mediumint");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(mediumintType), IntegerType.INTEGER);

    Type mediumintUnsignedType = Types.ExternalType.of("mediumint unsigned");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(mediumintUnsignedType), IntegerType.INTEGER);

    Type floatUnsignedType = Types.ExternalType.of("float unsigned");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(floatUnsignedType), RealType.REAL);

    Type doubleUnsignedType = Types.ExternalType.of("double unsigned");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(doubleUnsignedType), DoubleType.DOUBLE);

    Type tinytextType = Types.ExternalType.of("tinytext");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(tinytextType), VarcharType.VARCHAR);

    Type mediumtextType = Types.ExternalType.of("mediumtext");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(mediumtextType), VarcharType.VARCHAR);

    Type longtextType = Types.ExternalType.of("longtext");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(longtextType), VarcharType.VARCHAR);

    Type yearType = Types.ExternalType.of("year");
    Assertions.assertEquals(generalDataTypeTransformer.getTrinoType(yearType), DateType.DATE);

    Type enumType = Types.ExternalType.of("enum");
    Assertions.assertEquals(generalDataTypeTransformer.getTrinoType(enumType), VarcharType.VARCHAR);

    Type setType = Types.ExternalType.of("set");
    Assertions.assertEquals(generalDataTypeTransformer.getTrinoType(setType), VarcharType.VARCHAR);

    Type jsonType = Types.ExternalType.of("json");
    Assertions.assertEquals(generalDataTypeTransformer.getTrinoType(jsonType), JSON_TYPE);

    Type varbinaryType = Types.ExternalType.of("varbinary");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(varbinaryType), VarbinaryType.VARBINARY);

    Type tinyblobType = Types.ExternalType.of("tinyblob");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(tinyblobType), VarbinaryType.VARBINARY);

    Type mediumblobType = Types.ExternalType.of("mediumblob");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(mediumblobType), VarbinaryType.VARBINARY);

    Type longblobType = Types.ExternalType.of("longblob");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(longblobType), VarbinaryType.VARBINARY);

    Type geometryType = Types.ExternalType.of("geometry");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(geometryType), VarbinaryType.VARBINARY);

    Type unknownType = Types.ExternalType.of("unknown");
    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(unknownType), VarcharType.VARCHAR);
  }
}
