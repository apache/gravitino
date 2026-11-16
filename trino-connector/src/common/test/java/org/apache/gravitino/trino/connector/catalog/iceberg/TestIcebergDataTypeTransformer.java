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

package org.apache.gravitino.trino.connector.catalog.iceberg;

import io.trino.spi.TrinoException;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIcebergDataTypeTransformer {

  @Test
  public void testTrinoTypeToGravitinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer = new IcebergDataTypeTransformer();
    io.trino.spi.type.Type charTypeWithLengthOne = io.trino.spi.type.CharType.createCharType(1);

    Exception e =
        Assertions.assertThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(charTypeWithLengthOne));
    Assertions.assertTrue(e.getMessage().contains("Iceberg does not support the datatype CHAR"));

    io.trino.spi.type.Type varcharType = io.trino.spi.type.VarcharType.createVarcharType(1);
    e =
        Assertions.assertThrows(
            TrinoException.class, () -> generalDataTypeTransformer.getGravitinoType(varcharType));
    Assertions.assertTrue(
        e.getMessage().contains("Iceberg does not support the datatype VARCHAR with length"));

    io.trino.spi.type.Type varcharTypeWithoutLength = VarcharType.VARCHAR;

    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithoutLength),
        Types.StringType.get());

    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(Types.TimeType.get()), TimeType.TIME_MICROS);

    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(Types.TimestampType.withoutTimeZone()),
        TimestampType.TIMESTAMP_MICROS);

    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(Types.TimestampType.withTimeZone()),
        TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS);
  }
}
