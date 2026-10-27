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
package org.apache.gravitino.catalog.generic.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.gravitino.catalog.generic.converter.GenericJdbcTypeConverter.GenericJdbcTypeBean;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

/** Tests for {@link GenericJdbcTypeConverter}. */
public class TestGenericJdbcTypeConverter {

  private final GenericJdbcTypeConverter converter = new GenericJdbcTypeConverter();

  @Test
  public void testStandardJdbcTypesToGravitino() {
    GenericJdbcTypeBean decimal = type("DECIMAL", java.sql.Types.DECIMAL);
    decimal.setColumnSize(10);
    decimal.setScale(2);

    GenericJdbcTypeBean varchar = type("VARCHAR", java.sql.Types.VARCHAR);
    varchar.setColumnSize(32);

    assertEquals(
        Types.BooleanType.get(), converter.toGravitino(type("BOOLEAN", java.sql.Types.BOOLEAN)));
    assertEquals(
        Types.IntegerType.get(), converter.toGravitino(type("INTEGER", java.sql.Types.INTEGER)));
    assertEquals(Types.DecimalType.of(10, 2), converter.toGravitino(decimal));
    assertEquals(Types.VarCharType.of(32), converter.toGravitino(varchar));
    assertEquals(
        Types.StringType.get(),
        converter.toGravitino(type("LONGVARCHAR", java.sql.Types.LONGVARCHAR)));
    assertEquals(Types.BinaryType.get(), converter.toGravitino(type("BLOB", java.sql.Types.BLOB)));
  }

  @Test
  public void testUnknownJdbcTypeFallsBackToExternalType() {
    assertEquals(
        Types.ExternalType.of("GEOMETRY"),
        converter.toGravitino(type("GEOMETRY", java.sql.Types.OTHER)));
  }

  private GenericJdbcTypeBean type(String typeName, int jdbcType) {
    return new GenericJdbcTypeBean(typeName, jdbcType);
  }
}
