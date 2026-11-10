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
package org.apache.gravitino.catalog.clickhouse.converter;

import java.sql.SQLException;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestClickHouseExceptionConverter {

  private final ClickHouseExceptionConverter converter = new ClickHouseExceptionConverter();

  @ParameterizedTest
  @ValueSource(ints = {6, 36, 42, 44, 69, 80})
  void testRequestRefusedByClickHouseIsBadRequest(int errorCode) {
    SQLException failure =
        new SQLException("Storage Join requires at least 3 parameters", "HY000", errorCode);
    GravitinoRuntimeException error = converter.toGravitinoException(failure);
    Assertions.assertInstanceOf(BadRequestException.class, error);
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertEquals(failure.getMessage(), error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(ints = {47, 62})
  void testErrorsGeneratedStatementsCanRaiseStayServerErrors(int errorCode) {
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        converter
            .toGravitinoException(new SQLException("Missing columns: 'x'", "HY000", errorCode))
            .getClass());
  }

  @Test
  void testExistingMappingsAreKept() {
    Assertions.assertInstanceOf(
        NoSuchSchemaException.class,
        converter.toGravitinoException(new SQLException("Database x does not exist", "HY000", 81)));
  }
}
