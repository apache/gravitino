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
package org.apache.gravitino.catalog.jdbc.converter;

import java.sql.SQLException;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.ServiceUnavailableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Source messages are literal text, not format templates. */
public class TestJdbcExceptionConverter {

  @Test
  void testAccessDeniedForAnyHostKeepsSourceMessage() {
    SQLException failure =
        new SQLException("Access denied for user 'gravitino'@'%' to database 'db'", "42000", 1044);
    GravitinoRuntimeException error = new JdbcExceptionConverter().toGravitinoException(failure);
    Assertions.assertInstanceOf(ConnectionFailedException.class, error);
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertEquals(failure.getMessage(), error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(ints = {1007, 1050, 1049, 1146, 0})
  void testEveryBranchKeepsPercentSignsLiteral(int errorCode) {
    SQLException failure = new SQLException("object 'a%'b' failed at 100%", "HY000", errorCode);
    GravitinoRuntimeException error = new JdbcExceptionConverter().toGravitinoException(failure);
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertEquals(failure.getMessage(), error.getMessage());
  }

  @Test
  void testMissingSchemaIsStillClassified() {
    Assertions.assertInstanceOf(
        NoSuchSchemaException.class,
        new JdbcExceptionConverter()
            .toGravitinoException(new SQLException("Unknown database 'x%'", "42000", 1049)));
  }

  @ParameterizedTest
  @ValueSource(
      ints = {
        1059, 1060, 1061, 1062, 1063, 1067, 1068, 1072, 1074, 1075, 1091, 1101, 1113, 1117, 1118,
        1138, 1166, 1170, 1171, 1264, 1265, 1280, 1366, 1406, 1425, 1426, 3730, 3780
      })
  void testRequestRefusedBySourceIsBadRequest(int errorCode) {
    SQLException failure = new SQLException("refused at 100%", "42000", errorCode);
    GravitinoRuntimeException error = new JdbcExceptionConverter().toGravitinoException(failure);
    Assertions.assertInstanceOf(BadRequestException.class, error);
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertEquals(failure.getMessage(), error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(ints = {1205, 1213})
  void testLockTimeoutAndDeadlockAreServiceUnavailable(int errorCode) {
    Assertions.assertInstanceOf(
        ServiceUnavailableException.class,
        new JdbcExceptionConverter()
            .toGravitinoException(
                new SQLException("Lock wait timeout exceeded", "HY000", errorCode)));
  }

  @ParameterizedTest
  @ValueSource(ints = {1054, 1064})
  void testErrorsGeneratedStatementsCanRaiseStayServerErrors(int errorCode) {
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        new JdbcExceptionConverter()
            .toGravitinoException(new SQLException("Unknown column 'x'", "42S22", errorCode))
            .getClass());
  }
}
