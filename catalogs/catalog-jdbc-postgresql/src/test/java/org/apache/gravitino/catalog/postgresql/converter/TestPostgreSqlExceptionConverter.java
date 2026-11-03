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
package org.apache.gravitino.catalog.postgresql.converter;

import java.sql.SQLException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/** PostgreSQL source permission errors remain distinct from missing objects and outages. */
public class TestPostgreSqlExceptionConverter {
  @SuppressWarnings("OrphanedFormatString") // Source messages are literal, not format templates.
  @ParameterizedTest
  @NullSource
  @ValueSource(
      strings = {
        "42501", "42P04", "42P06", "42P07", "3D000", "3F000", "42P01", "08006", "08P01", "XX000"
      })
  void testSqlFailuresPreserveCauseAndLiteralMessage(String state) {
    SQLException failure = new SQLException("source failure containing literal %s", state);
    Exception error = new PostgreSqlExceptionConverter().toGravitinoException(failure);
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertEquals(failure.getMessage(), error.getMessage());
  }

  @SuppressWarnings("OrphanedFormatString") // Source messages are literal, not format templates.
  @Test
  void testAuthenticationFailureWithoutSqlStatePreservesCause() {
    SQLException failure = new SQLException("password authentication failed for role %s");
    Exception error = new PostgreSqlExceptionConverter().toGravitinoException(failure);
    Assertions.assertInstanceOf(ConnectionFailedException.class, error);
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertEquals(failure.getMessage(), error.getMessage());
  }

  @Test
  void testInsufficientPrivilegePreservesCauseAndReason() {
    SQLException failure = new SQLException("permission denied for schema restricted", "42501");
    Exception error = new PostgreSqlExceptionConverter().toGravitinoException(failure);
    Assertions.assertInstanceOf(ForbiddenException.class, error);
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertEquals(failure.getMessage(), error.getMessage());
  }

  @Test
  void testMissingSchemaAndConnectionFailuresRemainDistinct() {
    PostgreSqlExceptionConverter converter = new PostgreSqlExceptionConverter();
    Assertions.assertInstanceOf(
        NoSuchSchemaException.class,
        converter.toGravitinoException(new SQLException("schema missing", "3F000")));
    for (String state : new String[] {"08006", "08P01"}) {
      Assertions.assertInstanceOf(
          ConnectionFailedException.class,
          converter.toGravitinoException(
              new SQLException("connection or protocol failure", state)));
    }
  }
}
