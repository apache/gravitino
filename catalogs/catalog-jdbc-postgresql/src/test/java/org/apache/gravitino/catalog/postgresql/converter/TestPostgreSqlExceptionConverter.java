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
import java.time.Duration;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
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

  @Test
  void testPoolWrappedConnectionFailureUsesNestedSqlState() {
    SQLException driverFailure = new SQLException("The connection attempt failed.", "08001");
    SQLException poolFailure =
        new SQLException("Cannot create PoolableConnectionFactory (connect failed)", driverFailure);
    Exception error = new PostgreSqlExceptionConverter().toGravitinoException(poolFailure);
    Assertions.assertInstanceOf(ConnectionFailedException.class, error);
    Assertions.assertSame(poolFailure, error.getCause());
    Assertions.assertEquals(poolFailure.getMessage(), error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"28P01", "28000"})
  void testRejectedLoginIsConnectionFailure(String state) {
    SQLException failure = new SQLException("password authentication failed for user", state);
    Exception error = new PostgreSqlExceptionConverter().toGravitinoException(failure);
    Assertions.assertInstanceOf(ConnectionFailedException.class, error);
    Assertions.assertSame(failure, error.getCause());
  }

  @ParameterizedTest
  @ValueSource(strings = {"2BP01", "42601", "42703", "42804", "23502", "23505", "22003", "22P02"})
  void testRequestsRefusedBySourceAreBadRequests(String state) {
    SQLException failure = new SQLException("source refused the request", state);
    Exception error = new PostgreSqlExceptionConverter().toGravitinoException(failure);
    Assertions.assertInstanceOf(BadRequestException.class, error);
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertEquals(failure.getMessage(), error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"XX000", "53300", "57014"})
  void testServerSideFailuresRemainGeneric(String state) {
    SQLException failure = new SQLException("server side failure", state);
    Exception error = new PostgreSqlExceptionConverter().toGravitinoException(failure);
    Assertions.assertEquals(GravitinoRuntimeException.class, error.getClass());
  }

  @Test
  void testCauseCycleWithoutSqlStateEndsTheWalk() {
    SQLException failure = new SQLException("source failure");
    RuntimeException wrapper = new RuntimeException("wrapper");
    failure.initCause(wrapper);
    wrapper.initCause(failure);
    Exception error = convertWithinTimeout(failure);
    Assertions.assertEquals(GravitinoRuntimeException.class, error.getClass());
    Assertions.assertSame(failure, error.getCause());
  }

  @Test
  void testCauseCycleStillFindsNestedSqlState() {
    SQLException failure = new SQLException("pool failure");
    SQLException driverFailure = new SQLException("permission denied", "42501");
    failure.initCause(driverFailure);
    driverFailure.initCause(failure);
    Exception error = convertWithinTimeout(failure);
    Assertions.assertInstanceOf(ForbiddenException.class, error);
    Assertions.assertSame(failure, error.getCause());
  }

  // A regression loops without checking interruption, so the conversion must not run on the test
  // thread.
  private static Exception convertWithinTimeout(SQLException failure) {
    return Assertions.assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> new PostgreSqlExceptionConverter().toGravitinoException(failure));
  }
}
