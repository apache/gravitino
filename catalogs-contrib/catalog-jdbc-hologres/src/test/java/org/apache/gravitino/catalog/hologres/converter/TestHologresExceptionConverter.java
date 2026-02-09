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
package org.apache.gravitino.catalog.hologres.converter;

import java.sql.SQLException;

import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link HologresExceptionConverter}. */
public class TestHologresExceptionConverter {

  private final HologresExceptionConverter converter = new HologresExceptionConverter();

  @Test
  public void testDuplicateDatabase() {
    SQLException se = new SQLException("database already exists", "42P04");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(SchemaAlreadyExistsException.class, result);
  }

  @Test
  public void testDuplicateSchema() {
    SQLException se = new SQLException("schema already exists", "42P06");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(SchemaAlreadyExistsException.class, result);
  }

  @Test
  public void testDuplicateTable() {
    SQLException se = new SQLException("table already exists", "42P07");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(TableAlreadyExistsException.class, result);
  }

  @Test
  public void testInvalidSchemaName() {
    SQLException se = new SQLException("invalid schema name", "3D000");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(NoSuchSchemaException.class, result);
  }

  @Test
  public void testInvalidSchema() {
    SQLException se = new SQLException("invalid schema", "3F000");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(NoSuchSchemaException.class, result);
  }

  @Test
  public void testUndefinedTable() {
    SQLException se = new SQLException("undefined table", "42P01");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(NoSuchTableException.class, result);
  }

  @Test
  public void testConnectionException() {
    SQLException se = new SQLException("connection refused", "08001");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(ConnectionFailedException.class, result);
  }

  @Test
  public void testConnectionExceptionOtherSubclass() {
    SQLException se = new SQLException("connection broken", "08006");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(ConnectionFailedException.class, result);
  }

  @Test
  public void testUnknownSqlState() {
    SQLException se = new SQLException("some error", "99999");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(GravitinoRuntimeException.class, result);
    Assertions.assertFalse(result instanceof ConnectionFailedException);
    Assertions.assertFalse(result instanceof SchemaAlreadyExistsException);
    Assertions.assertFalse(result instanceof TableAlreadyExistsException);
    Assertions.assertFalse(result instanceof NoSuchSchemaException);
    Assertions.assertFalse(result instanceof NoSuchTableException);
  }

  @Test
  public void testNullSqlState() {
    SQLException se = new SQLException("some error");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(GravitinoRuntimeException.class, result);
  }

  @Test
  public void testNullSqlStateWithPasswordAuthenticationFailed() {
    SQLException se = new SQLException("password authentication failed for user");
    GravitinoRuntimeException result = converter.toGravitinoException(se);
    Assertions.assertInstanceOf(ConnectionFailedException.class, result);
  }

  @Test
  public void testNullSqlStateWithNullMessage() {
    // GravitinoRuntimeException uses String.format internally, so null message causes NPE.
    // This verifies the converter's behavior with null message and null SQL state.
    SQLException se = new SQLException((String) null);
    Assertions.assertThrows(NullPointerException.class, () -> converter.toGravitinoException(se));
  }
}
