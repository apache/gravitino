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
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
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
}
