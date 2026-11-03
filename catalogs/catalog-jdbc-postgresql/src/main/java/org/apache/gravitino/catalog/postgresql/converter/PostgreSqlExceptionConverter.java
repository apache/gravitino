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
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;

public class PostgreSqlExceptionConverter extends JdbcExceptionConverter {

  private static final String DUPLICATE_DATABASE = "42P04";
  private static final String DUPLICATE_SCHEMA = "42P06";
  private static final String DUPLICATE_TABLE = "42P07";
  private static final String INVALID_SCHEMA_NAME = "3D000";
  private static final String INVALID_SCHEMA = "3F000";
  private static final String UNDEFINED_TABLE = "42P01";
  private static final String INSUFFICIENT_PRIVILEGE = "42501";

  /**
   * SQLSTATE '08' is the class code of connection exceptions See <a
   * href="https://www.postgresql.org/docs/current/errcodes-appendix.html#ERRCODES-TABLE">PostgreSQL
   * errcodes appendix</a>.
   */
  private static final String CONNECTION_EXCEPTION = "08";

  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException se) {
    if (null != se.getSQLState()) {
      switch (se.getSQLState()) {
        case INSUFFICIENT_PRIVILEGE:
          return new ForbiddenException(se, "%s", se.getMessage());
        case DUPLICATE_DATABASE:
        case DUPLICATE_SCHEMA:
          return new SchemaAlreadyExistsException(se, "%s", se.getMessage());
        case DUPLICATE_TABLE:
          return new TableAlreadyExistsException(se, "%s", se.getMessage());
        case INVALID_SCHEMA_NAME:
        case INVALID_SCHEMA:
          return new NoSuchSchemaException(se, "%s", se.getMessage());
        case UNDEFINED_TABLE:
          return new NoSuchTableException(se, "%s", se.getMessage());
        default:
          {
            if (se.getSQLState().startsWith(CONNECTION_EXCEPTION)) {
              return new ConnectionFailedException(se, "%s", se.getMessage());
            }
            return new GravitinoRuntimeException(se, "%s", se.getMessage());
          }
      }
    } else {
      if (se.getMessage() != null && se.getMessage().contains("password authentication failed")) {
        return new ConnectionFailedException(se, "%s", se.getMessage());
      }
      return new GravitinoRuntimeException(se, "%s", se.getMessage());
    }
  }
}
