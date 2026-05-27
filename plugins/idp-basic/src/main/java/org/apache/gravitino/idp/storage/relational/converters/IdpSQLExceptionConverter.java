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
package org.apache.gravitino.idp.storage.relational.converters;

import java.io.IOException;
import java.sql.SQLException;
import org.apache.gravitino.exceptions.AlreadyExistsException;

/** Converts JDBC SQL exceptions to built-in IdP exceptions. */
public final class IdpSQLExceptionConverter {

  /** MySQL duplicate entry error code. */
  private static final int MYSQL_DUPLICATE_ENTRY_ERROR_CODE = 1062;

  /** H2 duplicate entry error code. */
  private static final int H2_DUPLICATE_ENTRY_ERROR_CODE = 23505;

  /** PostgreSQL duplicate entry SQL state. */
  private static final String POSTGRESQL_DUPLICATE_ENTRY_SQL_STATE = "23505";

  /** Supported JDBC backend types for IdP relational storage. */
  public enum JdbcType {
    MYSQL,
    H2,
    POSTGRESQL
  }

  private final JdbcType jdbcType;

  /**
   * Creates a converter for the given JDBC backend type.
   *
   * @param jdbcType The JDBC backend type.
   */
  public IdpSQLExceptionConverter(JdbcType jdbcType) {
    this.jdbcType = jdbcType;
  }

  /**
   * Convert JDBC exception to IdP exception.
   *
   * @param sqlException The sql exception to map
   * @param resourceType The resource type, for example {@code user} or {@code group}
   * @param name The name of the resource
   * @throws IOException if an I/O error occurs during exception conversion
   */
  @SuppressWarnings("FormatStringAnnotation")
  public void toIdpException(SQLException sqlException, String resourceType, String name)
      throws IOException {
    if (isDuplicateEntry(sqlException)) {
      throw new AlreadyExistsException(
          sqlException, "IdP %s %s already exists", resourceType, name);
    }
    throw toIOException(sqlException);
  }

  private boolean isDuplicateEntry(SQLException sqlException) {
    switch (jdbcType) {
      case MYSQL:
        return sqlException.getErrorCode() == MYSQL_DUPLICATE_ENTRY_ERROR_CODE;
      case H2:
        // Same as core H2ExceptionConverter: H2 in MySQL mode may report 1062 or 23505.
        return sqlException.getErrorCode() == H2_DUPLICATE_ENTRY_ERROR_CODE
            || sqlException.getErrorCode() == MYSQL_DUPLICATE_ENTRY_ERROR_CODE;
      case POSTGRESQL:
        return POSTGRESQL_DUPLICATE_ENTRY_SQL_STATE.equals(sqlException.getSQLState());
      default:
        throw new IllegalStateException("Unsupported JDBC type: " + jdbcType);
    }
  }

  private IOException toIOException(SQLException sqlException) {
    if (jdbcType == JdbcType.H2) {
      return new IOException("error code: " + sqlException.getErrorCode(), sqlException);
    }
    return new IOException(sqlException);
  }
}
