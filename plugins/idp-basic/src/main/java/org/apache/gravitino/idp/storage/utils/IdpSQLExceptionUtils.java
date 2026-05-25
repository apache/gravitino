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
package org.apache.gravitino.idp.storage.utils;

import java.sql.SQLException;
import org.apache.gravitino.idp.exception.AlreadyExistsException;

/** Utilities for translating JDBC exceptions in built-in IdP storage. */
public final class IdpSQLExceptionUtils {

  /** MySQL duplicate entry error code. */
  private static final int MYSQL_DUPLICATE_ENTRY_ERROR_CODE = 1062;

  /** H2 duplicate entry error code. */
  private static final int H2_DUPLICATE_ENTRY_ERROR_CODE = 23505;

  /** PostgreSQL duplicate entry SQL state. */
  private static final String POSTGRESQL_DUPLICATE_ENTRY_SQL_STATE = "23505";

  private IdpSQLExceptionUtils() {}

  /**
   * Converts duplicate-key SQL failures into {@link AlreadyExistsException}.
   *
   * @param re The runtime exception thrown by JDBC/MyBatis.
   * @param resourceType The resource type in the error message, for example {@code user}.
   * @param name The resource name.
   */
  public static void checkDuplicateEntry(RuntimeException re, String resourceType, String name) {
    SQLException sqlException = extractSQLException(re);
    if (sqlException != null && isDuplicateEntry(sqlException)) {
      throw new AlreadyExistsException("IdP %s %s already exists", resourceType, name);
    }
  }

  private static SQLException extractSQLException(Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof SQLException) {
        return (SQLException) current;
      }
      current = current.getCause();
    }
    return null;
  }

  private static boolean isDuplicateEntry(SQLException sqlException) {
    int errorCode = sqlException.getErrorCode();
    if (errorCode == MYSQL_DUPLICATE_ENTRY_ERROR_CODE
        || errorCode == H2_DUPLICATE_ENTRY_ERROR_CODE) {
      return true;
    }
    return POSTGRESQL_DUPLICATE_ENTRY_SQL_STATE.equals(sqlException.getSQLState());
  }
}
