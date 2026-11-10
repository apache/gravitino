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

import com.google.common.collect.ImmutableSet;
import java.sql.SQLException;
import java.util.Set;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.ServiceUnavailableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;

/** Interface for converter JDBC exceptions to Gravitino exceptions. */
public class JdbcExceptionConverter {

  /**
   * MySQL error codes for DDL the source refuses because of the request itself: a definition it
   * cannot create, a value or default that does not fit, or data that does not allow the change.
   * Unknown column (1054) and syntax errors (1064) are left out because generated statements can
   * also raise them.
   */
  private static final Set<Integer> INVALID_REQUEST_CODES =
      ImmutableSet.of(
          1059, // identifier name is too long
          1060, // duplicate column name
          1061, // duplicate key name
          1062, // duplicate entry, for example a unique key on duplicate values
          1063, // incorrect column specifier, for example AUTO_INCREMENT on a string column
          1067, // invalid default value
          1068, // multiple primary key defined
          1072, // key column doesn't exist in table
          1074, // column length too big
          1075, // incorrect table definition; only one auto column, which must be a key
          1091, // can't drop; check that the column or key exists
          1101, // BLOB, TEXT, GEOMETRY or JSON column can't have a default value
          1113, // a table must have at least 1 column
          1117, // too many columns
          1118, // row size too large
          1138, // invalid use of NULL value, for example NOT NULL on a column with NULLs
          1166, // incorrect column name
          1170, // BLOB/TEXT column used in key specification without a key length
          1171, // all parts of a PRIMARY KEY must be NOT NULL
          1264, // out of range value
          1265, // data truncated
          1280, // incorrect index name
          1366, // incorrect value for the column
          1406, // data too long for column
          1425, // too big scale
          1426, // too big precision
          3730, // cannot drop a table referenced by a foreign key constraint
          3780); // referencing and referenced columns in a foreign key are incompatible

  /** A lock wait timed out or a deadlock was detected; the request can be retried. */
  private static final Set<Integer> BUSY_CODES = ImmutableSet.of(1205, 1213);

  /**
   * Convert JDBC exception to GravitinoException. The default implementation of this method is
   * based on MySQL exception code, and if the catalog does not compatible with MySQL exception
   * code, this method needs to be rewritten.
   *
   * @param sqlException The sql exception to map
   * @return The best attempt at a corresponding connector exception or generic with the
   *     SQLException as the cause
   */
  public GravitinoRuntimeException toGravitinoException(final SQLException sqlException) {
    switch (sqlException.getErrorCode()) {
      case 1007:
        return new SchemaAlreadyExistsException(sqlException, "%s", sqlException.getMessage());
      case 1050:
        return new TableAlreadyExistsException(sqlException, "%s", sqlException.getMessage());
      case 1008:
      case 1049:
        return new NoSuchSchemaException(sqlException, "%s", sqlException.getMessage());
      case 1146:
      case 1051:
        return new NoSuchTableException(sqlException, "%s", sqlException.getMessage());
      default:
        if (INVALID_REQUEST_CODES.contains(sqlException.getErrorCode())) {
          return new BadRequestException(sqlException, "%s", sqlException.getMessage());
        }
        if (BUSY_CODES.contains(sqlException.getErrorCode())) {
          return new ServiceUnavailableException(sqlException, "%s", sqlException.getMessage());
        }
        if (sqlException.getMessage() != null
            && sqlException.getMessage().contains("Access denied")) {
          return new ConnectionFailedException(sqlException, "%s", sqlException.getMessage());
        }
        return new GravitinoRuntimeException(sqlException, "%s", sqlException.getMessage());
    }
  }
}
