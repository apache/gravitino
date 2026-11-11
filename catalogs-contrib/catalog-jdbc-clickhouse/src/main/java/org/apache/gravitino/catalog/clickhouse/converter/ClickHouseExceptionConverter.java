/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.clickhouse.converter;

import com.google.common.collect.ImmutableSet;
import java.sql.SQLException;
import java.util.Set;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.exceptions.BadRequestException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;

public class ClickHouseExceptionConverter extends JdbcExceptionConverter {
  static final int ERROR_CODE_UNKNOWN_DATABASE = 81;
  static final int ERROR_CODE_DATABASE_ALREADY_EXISTS = 82;

  static final int ERROR_CODE_TABLE_ALREADY_EXISTS = 57;
  static final int ERROR_CODE_TABLE_IS_DROPPED = 218;

  /**
   * ClickHouse error codes treated as request errors. BAD_ARGUMENTS (36) and ILLEGAL_COLUMN (44)
   * are intentionally included for invalid engine options and column definitions, although they can
   * also report server-side or generated-query failures. Without operation context, the same
   * mapping applies to metadata reads. Unknown identifiers (47) and syntax errors (62) remain
   * excluded because generated statements can also raise them.
   */
  private static final Set<Integer> INVALID_REQUEST_CODES =
      ImmutableSet.of(
          6, // CANNOT_PARSE_TEXT, for example a default that does not parse as the column type
          36, // BAD_ARGUMENTS, for example ORDER BY on an engine that does not support it
          42, // NUMBER_OF_ARGUMENTS_DOESNT_MATCH, for example an engine without its parameters
          44, // ILLEGAL_COLUMN, for example a duplicate column
          69, // ARGUMENT_OUT_OF_BOUND, for example a precision or scale out of range
          80); // INCORRECT_QUERY, for example an engine that cannot be created directly

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException sqlException) {
    int errorCode = sqlException.getErrorCode();
    switch (errorCode) {
      case ERROR_CODE_DATABASE_ALREADY_EXISTS:
        return new SchemaAlreadyExistsException(sqlException, sqlException.getMessage());
      case ERROR_CODE_TABLE_ALREADY_EXISTS:
        return new TableAlreadyExistsException(sqlException, sqlException.getMessage());
      case ERROR_CODE_UNKNOWN_DATABASE:
        return new NoSuchSchemaException(sqlException, sqlException.getMessage());
      case ERROR_CODE_TABLE_IS_DROPPED:
        return new NoSuchTableException(sqlException, sqlException.getMessage());
      default:
        if (INVALID_REQUEST_CODES.contains(errorCode)) {
          return new BadRequestException(sqlException, "%s", sqlException.getMessage());
        }
        return new GravitinoRuntimeException(sqlException, sqlException.getMessage());
    }
  }
}
