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
package org.apache.gravitino.catalog.clickhouse.converter;

import java.sql.SQLException;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;

/** Exception converter to Apache Gravitino exception for ClickHouse. */
public class ClickHouseExceptionConverter extends JdbcExceptionConverter {

  // see: SELECT concat('\t', name, ' = ', toString(number))
  // FROM
  // (
  //    SELECT
  //        number,
  //        errorCodeToName(number) AS name
  //    FROM system.numbers
  //    LIMIT 2000
  // )
  // WHERE NOT empty(errorCodeToName(number))
  static final int UNKNOWN_DATABASE = 81;
  static final int DATABASE_ALREADY_EXISTS = 82;

  static final int TABLE_ALREADY_EXISTS = 57;
  static final int TABLE_IS_DROPPED = 218;

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException sqlException) {
    int errorCode = sqlException.getErrorCode();
    switch (errorCode) {
      case DATABASE_ALREADY_EXISTS:
        return new SchemaAlreadyExistsException(sqlException, sqlException.getMessage());
      case TABLE_ALREADY_EXISTS:
        return new TableAlreadyExistsException(sqlException, sqlException.getMessage());
      case UNKNOWN_DATABASE:
        return new NoSuchSchemaException(sqlException, sqlException.getMessage());
      case TABLE_IS_DROPPED:
        return new NoSuchTableException(sqlException, sqlException.getMessage());
      default:
        return new GravitinoRuntimeException(sqlException, sqlException.getMessage());
    }
  }
}
