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
package org.apache.gravitino.storage.relational.converters;

import java.io.IOException;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.util.Locale;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception converter to Apache Gravitino exception for MySQL. The definition of error codes can be
 * found in the document: <a
 * href="https://dev.mysql.com/doc/connector-j/en/connector-j-reference-error-sqlstates.html"></a>
 */
public class MySQLExceptionConverter implements SQLExceptionConverter {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLExceptionConverter.class);

  /** It means found a duplicated primary key or unique key entry in MySQL. */
  static final int DUPLICATED_ENTRY_ERROR_CODE = 1062;

  /** It means a value is too long for its column in MySQL. */
  static final int DATA_TOO_LONG_ERROR_CODE = 1406;

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public void toGravitinoException(SQLException se, Entity.EntityType type, String name)
      throws IOException {
    // Without the strict SQL mode, MySQL truncates the value with a warning, which Connector/J
    // reports as a DataTruncation with error code 1265 rather than the 1406 error.
    if (se instanceof DataTruncation) {
      throw valueTooLong(se, type, name);
    }

    switch (se.getErrorCode()) {
      case DUPLICATED_ENTRY_ERROR_CODE:
        throw new EntityAlreadyExistsException(
            se, "The %s entity: %s already exists.", type.name(), name);
      case DATA_TOO_LONG_ERROR_CODE:
        throw valueTooLong(se, type, name);
      default:
        throw new IOException(se);
    }
  }

  private static IllegalArgumentException valueTooLong(
      SQLException se, Entity.EntityType type, String name) {
    // Do not attach the SQL exception as the cause, it would expose the database error message to
    // the client through the stack trace of the error response.
    LOG.warn("Failed to persist the {} entity: {}", type, name, se);
    return new IllegalArgumentException(
        String.format(
            "The %s entity has a value that exceeds the maximum length of its column.",
            type.name().toLowerCase(Locale.ROOT)));
  }
}
