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
import java.sql.SQLException;
import java.util.Locale;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception converter to Apache Gravitino exception for H2. The definition of error codes can be
 * found in the document: <a href="https://h2database.com/javadoc/org/h2/api/ErrorCode.html"></a>
 */
public class H2ExceptionConverter implements SQLExceptionConverter {
  private static final Logger LOG = LoggerFactory.getLogger(H2ExceptionConverter.class);

  /** It means found a duplicated primary key or unique key entry in H2. */
  private static final int DUPLICATED_ENTRY_ERROR_CODE = 23505;

  /** It means a value is too long for its column in H2. */
  private static final int VALUE_TOO_LONG_ERROR_CODE = 22001;

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public void toGravitinoException(SQLException se, Entity.EntityType type, String name)
      throws IOException {
    switch (se.getErrorCode()) {
      case DUPLICATED_ENTRY_ERROR_CODE:
        // compatible with H2 in MySQL mode
      case MySQLExceptionConverter.DUPLICATED_ENTRY_ERROR_CODE:
        throw new EntityAlreadyExistsException(
            se, "The %s entity: %s already exists.", type.name(), name);
      case VALUE_TOO_LONG_ERROR_CODE:
        // compatible with H2 in MySQL mode
      case MySQLExceptionConverter.DATA_TOO_LONG_ERROR_CODE:
        // Do not attach the SQL exception as the cause, it would expose the database error
        // message to the client through the stack trace of the error response.
        LOG.warn("Failed to persist the {} entity: {}", type, name, se);
        throw new IllegalArgumentException(
            String.format(
                "The %s entity has a value that exceeds the maximum length of its column.",
                type.name().toLowerCase(Locale.ROOT)));
      default:
        throw new IOException("error code: " + se.getErrorCode(), se);
    }
  }
}
