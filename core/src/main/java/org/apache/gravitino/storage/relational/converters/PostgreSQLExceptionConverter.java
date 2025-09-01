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
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;

/**
 * Exception converter to Apache Gravitino exception for PostgreSQL. The definition of error codes
 * can be found in the document: <a
 * href="https://www.postgresql.org/docs/8.4/errcodes-appendix.html">error code of PostgreSQL</a>
 */
public class PostgreSQLExceptionConverter implements SQLExceptionConverter {
  private static final String DUPLICATED_ENTRY_ERROR_CODE = "23505";

  @Override
  @SuppressWarnings("FormatStringAnnotation")
  public void toGravitinoException(SQLException sqlException, Entity.EntityType type, String name)
      throws IOException {
    String errorCode = sqlException.getSQLState();
    switch (errorCode) {
      case DUPLICATED_ENTRY_ERROR_CODE:
        throw new EntityAlreadyExistsException(
            sqlException, "The %s entity: %s already exists.", type.name(), name);
      default:
        throw new IOException(sqlException);
    }
  }
}
