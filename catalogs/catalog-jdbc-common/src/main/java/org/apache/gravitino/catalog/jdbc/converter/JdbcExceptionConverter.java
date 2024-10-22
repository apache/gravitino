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
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;

/** Interface for converter JDBC exceptions to Gravitino exceptions. */
public class JdbcExceptionConverter {

  /**
   * Convert JDBC exception to GravitinoException. The default implementation of this method is
   * based on MySQL exception code, and if the catalog does not compatible with MySQL exception
   * code, this method needs to be rewritten.
   *
   * @param sqlException The sql exception to map
   * @return The best attempt at a corresponding connector exception or generic with the
   *     SQLException as the cause
   */
  @SuppressWarnings("FormatStringAnnotation")
  public GravitinoRuntimeException toGravitinoException(final SQLException sqlException) {
    switch (sqlException.getErrorCode()) {
      case 1007:
        return new SchemaAlreadyExistsException(sqlException, sqlException.getMessage());
      case 1050:
        return new TableAlreadyExistsException(sqlException, sqlException.getMessage());
      case 1008:
      case 1049:
        return new NoSuchSchemaException(sqlException, sqlException.getMessage());
      case 1146:
      case 1051:
        return new NoSuchTableException(sqlException, sqlException.getMessage());
      default:
        return new GravitinoRuntimeException(sqlException, sqlException.getMessage());
    }
  }
}
