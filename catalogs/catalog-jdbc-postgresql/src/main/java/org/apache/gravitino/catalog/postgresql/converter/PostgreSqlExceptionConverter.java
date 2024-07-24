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
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;

public class PostgreSqlExceptionConverter extends JdbcExceptionConverter {

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException se) {
    if (null != se.getSQLState()) {
      switch (se.getSQLState()) {
        case "42P04":
        case "42P06":
          return new SchemaAlreadyExistsException(se.getMessage(), se);
        case "42P07":
          return new TableAlreadyExistsException(se.getMessage(), se);
        case "3D000":
        case "3F000":
          return new NoSuchSchemaException(se.getMessage(), se);
        case "42P01":
          return new NoSuchTableException(se.getMessage(), se);
        default:
          return new GravitinoRuntimeException(se.getMessage(), se);
      }
    } else {
      return new GravitinoRuntimeException(se.getMessage(), se);
    }
  }
}
