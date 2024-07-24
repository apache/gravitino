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
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;

public class SqliteExceptionConverter extends JdbcExceptionConverter {

  public static final int NO_SUCH_TABLE = 1;
  public static final int SCHEMA_ALREADY_EXISTS_CODE = 2;

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException sqlException) {
    if (sqlException.getErrorCode() == NO_SUCH_TABLE) {
      return new NoSuchTableException(sqlException.getMessage(), sqlException);
    } else if (sqlException.getErrorCode() == SCHEMA_ALREADY_EXISTS_CODE) {
      return new SchemaAlreadyExistsException(sqlException.getMessage(), sqlException);
    } else {
      return super.toGravitinoException(sqlException);
    }
  }
}
