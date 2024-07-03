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
package com.datastrato.gravitino.catalog.mysql.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import java.sql.SQLException;

/** Exception converter to Gravitino exception for MySQL. */
public class MysqlExceptionConverter extends JdbcExceptionConverter {

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException se) {
    switch (se.getErrorCode()) {
      case 1007:
        return new SchemaAlreadyExistsException(se, se.getMessage());
      case 1050:
        return new TableAlreadyExistsException(se, se.getMessage());
      case 1008:
      case 1049:
        return new NoSuchSchemaException(se, se.getMessage());
      case 1146:
      case 1051:
        return new NoSuchTableException(se, se.getMessage());
      default:
        return new GravitinoRuntimeException(se, se.getMessage());
    }
  }
}
