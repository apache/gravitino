/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import java.sql.SQLException;

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
