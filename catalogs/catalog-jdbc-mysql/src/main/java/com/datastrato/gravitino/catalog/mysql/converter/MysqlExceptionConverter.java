/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import java.sql.SQLException;

/** Exception converter to gravitino exception for MySQL. */
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
