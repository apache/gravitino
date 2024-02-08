/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchColumnException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.exceptions.TableAlreadyExistsException;
import com.datastrato.gravitino.exceptions.UnauthorizedException;
import java.sql.SQLException;

/** Exception converter to gravitino exception for Doris. */
public class DorisExceptionConverter extends JdbcExceptionConverter {

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException se) {
    switch (se.getErrorCode()) {
      case 1007:
        return new SchemaAlreadyExistsException(se, se.getMessage());
      case 1050:
        return new TableAlreadyExistsException(se, se.getMessage());
      case 1008:
      case 1046:
      case 1049:
        return new NoSuchSchemaException(se, se.getMessage());
      case 1051:
        return new NoSuchTableException(se, se.getMessage());
      case 1045:
        return new UnauthorizedException(se, se.getMessage());
      case 1054:
        return new NoSuchColumnException(se, se.getMessage());
      default:
        return new GravitinoRuntimeException(se, se.getMessage());
    }
  }
}
