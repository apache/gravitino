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
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;

/** Exception converter to Gravitino exception for Doris. */
public class DorisExceptionConverter extends JdbcExceptionConverter {

  // see: https://doris.apache.org/docs/admin-manual/maint-monitor/doris-error-code/
  @VisibleForTesting static final int CODE_DATABASE_EXISTS = 1007;

  static final int CODE_TABLE_EXISTS = 1050;
  static final int CODE_NO_SUCH_SCHEMA = 1049;
  static final int CODE_DATABASE_NOT_EXISTS = 1008;
  static final int CODE_NO_SUCH_TABLE = 1051;
  static final int CODE_UNAUTHORIZED = 1045;
  static final int CODE_NO_SUCH_COLUMN = 1054;
  static final int CODE_OTHER = 1105;

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException se) {
    int errorCode = se.getErrorCode();

    switch (errorCode) {
      case CODE_DATABASE_EXISTS:
        return new SchemaAlreadyExistsException(se, se.getMessage());
      case CODE_TABLE_EXISTS:
        return new TableAlreadyExistsException(se, se.getMessage());
      case CODE_DATABASE_NOT_EXISTS:
        return new NoSuchSchemaException(se, se.getMessage());
      case CODE_NO_SUCH_TABLE:
        return new NoSuchTableException(se, se.getMessage());
      case CODE_UNAUTHORIZED:
        return new UnauthorizedException(se, se.getMessage());
      case CODE_NO_SUCH_COLUMN:
        return new NoSuchColumnException(se, se.getMessage());
      default:
        return new GravitinoRuntimeException(se, se.getMessage());
    }
  }
}
