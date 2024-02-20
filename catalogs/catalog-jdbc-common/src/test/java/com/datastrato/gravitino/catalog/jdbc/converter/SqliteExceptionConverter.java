/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.exceptions.NoSuchTableException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import java.sql.SQLException;

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
