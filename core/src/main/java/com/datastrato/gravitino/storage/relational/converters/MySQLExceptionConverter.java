/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.converters;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.sql.SQLException;

/**
 * Exception converter to Gravitino exception for MySQL. The definition of error codes can be found
 * in the document: <a
 * href="https://dev.mysql.com/doc/connector-j/en/connector-j-reference-error-sqlstates.html"></a>
 */
public class MySQLExceptionConverter implements SQLExceptionConverter {
  /** It means found a duplicated primary key or unique key entry in MySQL. */
  private static final int DUPLICATED_ENTRY_ERROR_CODE = 1062;

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(
      SQLException se, Entity.EntityType type, String name) {
    switch (se.getErrorCode()) {
      case DUPLICATED_ENTRY_ERROR_CODE:
        return new AlreadyExistsException(se, se.getMessage());
      default:
        return new GravitinoRuntimeException(se, se.getMessage());
    }
  }
}
