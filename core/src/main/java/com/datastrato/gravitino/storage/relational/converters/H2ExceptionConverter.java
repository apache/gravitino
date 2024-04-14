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
 * Exception converter to Gravitino exception for H2. The definition of error codes can be found in
 * the document: <a href="https://h2database.com/javadoc/org/h2/api/ErrorCode.html"></a>
 */
public class H2ExceptionConverter implements SQLExceptionConverter {

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(
      SQLException se, Entity.EntityType type, String name) {
    switch (se.getErrorCode()) {
      case 23505:
        return new AlreadyExistsException(se, se.getMessage());
      default:
        return new GravitinoRuntimeException(se, se.getMessage());
    }
  }
}
