/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.sql.SQLException;

/** Interface for converter JDBC exceptions to Gravitino exceptions. */
public abstract class JdbcExceptionConverter {

  /**
   * Convert JDBC exception to GravitinoException.
   *
   * @param sqlException The sql exception to map
   * @return A best attempt at a corresponding connector exception or generic with the SQLException
   *     as the cause
   */
  @SuppressWarnings("FormatStringAnnotation")
  public GravitinoRuntimeException toGravitinoException(final SQLException sqlException) {
    // TODO we need to transform specific SQL exceptions into our own exceptions, such as
    // SchemaAlreadyExistsException
    return new GravitinoRuntimeException(sqlException, sqlException.getMessage());
  }
}
