/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.sql.SQLException;

/** Exception converter to gravitino exception for Doris. */
public class DorisExceptionConverter extends JdbcExceptionConverter {
  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException se) {
    // TODO: add implementation for doris catalog

    return super.toGravitinoException(se);
  }
}
