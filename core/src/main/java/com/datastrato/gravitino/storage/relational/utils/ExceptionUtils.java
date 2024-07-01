/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.utils;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.storage.relational.converters.SQLExceptionConverterFactory;
import java.io.IOException;
import java.sql.SQLException;

public class ExceptionUtils {
  private ExceptionUtils() {}

  public static void checkSQLException(
      RuntimeException re, Entity.EntityType type, String entityName) throws IOException {
    if (re.getCause() instanceof SQLException) {
      SQLExceptionConverterFactory.getConverter()
          .toGravitinoException((SQLException) re.getCause(), type, entityName);
    }
  }
}
