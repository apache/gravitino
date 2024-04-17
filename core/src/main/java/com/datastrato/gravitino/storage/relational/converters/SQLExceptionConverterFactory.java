/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.converters;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.google.common.base.Preconditions;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLExceptionConverterFactory {
  private static final Pattern TYPE_PATTERN = Pattern.compile("jdbc:(\\w+):");
  private static SQLExceptionConverter converter;

  private SQLExceptionConverterFactory() {}

  public static synchronized void initConverter(Config config) {
    if (converter == null) {
      String jdbcUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
      Matcher typeMatcher = TYPE_PATTERN.matcher(jdbcUrl);
      if (typeMatcher.find()) {
        String jdbcType = typeMatcher.group(1);
        if (jdbcType.equalsIgnoreCase("mysql")) {
          converter = new MySQLExceptionConverter();
        } else if (jdbcType.equalsIgnoreCase("h2")) {
          converter = new H2ExceptionConverter();
        } else {
          throw new IllegalArgumentException(String.format("Unsupported jdbc type: %s", jdbcType));
        }
      } else {
        throw new IllegalArgumentException(
            String.format("Cannot find jdbc type in jdbc url: %s", jdbcUrl));
      }
    }
  }

  public static SQLExceptionConverter getConverter() {
    Preconditions.checkState(converter != null, "Exception converter is not initialized.");
    return converter;
  }
}
