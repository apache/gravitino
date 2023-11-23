/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class SqliteTypeConverter extends JdbcTypeConverter {

  protected static final Map<Type, String> GRAVITINO_TO_SQLITE_MAPPING = new HashMap<>();

  static {
    GRAVITINO_TO_SQLITE_MAPPING.put(TypeCreator.NULLABLE.I8, "TINYINT");
    GRAVITINO_TO_SQLITE_MAPPING.put(TypeCreator.NULLABLE.I32, "INTEGER");
    GRAVITINO_TO_SQLITE_MAPPING.put(TypeCreator.NULLABLE.STRING, "TEXT");
    GRAVITINO_TO_SQLITE_MAPPING.put(TypeCreator.NULLABLE.BINARY, "BLOB");
  }

  @Override
  public Type toGravitinoType(String type) {
    return GRAVITINO_TO_SQLITE_MAPPING.entrySet().stream()
        .filter(entry -> StringUtils.equalsIgnoreCase(type, entry.getValue()))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElse(null);
  }

  @Override
  public String fromGravitinoType(Type type) {
    return GRAVITINO_TO_SQLITE_MAPPING.get(type);
  }

  public Collection<Type> getGravitinoTypes() {
    return GRAVITINO_TO_SQLITE_MAPPING.keySet();
  }
}
