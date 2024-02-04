/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class SqliteTypeConverter extends JdbcTypeConverter<String> {

  protected static final Map<Type, String> GRAVITINO_TO_SQLITE_MAPPING = new HashMap<>();

  static {
    GRAVITINO_TO_SQLITE_MAPPING.put(Types.ByteType.get(), "TINYINT");
    GRAVITINO_TO_SQLITE_MAPPING.put(Types.IntegerType.get(), "INTEGER");
    GRAVITINO_TO_SQLITE_MAPPING.put(Types.StringType.get(), "TEXT");
    GRAVITINO_TO_SQLITE_MAPPING.put(Types.BinaryType.get(), "BLOB");
  }

  @Override
  public Type toGravitinoType(JdbcTypeBean type) {
    return GRAVITINO_TO_SQLITE_MAPPING.entrySet().stream()
        .filter(entry -> StringUtils.equalsIgnoreCase(type.getTypeName(), entry.getValue()))
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
