/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris.converter;

import com.datastrato.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rel.types.Types;

/** Type converter for Doris. */
public class DorisTypeConverter extends JdbcTypeConverter<String> {
  // TODO: add implementation for doris catalog

  @Override
  public Type toGravitinoType(JdbcTypeBean typeBean) {
    return Types.UnparsedType.of(typeBean.getTypeName());
  }

  @Override
  public String fromGravitinoType(Type type) {
    throw new IllegalArgumentException("unsupported type: " + type.simpleString());
  }
}
