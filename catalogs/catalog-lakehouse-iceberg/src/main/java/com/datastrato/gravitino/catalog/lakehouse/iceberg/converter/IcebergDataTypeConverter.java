/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.converter;

import com.datastrato.gravitino.connector.DataTypeConverter;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;

public class IcebergDataTypeConverter implements DataTypeConverter<Type, Type> {
  public static final IcebergDataTypeConverter CONVERTER = new IcebergDataTypeConverter();

  @Override
  public Type fromGravitino(com.datastrato.gravitino.rel.types.Type gravitinoType) {
    return ToIcebergTypeVisitor.visit(gravitinoType, new ToIcebergType());
  }

  @Override
  public com.datastrato.gravitino.rel.types.Type toGravitino(Type type) {
    return TypeUtil.visit(type, new FromIcebergType());
  }
}
