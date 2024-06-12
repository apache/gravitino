/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector;

import com.datastrato.gravitino.rel.types.Type;

/**
 * The interface for converting data types between Gravitino and catalogs. In most cases, the ToType
 * and FromType are the same. But in some cases, such as converting between Gravitino and JDBC
 * types, the ToType is String and the FromType is JdbcTypeBean.
 *
 * @param <ToType> The Gravitino type will be converted to.
 * @param <FromType> The type will be converted to Gravitino type.
 */
public interface DataTypeConverter<ToType, FromType> {
  /**
   * Convert the Gravitino type to the catalog type.
   *
   * @param type The Gravitino type.
   * @return The catalog type.
   */
  ToType fromGravitino(Type type);

  /**
   * Convert the catalog type to the Gravitino type.
   *
   * @param type The catalog type.
   * @return The Gravitino type.
   */
  Type toGravitino(FromType type);
}
