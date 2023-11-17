/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import io.substrait.type.Type;

public abstract class JdbcTypeConverter {

  /**
   * Convert from Gravitino type to JDBC type
   *
   * @param type
   * @return
   */
  public abstract Type toGravitinoType(String type);

  /**
   * Convert from JDBC type to Gravitino type
   *
   * @param type
   * @return
   */
  public abstract String fromGravitinoType(Type type);
}
