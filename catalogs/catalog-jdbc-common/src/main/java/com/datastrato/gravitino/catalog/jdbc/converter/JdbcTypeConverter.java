/*
 * Copyright 2023 DATASTRATO Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.converter;

import com.datastrato.gravitino.rel.types.Type;

public abstract class JdbcTypeConverter<FROM, TO> {

  /**
   * Convert from Gravitino type to JDBC type
   *
   * @param type
   * @return
   */
  public abstract Type toGravitinoType(FROM type);

  /**
   * Convert from JDBC type to Gravitino type
   *
   * @param type
   * @return
   */
  public abstract TO fromGravitinoType(Type type);
}
