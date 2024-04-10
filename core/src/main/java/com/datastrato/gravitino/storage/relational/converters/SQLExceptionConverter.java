/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.converters;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import java.sql.SQLException;

/** Interface for converter JDBC SQL exceptions to Gravitino exceptions. */
public interface SQLExceptionConverter {
  /**
   * Convert JDBC exception to GravitinoException.
   *
   * @param sqlException The sql exception to map
   * @param type The type of the entity
   * @param name The name of the entity
   * @return A best attempt at a corresponding connector exception or generic with the SQLException
   *     as the cause
   */
  GravitinoRuntimeException toGravitinoException(
      SQLException sqlException, Entity.EntityType type, String name);
}
