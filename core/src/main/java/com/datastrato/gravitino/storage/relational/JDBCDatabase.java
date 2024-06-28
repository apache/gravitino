/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational;

import com.datastrato.gravitino.Config;
import java.io.Closeable;

public interface JDBCDatabase extends Closeable {

  /**
   * Initializes the Relational database environment with the provided configuration.
   *
   * @param config The configuration for the database backend.
   * @throws RuntimeException
   */
  void initialize(Config config);
}
