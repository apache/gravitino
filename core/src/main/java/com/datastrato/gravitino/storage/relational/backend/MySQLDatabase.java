/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.backend;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.storage.relational.JDBCDatabase;

public class MySQLDatabase implements JDBCDatabase {

  @Override
  public void initialize(Config config) throws RuntimeException {
    // Do nothing. MySQL does not support start in embedded mode.
  }
}
