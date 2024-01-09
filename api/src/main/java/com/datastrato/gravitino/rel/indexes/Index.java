/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.rel.indexes;

/**
 * The Index interface defines methods for implementing table index columns. Currently, settings for
 * PRIMARY_KEY and UNIQUE_KEY are provided.
 */
public interface Index {

  /** @return The type of the index. eg: PRIMARY_KEY and UNIQUE_KEY. */
  IndexType type();

  /** @return The name of the index. */
  String name();

  /** @return The field name under the table contained in the index. eg: table.id */
  String[][] fieldNames();

  enum IndexType {
    /** Primary key index. */
    PRIMARY_KEY,
    /** Unique key index. */
    UNIQUE_KEY,
  }
}
