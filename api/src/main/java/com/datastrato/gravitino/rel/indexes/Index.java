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

  /**
   * @return The field name under the table contained in the index. it is the column names, could be
   *     "a.b.c" for nested column, but normally it could only be "a".
   */
  String[][] fieldNames();

  enum IndexType {
    /**
     * Primary key index. Primary key in a relational database is a field or a combination of fields
     * that uniquely identifies each record in a table. It serves as a unique identifier for each
     * row, ensuring that no two rows have the same key.
     */
    PRIMARY_KEY,
    /**
     * Unique key index. A unique key in a relational database is a field or a combination of fields
     * that ensures each record in a table has a distinct value or combination of values. Unlike a
     * primary key, a unique key allows for the presence of null values, but it still enforces the
     * constraint that no two records can have the same unique key value(s).
     */
    UNIQUE_KEY,
  }
}
