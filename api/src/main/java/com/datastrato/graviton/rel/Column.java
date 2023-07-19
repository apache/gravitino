/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.rel;

import com.datastrato.graviton.NameIdentifier;
import io.substrait.type.Type;
import java.util.Map;

/**
 * An interface representing a column of a {@link Table}. It defines basic properties of a column,
 * such as name and data type.
 *
 * <p>Catalog implementation needs to implement it. They should consume it in APIs like {@link
 * TableCatalog#createTable(NameIdentifier, Column[], String, Map)}, and report it in {@link
 * Table#columns()} a default value and a generation expression.
 */
public interface Column {

  /** Returns the name of this column. */
  String name();

  /** Returns the data type of this column. */
  Type dataType();

  /** Returns the comment of this column, null if not specified. */
  String comment();

  // TODO. Support column default value. @Jerry
}
