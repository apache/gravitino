/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.rel;

import com.datastrato.graviton.Auditable;
import com.datastrato.graviton.Namespace;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An interface representing a table in a {@link Namespace}. It defines basic properties of a table.
 * A catalog implementation with {@link TableCatalog} should implement it.
 */
public interface Table extends Auditable {

  /** return the name of the table. */
  String name();

  /** return the columns of the table. */
  Column[] columns();

  /** return the comment of the table. Null is returned if no comment is set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** return the properties of the table. Empty map is returned if no properties are set. */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
