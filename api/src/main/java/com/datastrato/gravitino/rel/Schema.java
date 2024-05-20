/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.annotation.Evolving;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An interface representing a schema in the {@link com.datastrato.gravitino.Catalog}. A Schema is a
 * basic container of relational objects, like tables, views, etc. A Schema can be self-nested,
 * which means it can be schema1.schema2.table.
 *
 * <p>This defines the basic properties of a schema. A catalog implementation with {@link
 * SupportsSchemas} should implement this interface.
 */
@Evolving
public interface Schema extends Auditable {

  /** @return The name of the Schema. */
  String name();

  /** @return The comment of the Schema. Null is returned if the comment is not set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** @return The properties of the Schema. An empty map is returned if no properties are set. */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
