/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.annotation.Evolving;

/**
 * The securable object is the entity which access can be granted. Unless allowed by a grant, access
 * is denied. Gravitino organizes the securable objects using tree structure. The securable object
 * may be a catalog, a table or a schema, etc. For example, `catalog1.schema1.table1` represents a
 * table named `table1`. It's in the schema named `schema1`. The schema is in the catalog named
 * `catalog1`.
 */
@Evolving
public interface SecurableObject {

  /**
   * The parent securable object. If the securable object doesn't have parent, this method will
   * return null.
   *
   * @return The parent securable object.
   */
  SecurableObject parent();

  /**
   * The name of th securable object.
   *
   * @return The name of the securable object.
   */
  String name();
}
