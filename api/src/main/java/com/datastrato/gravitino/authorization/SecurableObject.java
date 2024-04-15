/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.annotation.Evolving;
import javax.annotation.Nullable;

/**
 * The securable object is the entity which access can be granted. Unless allowed by a grant, access
 * is denied. Gravitino organizes the securable objects using tree structure. The securable object
 * may be a catalog, a table or a schema, etc. For example, `catalog1.schema1.table1` represents a
 * table named `table1`. It's in the schema named `schema1`. The schema is in the catalog named
 * `catalog1`. Similarly, `catalog1.schema1.topic1` can represent a topic.
 * `catalog1.schema1.fileset1` can represent a fileset. `*` represents all the catalogs.If you want
 * to use an another securable object which represents all entities," you can use its parent entity,
 * For example if you want to have read table privileges of all tables of `catalog1.schema1`, " you
 * can use add `read table` privilege for `catalog1.schema1` directly
 */
@Evolving
public interface SecurableObject {

  /**
   * The parent securable object. If the securable object doesn't have parent, this method will
   * return null.
   *
   * @return The parent securable object.
   */
  @Nullable
  SecurableObject parent();

  /**
   * The name of th securable object.
   *
   * @return The name of the securable object.
   */
  String name();
}
