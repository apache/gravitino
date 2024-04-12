/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.annotation.Evolving;

/**
 * The resource is the unit of the authorization. Our system organizes the resources using tree
 * structure. The resource may be a catalog, a table or a schema, etc. For example,
 * `catalog1.schema1.table1` represents a table named `table1`. It's in the schema named `schema1`.
 * The schema is in the catalog named `catalog1`.
 */
@Evolving
public interface Resource {

  Resource parent();

  String name();
}
