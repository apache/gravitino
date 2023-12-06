/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/** Interface for adding property metadata to the catalogs */
public interface HasPropertyMeta {

  /** Property metadata for schema */
  default List<PropertyMetadata<?>> getSchemaPropertyMetadata() {
    return ImmutableList.of();
  }

  /** Property metadata for table */
  default List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return ImmutableList.of();
  }

  /** Property metadata for column */
  default List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return ImmutableList.of();
  }

  default List<PropertyMetadata<?>> getCatalogPropertyMeta() {
    return ImmutableList.of();
  }
}
