/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog;

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;

/** Transforming gravitino metadata to trino. */
public interface HasProperties {

  /** @return TableProperties list that used to validate table properties. */
  default Map<String, String> toTrinoProperties(Map<String, String> properties) {
    return properties;
  }

  /** @return SchemaProperties list that used to validate schema properties. */
  default Map<String, String> toGravitinoProperties(Map<String, String> properties) {
    return properties;
  }

  default List<PropertyMetadata<?>> getPropertyMetadata() {
    return ImmutableList.of();
  }
}
