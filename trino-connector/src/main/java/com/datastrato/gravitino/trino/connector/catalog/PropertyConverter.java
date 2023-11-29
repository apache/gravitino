/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog;

import java.util.Map;

/** Transforming between gravitino schema/table/column property and trino property. */
public interface PropertyConverter {

  /** Convert trino properties to gravitino properties. */
  default Map<String, String> toTrinoProperties(Map<String, String> properties) {
    return properties;
  }

  /** Convert gravitino properties to trino properties. */
  default Map<String, Object> toGravitinoProperties(Map<String, Object> properties) {
    return properties;
  }
}
