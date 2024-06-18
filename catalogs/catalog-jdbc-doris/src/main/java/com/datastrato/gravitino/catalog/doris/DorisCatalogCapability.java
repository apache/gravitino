/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.doris;

import com.datastrato.gravitino.connector.capability.Capability;

public class DorisCatalogCapability implements Capability {
  // Doris best practice mention that the name should be in lowercase, separated by underscores
  // https://doris.apache.org/docs/2.0/table-design/best-practice/
  // We can use the more general DEFAULT_NAME_PATTERN for Doris and update as needed in the future
}
