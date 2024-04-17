/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.connector.capability.CapabilityResult;

public class IcebergCatalogCapability implements Capability {
  @Override
  public CapabilityResult columnDefaultValue() {
    // Iceberg column default value is WIP, see
    // https://github.com/apache/iceberg/pull/4525
    return CapabilityResult.unsupported("Iceberg does not support column default value.");
  }
}
