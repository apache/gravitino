/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.connector.capability.CapabilityResult;

public class HiveCatalogCapability implements Capability {
  @Override
  public CapabilityResult columnNotNull() {
    // The NOT NULL constraint for column is supported since Hive3.0, see
    // https://issues.apache.org/jira/browse/HIVE-16575
    return CapabilityResult.unsupported(
        "The NOT NULL constraint for column is only supported since Hive 3.0, "
            + "but the current Gravitino Hive catalog only supports Hive 2.x.");
  }

  @Override
  public CapabilityResult columnDefaultValue() {
    // The DEFAULT constraint for column is supported since Hive3.0, see
    // https://issues.apache.org/jira/browse/HIVE-18726
    return CapabilityResult.unsupported(
        "The DEFAULT constraint for column is only supported since Hive 3.0, "
            + "but the current Gravitino Hive catalog only supports Hive 2.x.");
  }
}
