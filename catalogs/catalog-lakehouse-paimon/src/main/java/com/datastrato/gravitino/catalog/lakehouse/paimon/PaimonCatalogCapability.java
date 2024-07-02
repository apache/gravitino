/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.paimon;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.connector.capability.CapabilityResult;

public class PaimonCatalogCapability implements Capability {

  @Override
  public CapabilityResult columnDefaultValue() {
    // See https://github.com/apache/paimon/pull/1425/files
    return CapabilityResult.unsupported(
        "Paimon set column default value through table properties instead of column info.");
  }
}
