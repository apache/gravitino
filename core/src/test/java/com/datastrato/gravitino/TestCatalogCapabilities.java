/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.connector.capability.CapabilityResult;

public class TestCatalogCapabilities implements Capability {

  @Override
  public CapabilityResult caseSensitiveOnName(Scope scope) {
    return CapabilityResult.unsupported("The case sensitive on name is not supported.");
  }
}
