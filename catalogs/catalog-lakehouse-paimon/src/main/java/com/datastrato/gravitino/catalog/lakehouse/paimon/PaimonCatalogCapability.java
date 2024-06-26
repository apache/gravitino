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
    return CapabilityResult.unsupported(
        "Paimon does not support setting the column default value through column info. Instead, it should be set through table properties. See https://github.com/apache/paimon/pull/1425/files#diff-5a41731b962ed7fbf3c2623031bbc4e34dc3e8bfeb40df68c594c88a740f8800.");
  }
}
