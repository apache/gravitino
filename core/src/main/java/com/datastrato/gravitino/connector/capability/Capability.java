/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

import com.datastrato.gravitino.annotation.Evolving;
import com.google.common.base.Preconditions;

@Evolving
public interface Capability {
  /** The scope of the capability. */
  enum Scope {
    CATALOG,
    SCHEMA,
    TABLE,
    COLUMN,
    FILESET
  }

  Capability SUPPORTED = new CapabilityImpl(true, null);

  static Capability unsupported(String unsupportedMessage) {
    return new CapabilityImpl(false, unsupportedMessage);
  }

  boolean supported();

  String unsupportedMessage();

  class CapabilityImpl implements Capability {
    private final boolean supported;
    private final String unsupportedMessage;

    private CapabilityImpl(boolean supported, String unsupportedMessage) {
      Preconditions.checkArgument(
          supported || unsupportedMessage != null,
          "unsupportedReason is required when supportsNotNull is false");
      this.supported = supported;
      this.unsupportedMessage = unsupportedMessage;
    }

    @Override
    public boolean supported() {
      return supported;
    }

    @Override
    public String unsupportedMessage() {
      return unsupportedMessage;
    }
  }
}
