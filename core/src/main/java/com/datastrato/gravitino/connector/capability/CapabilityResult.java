/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.connector.capability;

import com.datastrato.gravitino.annotation.Evolving;
import com.google.common.base.Preconditions;

/** The CapabilityResult class is responsible for managing the capability result. */
@Evolving
public interface CapabilityResult {

  /** The supported capability result. */
  CapabilityResult SUPPORTED = new ResultImpl(true, null);

  /**
   * The unsupported capability result.
   *
   * @param unsupportedMessage The unsupported message.
   * @return The unsupported capability result.
   */
  static CapabilityResult unsupported(String unsupportedMessage) {
    return new ResultImpl(false, unsupportedMessage);
  }

  /**
   * Check if the capability is supported.
   *
   * @return true if the capability is supported, false otherwise.
   */
  boolean supported();

  /**
   * Get the unsupported message.
   *
   * @return The unsupported message.
   */
  String unsupportedMessage();

  /** The CapabilityResult implementation. */
  class ResultImpl implements CapabilityResult {
    private final boolean supported;
    private final String unsupportedMessage;

    private ResultImpl(boolean supported, String unsupportedMessage) {
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
