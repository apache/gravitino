/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.connector.capability;

import com.google.common.base.Preconditions;
import org.apache.gravitino.annotation.Evolving;

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
