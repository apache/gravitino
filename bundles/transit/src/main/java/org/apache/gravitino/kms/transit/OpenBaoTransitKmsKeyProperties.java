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
package org.apache.gravitino.kms.transit;

import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;

final class OpenBaoTransitKmsKeyProperties implements KmsKeyProperties {

  private final KmsReference reference;
  private final boolean present;
  private final boolean enabled;
  private final boolean supportsWrapping;
  private final boolean supportsUnwrapping;

  OpenBaoTransitKmsKeyProperties(
      KmsReference reference,
      boolean present,
      boolean enabled,
      boolean supportsWrapping,
      boolean supportsUnwrapping) {
    this.reference = reference;
    this.present = present;
    this.enabled = enabled;
    this.supportsWrapping = supportsWrapping;
    this.supportsUnwrapping = supportsUnwrapping;
  }

  /** {@inheritDoc} */
  @Override
  public KmsReference reference() {
    return reference;
  }

  /** {@inheritDoc} */
  @Override
  public boolean present() {
    return present;
  }

  /** {@inheritDoc} */
  @Override
  public boolean enabled() {
    return enabled;
  }

  /** {@inheritDoc} */
  @Override
  public boolean supportsWrapping() {
    return supportsWrapping;
  }

  /** {@inheritDoc} */
  @Override
  public boolean supportsUnwrapping() {
    return supportsUnwrapping;
  }
}
