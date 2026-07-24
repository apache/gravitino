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
package org.apache.gravitino.encryption.kms;

import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Common properties reported for a successfully located KMS key.
 *
 * <p>Provider-specific lifecycle state may describe the logical key or its selected or current
 * version. Capabilities describe key-specific structural support; this API does not perform
 * cryptographic operations.
 */
@DeveloperApi
public interface KmsKeyProperties {

  /**
   * Returns the requested key reference.
   *
   * @return the key reference
   */
  KmsReference reference();

  /**
   * Returns the provider's normalized lifecycle state for the key.
   *
   * <p>This does not indicate caller authorization, service availability, or that a subsequent
   * operation is guaranteed to succeed.
   *
   * @return whether the key is enabled
   */
  boolean enabled();

  /**
   * Returns whether the key structurally supports wrapping data-encryption keys.
   *
   * @return whether wrapping is supported
   */
  boolean supportsWrapping();

  /**
   * Returns whether the key structurally supports unwrapping data-encryption keys.
   *
   * @return whether unwrapping is supported
   */
  boolean supportsUnwrapping();
}
