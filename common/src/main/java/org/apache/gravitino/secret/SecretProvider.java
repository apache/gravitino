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

package org.apache.gravitino.secret;

import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;

/** Service provider interface for secret backends. */
@DeveloperApi
public interface SecretProvider {

  /**
   * Initializes this provider after construction.
   *
   * <p>Implementations must override this method and explicitly decide whether configuration is
   * required. An empty body is acceptable for providers that need no setup.
   *
   * @param name the configured provider instance name
   * @param config provider-specific configuration (without the {@code gravitino.secret.provider.
   *     <name>.} prefix)
   */
  void initialize(String name, Map<String, String> config);

  /**
   * Returns the provider type identifier.
   *
   * @return the provider type
   */
  String type();

  /**
   * Writes a plaintext secret and returns its URN.
   *
   * <p>Provider-specific write metadata is supplied as {@code attributes}. Required keys depend on
   * the provider implementation; for example the in-memory write-through provider expects {@link
   * SecretConstants#ATTR_ENTITY_TYPE}, {@link SecretConstants#ATTR_ENTITY_ID}, and {@link
   * SecretConstants#ATTR_PROPERTY_KEY}.
   *
   * @param plaintext the secret plaintext
   * @param attributes provider-specific write attributes
   * @return the secret URN
   */
  SecretUrn writeSecret(String plaintext, Map<String, String> attributes);

  /**
   * Reads a secret by URN.
   *
   * @param urn the secret URN
   * @return the secret plaintext
   */
  String readSecret(SecretUrn urn);

  /**
   * Deletes a secret by URN.
   *
   * @param urn the secret URN
   */
  void deleteSecret(SecretUrn urn);

  /**
   * Builds a URN for an external secret reference without writing secret material.
   *
   * <p>Providers that only support write-through must leave the default implementation, which
   * rejects external references.
   *
   * @param propertyKey the entity property key that will store the URN
   * @param attributes provider-specific locator attributes
   * @return the external-reference secret URN (must end with {@code propertyKey})
   * @throws UnsupportedOperationException if this provider does not support external references
   * @throws IllegalArgumentException if attributes are invalid for this provider
   */
  default SecretUrn buildReferenceUrn(String propertyKey, Map<String, String> attributes) {
    throw new UnsupportedOperationException(
        type() + " does not support external secret references");
  }

  /**
   * Releases resources owned by this provider.
   *
   * <p>Implementations must override this method and explicitly release any held resources. An
   * empty body is acceptable when there is nothing to clean up.
   */
  void close();
}
