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
import javax.annotation.Nullable;
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
   * Writes a plaintext secret and returns its URN. The written value becomes the value {@link
   * #readSecret} returns.
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
   * Deletes a secret by URN, including every version of it.
   *
   * @param urn the secret URN
   */
  void deleteSecret(SecretUrn urn);

  /**
   * Writes {@code plaintext} as a new version of the write-through secret {@code urn}.
   *
   * <p>The new version becomes the value {@link #readSecret} returns only when {@code urn} has no
   * current version. Otherwise the current version stays readable until {@link
   * #promoteSecretVersion} is called, so a failed entity alter can discard the new version and keep
   * the value the entity still references.
   *
   * <p>Providers that support write-through must implement this method and the other version
   * methods; altering a write-through secret uses them. Providers that only build external
   * references can keep the defaults, which throw.
   *
   * @param urn the write-through secret URN
   * @param plaintext the secret plaintext
   * @return the id of the new version
   * @throws UnsupportedOperationException if this provider does not support versions
   */
  default String writeSecretVersion(SecretUrn urn, String plaintext) {
    throw new UnsupportedOperationException(type() + " does not support secret versions");
  }

  /**
   * Makes {@code version} the value {@link #readSecret} returns for {@code urn}.
   *
   * @param urn the write-through secret URN
   * @param version a version id returned by {@link #writeSecretVersion}
   * @throws IllegalArgumentException if {@code urn} has no such version
   * @throws UnsupportedOperationException if this provider does not support versions
   */
  default void promoteSecretVersion(SecretUrn urn, String version) {
    throw new UnsupportedOperationException(type() + " does not support secret versions");
  }

  /**
   * Deletes one version of {@code urn} and keeps the others.
   *
   * <p>If {@code version} is the current version, {@code urn} has no readable value afterwards.
   * Deleting a version that does not exist does nothing.
   *
   * @param urn the write-through secret URN
   * @param version the version id to delete
   * @throws UnsupportedOperationException if this provider does not support versions
   */
  default void deleteSecretVersion(SecretUrn urn, String version) {
    throw new UnsupportedOperationException(type() + " does not support secret versions");
  }

  /**
   * Returns the id of the version {@link #readSecret} returns for {@code urn}.
   *
   * @param urn the write-through secret URN
   * @return the current version id, or {@code null} if {@code urn} has no readable value
   * @throws UnsupportedOperationException if this provider does not support versions
   */
  @Nullable
  default String currentSecretVersion(SecretUrn urn) {
    throw new UnsupportedOperationException(type() + " does not support secret versions");
  }

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
