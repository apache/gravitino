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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/** In-memory KMS client for contract and consumer tests. */
public final class FakeKmsClient implements KmsClient {

  private final String api;
  private final String source;
  private final Map<String, KeyState> keys = new HashMap<>();

  /**
   * Creates an empty fake client.
   *
   * @param api canonical KMS API identifier accepted by the client
   * @param source configured source accepted by the client
   */
  public FakeKmsClient(String api, String source) {
    if (StringUtils.isBlank(api)) {
      throw new IllegalArgumentException("KMS API cannot be blank");
    }
    this.api = api.trim().toLowerCase(Locale.ROOT);
    this.source = source;
  }

  /**
   * Adds or replaces a key reported by this client.
   *
   * @param keyId provider-native key identifier
   * @param enabled whether the key is enabled
   * @param supportsWrapping whether the key supports wrapping
   * @param supportsUnwrapping whether the key supports unwrapping
   * @return this client
   */
  public FakeKmsClient putKey(
      String keyId, boolean enabled, boolean supportsWrapping, boolean supportsUnwrapping) {
    keys.put(keyId, new KeyState(enabled, supportsWrapping, supportsUnwrapping));
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public Optional<KmsKeyProperties> getKeyProperties(KmsReference reference) {
    requireReference(reference);
    KeyState state = keys.get(reference.keyId());
    return state == null
        ? Optional.empty()
        : Optional.of(
            new Properties(
                reference, state.enabled, state.supportsWrapping, state.supportsUnwrapping));
  }

  private void requireReference(KmsReference reference) {
    if (reference == null) {
      throw new IllegalArgumentException("KMS reference cannot be null");
    }
    if (!reference.api().equals(api)) {
      throw new IllegalArgumentException(
          String.format("Expected KMS API %s but received %s", api, reference.api()));
    }
    if (!source.equals(reference.source())) {
      throw new IllegalArgumentException(
          String.format(
              "KMS source %s does not match configured source %s", reference.source(), source));
    }
  }

  private static final class KeyState {

    private final boolean enabled;
    private final boolean supportsWrapping;
    private final boolean supportsUnwrapping;

    private KeyState(boolean enabled, boolean supportsWrapping, boolean supportsUnwrapping) {
      this.enabled = enabled;
      this.supportsWrapping = supportsWrapping;
      this.supportsUnwrapping = supportsUnwrapping;
    }
  }

  private static final class Properties implements KmsKeyProperties {

    private final KmsReference reference;
    private final boolean enabled;
    private final boolean supportsWrapping;
    private final boolean supportsUnwrapping;

    private Properties(
        KmsReference reference,
        boolean enabled,
        boolean supportsWrapping,
        boolean supportsUnwrapping) {
      this.reference = reference;
      this.enabled = enabled;
      this.supportsWrapping = supportsWrapping;
      this.supportsUnwrapping = supportsUnwrapping;
    }

    @Override
    public KmsReference reference() {
      return reference;
    }

    @Override
    public boolean enabled() {
      return enabled;
    }

    @Override
    public boolean supportsWrapping() {
      return supportsWrapping;
    }

    @Override
    public boolean supportsUnwrapping() {
      return supportsUnwrapping;
    }
  }
}
