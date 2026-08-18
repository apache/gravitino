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

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Identifies a key owned by a named KMS provider.
 *
 * <p>Contains no credentials or key material. The provider name is the configured instance handle
 * ({@code gravitino.kms.providers}). The server loads that name's factory from {@code
 * gravitino.kms.provider.<name>.className} and resolves a client from {@code KmsClientRegistry}.
 */
@DeveloperApi
public final class KmsReference {

  private final String provider;
  private final String keyId;

  /**
   * Creates a structurally valid key reference without contacting the provider.
   *
   * @param provider configured KMS provider name
   * @param keyId provider-native key identifier
   * @throws IllegalArgumentException if either argument is null or blank
   */
  public KmsReference(String provider, String keyId) {
    Preconditions.checkArgument(StringUtils.isNotBlank(provider), "KMS provider cannot be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(keyId), "KMS key ID cannot be blank");

    this.provider = provider.trim();
    this.keyId = keyId;
  }

  /**
   * Returns the configured KMS provider name.
   *
   * @return the provider name
   */
  public String provider() {
    return provider;
  }

  /**
   * Returns the provider-native key identifier.
   *
   * @return the key identifier
   */
  public String keyId() {
    return keyId;
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof KmsReference)) {
      return false;
    }
    KmsReference that = (KmsReference) other;
    return provider.equals(that.provider) && keyId.equals(that.keyId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, keyId);
  }

  @Override
  public String toString() {
    return String.format("KmsReference{provider='%s', keyId='%s'}", provider, keyId);
  }
}
