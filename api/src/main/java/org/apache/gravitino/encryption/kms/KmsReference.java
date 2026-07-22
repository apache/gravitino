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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Locale;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Identifies a key owned by a configured KMS source.
 *
 * <p>Contains no credentials or key material.
 */
@DeveloperApi
public final class KmsReference {

  private final String api;
  private final String source;
  private final String keyId;

  /**
   * Creates a structurally valid key reference without contacting the provider.
   *
   * @param api explicitly selected KMS API identifier
   * @param source configured KMS client-instance name
   * @param keyId provider-native key identifier
   * @throws IllegalArgumentException if any argument is null or blank
   */
  @JsonCreator
  public KmsReference(
      @JsonProperty("api") String api,
      @JsonProperty("source") String source,
      @JsonProperty("keyId") String keyId) {
    Preconditions.checkArgument(StringUtils.isNotBlank(api), "KMS API cannot be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(source), "KMS source cannot be blank");
    Preconditions.checkArgument(StringUtils.isNotBlank(keyId), "KMS key ID cannot be blank");

    this.api = api.trim().toLowerCase(Locale.ROOT);
    this.source = source;
    this.keyId = keyId;
  }

  /**
   * Returns the explicitly selected KMS API identifier.
   *
   * @return the canonical KMS API identifier
   */
  public String api() {
    return api;
  }

  /**
   * Returns the configured KMS client-instance name.
   *
   * @return the source name
   */
  public String source() {
    return source;
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
    return api.equals(that.api) && source.equals(that.source) && keyId.equals(that.keyId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(api, source, keyId);
  }

  @Override
  public String toString() {
    return String.format("KmsReference{api='%s', source='%s', keyId='%s'}", api, source, keyId);
  }
}
