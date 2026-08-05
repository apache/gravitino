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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Write-through secret binding for create/alter requests: a registered provider instance name plus
 * plaintext to store.
 */
@DeveloperApi
public final class SecretBinding {

  private final String provider;
  private final String value;

  /**
   * Creates a write-through binding.
   *
   * @param provider registered provider instance name
   * @param value plaintext secret value
   */
  @JsonCreator
  public SecretBinding(
      @JsonProperty("provider") String provider, @JsonProperty("value") String value) {
    Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
    Preconditions.checkArgument(value != null, "value must not be null");
    Preconditions.checkArgument(
        !"******".equals(value), "value must not be the masked placeholder");
    this.provider = provider;
    this.value = value;
  }

  /**
   * Returns the registered provider instance name.
   *
   * @return the provider name
   */
  @JsonProperty("provider")
  public String provider() {
    return provider;
  }

  /**
   * Returns the plaintext secret value.
   *
   * @return the plaintext value
   */
  @JsonProperty("value")
  public String value() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SecretBinding)) {
      return false;
    }
    SecretBinding that = (SecretBinding) o;
    return Objects.equals(provider, that.provider) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, value);
  }

  @Override
  public String toString() {
    return "SecretBinding{provider='" + provider + "', value=***}";
  }
}
