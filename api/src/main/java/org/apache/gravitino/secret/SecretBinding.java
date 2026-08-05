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

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * Write-through secret binding for create/alter requests: a registered provider instance name plus
 * plaintext to store.
 */
@Evolving
public final class SecretBinding {

  private final String provider;
  private final String plaintext;

  /**
   * Creates a write-through binding.
   *
   * @param provider registered provider instance name
   * @param plaintext plaintext secret to write through
   */
  public SecretBinding(String provider, String plaintext) {
    Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
    Preconditions.checkArgument(plaintext != null, "plaintext must not be null");
    this.provider = provider;
    this.plaintext = plaintext;
  }

  /**
   * Returns the registered provider instance name.
   *
   * @return the provider name
   */
  public String provider() {
    return provider;
  }

  /**
   * Returns the plaintext secret to write through.
   *
   * @return the plaintext secret
   */
  public String plaintext() {
    return plaintext;
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
    return Objects.equals(provider, that.provider) && Objects.equals(plaintext, that.plaintext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, plaintext);
  }

  @Override
  public String toString() {
    return "SecretBinding{provider='" + provider + "', plaintext=***}";
  }
}
