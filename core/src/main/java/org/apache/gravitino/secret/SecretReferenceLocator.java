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

import java.util.Objects;
import javax.annotation.Nullable;

/** Locator for an externally managed secret referenced at entity create time. */
public final class SecretReferenceLocator {

  private final String provider;
  @Nullable private final String mount;
  @Nullable private final String path;

  /**
   * Creates a secret reference locator.
   *
   * @param provider the configured secret provider name
   * @param mount optional mount or namespace within the external store
   * @param path optional path to the secret within the external store
   */
  public SecretReferenceLocator(String provider, @Nullable String mount, @Nullable String path) {
    this.provider = provider;
    this.mount = mount;
    this.path = path;
  }

  /**
   * Returns the configured secret provider name.
   *
   * @return the provider name
   */
  public String provider() {
    return provider;
  }

  /**
   * Returns the optional mount or namespace.
   *
   * @return the mount, or {@code null}
   */
  @Nullable
  public String mount() {
    return mount;
  }

  /**
   * Returns the optional secret path.
   *
   * @return the path, or {@code null}
   */
  @Nullable
  public String path() {
    return path;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof SecretReferenceLocator)) {
      return false;
    }
    SecretReferenceLocator that = (SecretReferenceLocator) other;
    return Objects.equals(provider, that.provider)
        && Objects.equals(mount, that.mount)
        && Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, mount, path);
  }
}
