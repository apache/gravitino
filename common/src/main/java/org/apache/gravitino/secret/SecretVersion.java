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
import org.apache.gravitino.annotation.DeveloperApi;

/**
 * Result of writing a secret version: the durable {@link SecretUrn} and the provider version id.
 *
 * <p>The URN identifies the logical secret (fixed across rotates). The version id identifies one
 * immutable payload under that URN, matching Vault / AWS / GCP multi-version stores.
 */
@DeveloperApi
public final class SecretVersion {

  private final SecretUrn urn;
  private final String version;

  /**
   * Creates a secret version result.
   *
   * @param urn the secret URN
   * @param version the provider version id
   */
  public SecretVersion(SecretUrn urn, String version) {
    Preconditions.checkArgument(urn != null, "urn must not be null");
    Preconditions.checkArgument(version != null && !version.isEmpty(), "version must not be blank");
    this.urn = urn;
    this.version = version;
  }

  /**
   * Returns the secret URN.
   *
   * @return the URN
   */
  public SecretUrn urn() {
    return urn;
  }

  /**
   * Returns the provider version id.
   *
   * @return the version
   */
  public String version() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SecretVersion)) {
      return false;
    }
    SecretVersion that = (SecretVersion) o;
    return Objects.equals(urn, that.urn) && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn, version);
  }

  @Override
  public String toString() {
    return "SecretVersion{urn=" + urn + ", version=" + version + '}';
  }
}
