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
import org.apache.gravitino.annotation.Evolving;

/**
 * Write-through secret material: the target {@link SecretUrn} paired with the plaintext to store.
 *
 * <p>Internal to the secrets write path in {@code core}. Produced by {@link
 * SecretManager#assembleSecretMaterials} and consumed by {@link SecretManager#writeSecrets} /
 * {@link SecretManager#rollbackSecrets}.
 */
@Evolving
public final class SecretMaterial {

  private final SecretUrn urn;
  private final String plaintext;

  /**
   * Creates secret material for a write-through secret.
   *
   * @param urn write-through URN (provider + entity locator)
   * @param plaintext plaintext secret to write
   */
  public SecretMaterial(SecretUrn urn, String plaintext) {
    Preconditions.checkArgument(urn != null, "urn must not be null");
    Preconditions.checkArgument(plaintext != null, "plaintext must not be null");
    this.urn = urn;
    this.plaintext = plaintext;
  }

  /**
   * Returns the write-through URN.
   *
   * @return the URN
   */
  public SecretUrn urn() {
    return urn;
  }

  /**
   * Returns the plaintext secret to write.
   *
   * @return the plaintext
   */
  public String plaintext() {
    return plaintext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SecretMaterial)) {
      return false;
    }
    SecretMaterial that = (SecretMaterial) o;
    return Objects.equals(urn, that.urn) && Objects.equals(plaintext, that.plaintext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn, plaintext);
  }

  @Override
  public String toString() {
    return "SecretMaterial{urn=" + urn + ", plaintext=***}";
  }
}
