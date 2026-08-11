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
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/**
 * A pending write-through secret: the target {@link SecretUrn} paired with the plaintext to store.
 *
 * <p>Produced by {@code SecretManager#assembleSecretUrns} and consumed by {@code
 * SecretManager#writeSecrets}.
 */
@Evolving
public final class SecretWrite {

  private final SecretUrn urn;
  private final String plaintext;

  /**
   * Creates a pending write-through secret.
   *
   * @param urn write-through URN (provider + entity locator)
   * @param plaintext plaintext secret to write
   */
  public SecretWrite(SecretUrn urn, String plaintext) {
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

  /**
   * Pairs write-through URNs with plaintext from {@code secretBindings} by property key.
   *
   * @param urns write-through URNs (e.g. from {@code SecretManager#getSecretBindingUrns})
   * @param secretBindings property key → write-through binding
   * @return pending writes in {@code urns} order
   */
  public static List<SecretWrite> from(
      @Nullable List<SecretUrn> urns, @Nullable Map<String, SecretBinding> secretBindings) {
    if (urns == null || urns.isEmpty()) {
      Preconditions.checkArgument(
          secretBindings == null || secretBindings.isEmpty(),
          "secretBindings must be empty when urns are empty");
      return ImmutableList.of();
    }
    Preconditions.checkArgument(secretBindings != null, "secretBindings must not be null");
    List<SecretWrite> writes = new ArrayList<>(urns.size());
    for (SecretUrn urn : urns) {
      String propertyKey = urn.propertyKey();
      SecretBinding binding = secretBindings.get(propertyKey);
      Preconditions.checkArgument(
          binding != null, "No secretBindings entry for property key \"%s\"", propertyKey);
      writes.add(new SecretWrite(urn, binding.plaintext()));
    }
    return ImmutableList.copyOf(writes);
  }

  /**
   * Extracts URNs from pending writes (e.g. for rollback).
   *
   * @param writes pending writes (null or empty returns an empty list)
   * @return URNs in the same order
   */
  public static List<SecretUrn> urns(@Nullable List<SecretWrite> writes) {
    if (writes == null || writes.isEmpty()) {
      return ImmutableList.of();
    }
    List<SecretUrn> urns = new ArrayList<>(writes.size());
    for (SecretWrite write : writes) {
      urns.add(write.urn());
    }
    return ImmutableList.copyOf(urns);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SecretWrite)) {
      return false;
    }
    SecretWrite that = (SecretWrite) o;
    return Objects.equals(urn, that.urn) && Objects.equals(plaintext, that.plaintext);
  }

  @Override
  public int hashCode() {
    return Objects.hash(urn, plaintext);
  }

  @Override
  public String toString() {
    return "SecretWrite{urn=" + urn + ", plaintext=***}";
  }
}
