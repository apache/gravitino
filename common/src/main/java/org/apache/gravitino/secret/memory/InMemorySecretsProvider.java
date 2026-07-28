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

package org.apache.gravitino.secret.memory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.gravitino.secret.GravitinoSecretProvider;
import org.apache.gravitino.secret.SecretUrn;
import org.apache.gravitino.secret.SecretWriteContext;

/**
 * In-memory secret provider for development and unit tests.
 *
 * <p>Secrets are stored in process memory only and are lost on restart. Material is kept as a
 * mutable {@code byte[]} so {@link #deleteSecret(String)} and {@link #close()} can explicitly zero
 * the contents before dropping references. This is still <strong>not</strong> encryption and does
 * not wipe caller-owned {@link String} arguments or return values from {@link #writeSecret}/{@link
 * #readSecret}. Do not use this provider in production.
 */
public class InMemorySecretsProvider implements GravitinoSecretProvider {

  private final ConcurrentHashMap<String, byte[]> secrets = new ConcurrentHashMap<>();

  @Override
  public void initialize(String name, Map<String, String> config) {
    // No configuration required for the in-memory provider.
  }

  @Override
  public String type() {
    return "memory";
  }

  @Override
  public String writeSecret(String plaintext, SecretWriteContext context) {
    if (plaintext == null) {
      throw new IllegalArgumentException("plaintext must not be null");
    }
    if (context == null) {
      throw new IllegalArgumentException("context must not be null");
    }
    String urn =
        SecretUrn.buildWriteThrough(
            context.providerName(),
            context.entityType(),
            context.entityId(),
            context.propertyKey());
    // Own a mutable UTF-8 copy so delete/close can wipe the stored material.
    secrets.put(urn, plaintext.getBytes(StandardCharsets.UTF_8));
    return urn;
  }

  @Override
  public String readSecret(String urn) {
    byte[] material = secrets.get(urn);
    if (material == null) {
      throw new IllegalArgumentException("Secret not found for URN: " + urn);
    }
    return new String(material, StandardCharsets.UTF_8);
  }

  @Override
  public void deleteSecret(String urn) {
    wipeAndRemove(urn);
  }

  @Override
  public void close() {
    secrets.forEach((ignored, material) -> Arrays.fill(material, (byte) 0));
    secrets.clear();
  }

  /**
   * Returns the stored secret bytes for {@code urn}, or {@code null} if absent. Visible for tests.
   *
   * @param urn the secret URN
   * @return the stored byte array reference, or null
   */
  byte[] storedBytes(String urn) {
    return secrets.get(urn);
  }

  private void wipeAndRemove(String urn) {
    byte[] material = secrets.remove(urn);
    if (material != null) {
      Arrays.fill(material, (byte) 0);
    }
  }
}
