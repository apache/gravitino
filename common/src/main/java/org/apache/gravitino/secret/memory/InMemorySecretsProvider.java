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
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.secret.SecretProvider;
import org.apache.gravitino.secret.SecretUrn;

/**
 * In-memory secret provider for development and unit tests only.
 *
 * <p>Secrets are stored in process memory only and are lost on restart. Values are Base64-encoded
 * for opaque storage, which is <strong>not</strong> encryption. {@link #close()} clears map
 * references but does not securely zero heap contents ({@link String} is not wipeable); that is
 * acceptable only because this backend is test-oriented. Production providers should store wipeable
 * {@code byte[]}/{@code char[]} and explicitly zero them on delete/close. Do not use this provider
 * in production.
 */
public class InMemorySecretsProvider implements SecretProvider {

  private final ConcurrentHashMap<String, String> secrets = new ConcurrentHashMap<>();
  private String providerName;

  @Override
  public void initialize(String name, Map<String, String> config) {
    if (StringUtils.isBlank(name)) {
      throw new IllegalArgumentException("provider name must not be blank");
    }
    this.providerName = name;
  }

  @Override
  public String type() {
    return "memory";
  }

  @Override
  public SecretUrn writeSecret(String plaintext, Map<String, String> attributes) {
    if (plaintext == null) {
      throw new IllegalArgumentException("plaintext must not be null");
    }
    if (providerName == null) {
      throw new IllegalStateException("InMemorySecretsProvider is not initialized");
    }

    SecretUrn urn = SecretUrn.buildWriteThrough(providerName, attributes);
    secrets.put(
        urn.toString(),
        Base64.getEncoder().encodeToString(plaintext.getBytes(StandardCharsets.UTF_8)));
    return urn;
  }

  @Override
  public String readSecret(SecretUrn urn) {
    if (urn == null) {
      throw new IllegalArgumentException("urn must not be null");
    }
    String encoded = secrets.get(urn.toString());
    if (encoded == null) {
      throw new IllegalArgumentException("Secret not found for URN: " + urn);
    }
    return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
  }

  @Override
  public void deleteSecret(SecretUrn urn) {
    if (urn == null) {
      throw new IllegalArgumentException("urn must not be null");
    }
    secrets.remove(urn.toString());
  }

  @Override
  public void close() {
    secrets.clear();
  }
}
