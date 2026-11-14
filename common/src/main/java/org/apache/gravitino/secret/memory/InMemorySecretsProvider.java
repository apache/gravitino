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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
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
 *
 * <p>Each URN keeps its versions and a pointer to the current one, which {@link #readSecret}
 * returns. {@link #writeSecret} replaces every version of the URN.
 */
public class InMemorySecretsProvider implements SecretProvider {

  private final ConcurrentHashMap<String, Versions> secrets = new ConcurrentHashMap<>();
  private final AtomicLong lastVersion = new AtomicLong();
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
    String version = nextVersion();
    secrets.put(urn.toString(), Versions.EMPTY.with(version, encode(plaintext), true));
    return urn;
  }

  @Override
  public String readSecret(SecretUrn urn) {
    Versions versions = secrets.get(checkUrn(urn).toString());
    if (versions == null || versions.current == null) {
      throw new IllegalArgumentException("Secret not found for URN: " + urn);
    }
    String encoded = versions.encoded.get(versions.current);
    return new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
  }

  @Override
  public void deleteSecret(SecretUrn urn) {
    secrets.remove(checkUrn(urn).toString());
  }

  @Override
  public String writeSecretVersion(SecretUrn urn, String plaintext) {
    checkUrn(urn);
    if (plaintext == null) {
      throw new IllegalArgumentException("plaintext must not be null");
    }
    if (providerName == null) {
      throw new IllegalStateException("InMemorySecretsProvider is not initialized");
    }
    if (!urn.providerName().equals(providerName)) {
      throw new IllegalArgumentException(
          "URN " + urn + " does not belong to secret provider " + providerName);
    }

    String version = nextVersion();
    String encoded = encode(plaintext);
    secrets.compute(
        urn.toString(),
        (key, versions) -> {
          Versions existing = Versions.of(versions);
          return existing.with(version, encoded, existing.current == null);
        });
    return version;
  }

  @Override
  public void setCurrentSecretVersion(SecretUrn urn, String version) {
    checkUrn(urn);
    checkVersion(version);
    secrets.compute(
        urn.toString(),
        (key, versions) -> {
          if (versions == null || !versions.encoded.containsKey(version)) {
            throw new IllegalArgumentException(
                "Secret version " + version + " not found for URN: " + urn);
          }
          return versions.withCurrent(version);
        });
  }

  @Override
  public void deleteSecretVersion(SecretUrn urn, String version) {
    checkUrn(urn);
    checkVersion(version);
    secrets.computeIfPresent(urn.toString(), (key, versions) -> versions.without(version));
  }

  @Override
  @Nullable
  public String currentSecretVersion(SecretUrn urn) {
    Versions versions = secrets.get(checkUrn(urn).toString());
    return versions == null ? null : versions.current;
  }

  @Override
  public void close() {
    secrets.clear();
  }

  private String nextVersion() {
    return String.valueOf(lastVersion.incrementAndGet());
  }

  private static String encode(String plaintext) {
    return Base64.getEncoder().encodeToString(plaintext.getBytes(StandardCharsets.UTF_8));
  }

  private static SecretUrn checkUrn(SecretUrn urn) {
    if (urn == null) {
      throw new IllegalArgumentException("urn must not be null");
    }
    return urn;
  }

  private static void checkVersion(String version) {
    if (StringUtils.isBlank(version)) {
      throw new IllegalArgumentException("version must not be blank");
    }
  }

  /** Immutable versions of one URN; every change returns a new instance. */
  private static final class Versions {
    private static final Versions EMPTY = new Versions(Collections.emptyMap(), null);

    private final Map<String, String> encoded;
    @Nullable private final String current;

    private Versions(Map<String, String> encoded, @Nullable String current) {
      this.encoded = encoded;
      this.current = current;
    }

    private static Versions of(@Nullable Versions versions) {
      return versions == null ? EMPTY : versions;
    }

    private Versions with(String version, String value, boolean makeCurrent) {
      Map<String, String> updated = new HashMap<>(encoded);
      updated.put(version, value);
      return new Versions(Collections.unmodifiableMap(updated), makeCurrent ? version : current);
    }

    private Versions withCurrent(String version) {
      return new Versions(encoded, version);
    }

    /** Returns {@code null} when no version is left, so the map entry is removed. */
    @Nullable
    private Versions without(String version) {
      if (!encoded.containsKey(version)) {
        return this;
      }
      Map<String, String> updated = new HashMap<>(encoded);
      updated.remove(version);
      if (updated.isEmpty()) {
        return null;
      }
      return new Versions(
          Collections.unmodifiableMap(updated), version.equals(current) ? null : current);
    }
  }
}
