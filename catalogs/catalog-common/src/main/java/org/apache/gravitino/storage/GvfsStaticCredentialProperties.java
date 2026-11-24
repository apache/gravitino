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
package org.apache.gravitino.storage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Filters static cloud credentials out of REST {@code properties()} merges for GVFS
 * (dependency-light; no {@code api}).
 *
 * <p>Plaintext for hidden static credentials is recovered via {@code getSecrets()} / {@code
 * getCredentials()}. Server-side {@code getSecrets} credential-type filtering lives in {@link
 * org.apache.gravitino.secret.StaticCredentialSecretKeys}, which uses a broader key set (Glue AWS,
 * Paimon DLF, JDBC, etc.) keyed by credential-provider type. This class keeps a narrower HCFS-only
 * set and must not be unified with that map.
 *
 * <p>{@code azure-storage-account-name} is intentionally left non-hidden: it is already disclosed
 * in ADLS URIs. Only {@code azure-storage-account-key} and {@code azure-client-secret} are stripped
 * here.
 */
public final class GvfsStaticCredentialProperties {

  /** Placeholder returned for masked hidden properties in REST responses. */
  public static final String MASKED_PROPERTY_VALUE = "******";

  /**
   * Static cloud credential property keys omitted from REST {@code properties()} merges for GVFS
   * (alongside masked placeholders). Does not include Glue AWS or Paimon DLF keys (metastore, not
   * fileset HCFS config).
   */
  private static final Set<String> STATIC_CREDENTIAL_KEYS =
      ImmutableSet.of(
          S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
          S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID,
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
          AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
          AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET,
          COSProperties.GRAVITINO_COS_ACCESS_KEY_ID,
          COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET);

  private GvfsStaticCredentialProperties() {}

  /**
   * Returns whether the property key holds a static cloud credential for GVFS.
   *
   * @param key the property key
   * @return true when the key is a static credential property
   */
  public static boolean isStaticCredentialKey(@Nullable String key) {
    return key != null && STATIC_CREDENTIAL_KEYS.contains(key);
  }

  /**
   * Returns a copy of {@code properties} with static credential keys and masked placeholders
   * removed. Used when merging REST metadata into GVFS client configuration.
   *
   * @param properties source properties from REST metadata responses
   * @return filtered properties safe to pass to the underlying HCFS client
   */
  public static Map<String, String> omitStaticCredentialProperties(
      @Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return ImmutableMap.of();
    }
    Map<String, String> filtered = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (key == null || value == null) {
        continue;
      }
      if (isStaticCredentialKey(key) || MASKED_PROPERTY_VALUE.equals(value)) {
        continue;
      }
      filtered.put(key, value);
    }
    return ImmutableMap.copyOf(filtered);
  }

  /** Returns the static credential property keys for GVFS {@code properties()} filtering. */
  public static Set<String> staticCredentialKeys() {
    return STATIC_CREDENTIAL_KEYS;
  }
}
