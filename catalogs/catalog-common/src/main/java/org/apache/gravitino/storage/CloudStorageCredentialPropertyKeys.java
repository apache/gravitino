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
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;

/**
 * Gravitino property keys for cloud static credentials (access-key pairs and related secrets).
 *
 * <p>GVFS must not consume these keys from REST catalog/schema/fileset {@code properties()}
 * responses (which may be masked as {@code ******}). Plaintext for hidden static credentials,
 * including {@code s3-access-key-id}, is recovered via {@code getSecrets()} when the caller holds
 * {@code USE_SECRETS} and {@code INCLUDE_CREDENTIAL_SECRETS}, or via {@code getCredentials()}.
 * Catalog credential vending requires both privileges or metalake ownership, in addition to
 * metadata access; fileset credential vending uses path-based authorization. Masked placeholders
 * are dropped by {@link #omitStaticCredentialProperties}. Callers with only {@code USE_SECRETS}
 * receive {@code getSecrets()} with object-scoped static credential properties omitted via {@link
 * #omitCloudAccessKeyPairProperties(Map, Set)}.
 *
 * <p>{@code azure-storage-account-name} is intentionally left non-hidden (and not listed here):
 * unlike S3/OSS/COS access key IDs, the account name is already disclosed in ADLS URIs such as
 * {@code abfss://container@account.dfs.core.windows.net}, so masking it on load/list adds no
 * confidentiality. Only {@code azure-storage-account-key} is treated as a secret half.
 *
 * <p>Credential-type → property-key mapping for {@code getSecrets} filtering lives in server {@code
 * core} (needs {@code api} credential-type constants); this class stays dependency-light for GVFS
 * and catalog clients.
 */
public final class CloudStorageCredentialPropertyKeys {

  /** Placeholder returned for masked hidden properties in REST responses. */
  public static final String MASKED_PROPERTY_VALUE = "******";

  /** JDBC username property used by the {@code jdbc-user-password} credential provider. */
  public static final String JDBC_USER = "jdbc-user";

  /** JDBC password property used by the {@code jdbc-user-password} credential provider. */
  public static final String JDBC_PASSWORD = "jdbc-password";

  /**
   * Static cloud credential property keys omitted from REST {@code properties()} merges for GVFS
   * (alongside masked placeholders).
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

  /**
   * Static credential property keys omitted from {@code getSecrets()} when the caller has {@code
   * USE_SECRETS} but not {@code INCLUDE_CREDENTIAL_SECRETS}. Includes cloud access-key pairs, Azure
   * client secret, DLF static secrets, and JDBC login properties that correspond to static
   * credential providers. Short-lived token credentials are out of scope for this list.
   */
  private static final Set<String> CLOUD_ACCESS_KEY_PAIR_KEYS =
      ImmutableSet.of(
          S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
          S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID,
          OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
          COSProperties.GRAVITINO_COS_ACCESS_KEY_ID,
          COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET,
          AWSProperties.GRAVITINO_AWS_ACCESS_KEY_ID,
          AWSProperties.GRAVITINO_AWS_SECRET_ACCESS_KEY,
          AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
          AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET,
          PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID,
          PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET,
          PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN,
          JDBC_USER,
          JDBC_PASSWORD);

  private CloudStorageCredentialPropertyKeys() {}

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
   * Returns whether the property key is a static credential secret that {@code USE_SECRETS} must
   * not see via {@code getSecrets()} without {@code INCLUDE_CREDENTIAL_SECRETS}.
   *
   * @param key the property key
   * @return true when the key must be omitted for use-secret-only callers
   */
  public static boolean isCloudAccessKeyPairKey(@Nullable String key) {
    return key != null && CLOUD_ACCESS_KEY_PAIR_KEYS.contains(key);
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

  /**
   * Returns a copy of {@code secrets} with every static credential key removed. Prefer {@link
   * #omitCloudAccessKeyPairProperties(Map, Set)} with an object-scoped key set so the same key name
   * (for example {@code jdbc-user}) is omitted only when it is a credential property for that
   * metadata object.
   *
   * @param secrets plaintext secrets map
   * @return secrets without static credential properties
   */
  public static Map<String, String> omitCloudAccessKeyPairProperties(
      @Nullable Map<String, String> secrets) {
    return omitCloudAccessKeyPairProperties(secrets, CLOUD_ACCESS_KEY_PAIR_KEYS);
  }

  /**
   * Returns a copy of {@code secrets} with keys in {@code keysToOmit} removed. Callers that know
   * which static credential keys apply to the current metadata object (declared in {@code
   * PropertiesMetadata} and/or enabled via static {@code credential-providers}) should pass that
   * scoped set so undeclared same-named properties are not stripped.
   *
   * @param secrets plaintext secrets map
   * @param keysToOmit keys to drop; {@code null} omits the full static-credential key set
   * @return secrets without the given keys
   */
  public static Map<String, String> omitCloudAccessKeyPairProperties(
      @Nullable Map<String, String> secrets, @Nullable Set<String> keysToOmit) {
    if (secrets == null || secrets.isEmpty()) {
      return ImmutableMap.of();
    }
    Set<String> omit = keysToOmit == null ? CLOUD_ACCESS_KEY_PAIR_KEYS : keysToOmit;
    if (omit.isEmpty()) {
      return ImmutableMap.copyOf(secrets);
    }
    Map<String, String> filtered = new HashMap<>();
    for (Map.Entry<String, String> entry : secrets.entrySet()) {
      String key = entry.getKey();
      if (key == null || omit.contains(key)) {
        continue;
      }
      filtered.put(key, entry.getValue());
    }
    return ImmutableMap.copyOf(filtered);
  }

  /** Returns the static credential property keys. */
  public static Set<String> staticCredentialKeys() {
    return STATIC_CREDENTIAL_KEYS;
  }

  /**
   * Returns static credential keys omitted for {@code USE_SECRETS}-only {@code getSecrets} callers.
   */
  public static Set<String> cloudAccessKeyPairKeys() {
    return CLOUD_ACCESS_KEY_PAIR_KEYS;
  }
}
