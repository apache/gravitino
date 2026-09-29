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
 * Gravitino property keys for cloud static credentials that must not be taken from REST {@code
 * properties()} responses.
 *
 * <p>GVFS must not consume static credential keys from REST catalog/schema/fileset {@code
 * properties()} responses (which may be masked as {@code ******}). Plaintext for S3/OSS/COS access
 * key pairs and Azure storage account keys is recovered via {@code getCredentials()}. Masked
 * placeholders are dropped by {@link #omitStaticCredentialProperties}. Non-credential configuration
 * such as {@code gcs-service-account-file} may still appear in {@code properties()}.
 *
 * <p>{@code azure-storage-account-name} is intentionally left non-hidden (and not listed here):
 * unlike S3/OSS/COS access key IDs, the account name is already disclosed in ADLS URIs such as
 * {@code abfss://container@account.dfs.core.windows.net}, so masking it on load/list adds no
 * confidentiality. Only {@code azure-storage-account-key} is treated as a secret half.
 *
 * <p>Azure client secrets that are not credential-vending keys remain omitted here; recover them
 * via {@code getSecrets()} when needed.
 */
public final class CloudStorageCredentialPropertyKeys {

  /** Placeholder returned for masked hidden properties in REST responses. */
  public static final String MASKED_PROPERTY_VALUE = "******";

  /**
   * Static cloud credential property keys omitted from REST {@code properties()} merges. Access key
   * IDs and secret halves that belong to credential vending are recovered via {@code
   * getCredentials()}.
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

  /** Returns the static credential property keys. */
  public static Set<String> staticCredentialKeys() {
    return STATIC_CREDENTIAL_KEYS;
  }
}
