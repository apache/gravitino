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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.credential.AwsSecretKeyCredential;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.COSSecretKeyCredential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.DlfSecretKeyCredential;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.storage.AWSProperties;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.COSProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

/**
 * Static credential type → property keys for {@code getSecrets} filtering when the caller has
 * {@code USE_SECRETS} but not {@code INCLUDE_CREDENTIAL_SECRETS}. Token providers are absent.
 *
 * <p>Includes Glue AWS, Paimon DLF, JDBC, and cloud storage static keys. GVFS only drops masked
 * {@code properties()} placeholders in {@code BaseGVFSOperations}.
 */
final class StaticCredentialSecretKeys {

  private static final Map<String, Set<String>> KEYS_BY_CREDENTIAL_TYPE =
      ImmutableMap.<String, Set<String>>builder()
          .put(
              S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  S3Properties.GRAVITINO_S3_ACCESS_KEY_ID,
                  S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY))
          .put(
              OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID,
                  OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET))
          .put(
              COSSecretKeyCredential.COS_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  COSProperties.GRAVITINO_COS_ACCESS_KEY_ID,
                  COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET))
          .put(
              AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  AWSProperties.GRAVITINO_AWS_ACCESS_KEY_ID,
                  AWSProperties.GRAVITINO_AWS_SECRET_ACCESS_KEY))
          .put(
              AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY))
          .put(
              DlfSecretKeyCredential.DLF_SECRET_KEY_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_ID,
                  PaimonConstants.GRAVITINO_DLF_ACCESS_KEY_SECRET,
                  PaimonConstants.GRAVITINO_DLF_SECURITY_TOKEN))
          .put(
              JdbcCredential.JDBC_CREDENTIAL_TYPE,
              ImmutableSet.of(
                  JdbcCredential.GRAVITINO_JDBC_USER, JdbcCredential.GRAVITINO_JDBC_PASSWORD))
          .build();

  /**
   * Union of all keys in {@link #KEYS_BY_CREDENTIAL_TYPE}, plus {@code azure-client-secret}
   * (catalog Azure AD config; no credential-provider type).
   */
  private static final Set<String> ALL_STATIC_CREDENTIAL_SECRET_KEYS;

  static {
    Set<String> all = new HashSet<>();
    for (Set<String> keys : KEYS_BY_CREDENTIAL_TYPE.values()) {
      all.addAll(keys);
    }
    all.add(AzureProperties.GRAVITINO_AZURE_CLIENT_SECRET);
    ALL_STATIC_CREDENTIAL_SECRET_KEYS = ImmutableSet.copyOf(all);
  }

  private StaticCredentialSecretKeys() {}

  /** All static credential secret keys (conservative omit when metadata is unavailable). */
  static Set<String> allKeys() {
    return ALL_STATIC_CREDENTIAL_SECRET_KEYS;
  }

  /**
   * Static credential property keys implied by a comma-separated {@code credential-providers} value
   * on {@code properties}. Token providers are ignored.
   */
  static Set<String> keysForCredentialProviders(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return ImmutableSet.of();
    }
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    if (StringUtils.isBlank(credentialProviders)) {
      return ImmutableSet.of();
    }
    Set<String> keys = new HashSet<>();
    for (String provider : credentialProviders.split(",")) {
      String type = provider.trim();
      if (type.isEmpty()) {
        continue;
      }
      Set<String> forType = KEYS_BY_CREDENTIAL_TYPE.get(type.toLowerCase(Locale.ROOT));
      if (forType != null) {
        keys.addAll(forType);
      }
    }
    return keys;
  }

  /** Returns a copy of {@code secrets} with keys in {@code keysToOmit} removed. */
  static Map<String, String> omit(
      @Nullable Map<String, String> secrets, @Nullable Set<String> keysToOmit) {
    if (secrets == null || secrets.isEmpty()) {
      return ImmutableMap.of();
    }
    Set<String> omit = keysToOmit == null ? ALL_STATIC_CREDENTIAL_SECRET_KEYS : keysToOmit;
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
}
