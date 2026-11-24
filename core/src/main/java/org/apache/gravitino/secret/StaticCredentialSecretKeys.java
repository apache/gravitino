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
import org.apache.gravitino.storage.GvfsStaticCredentialProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

/**
 * Static credential types and property keys for {@code getSecrets} authorization filtering ({@code
 * INCLUDE_CREDENTIAL_SECRETS}). Lives in {@code core} with access to {@code api} credential-type
 * constants.
 *
 * <p>Key set is broader than GVFS HCFS stripping in {@link GvfsStaticCredentialProperties}
 * (includes Glue AWS, Paimon DLF, JDBC, etc.) and is indexed by credential-provider type. Do not
 * merge the two classes: GVFS must stay dependency-light and must not omit metastore-only keys from
 * {@code properties()} merges.
 */
public final class StaticCredentialSecretKeys {

  /** JDBC username property used by the {@code jdbc-user-password} credential provider. */
  public static final String JDBC_USER = JdbcCredential.GRAVITINO_JDBC_USER;

  /** JDBC password property used by the {@code jdbc-user-password} credential provider. */
  public static final String JDBC_PASSWORD = JdbcCredential.GRAVITINO_JDBC_PASSWORD;

  /**
   * Static credential type → property keys omitted from {@code getSecrets()} when the caller has
   * {@code USE_SECRETS} but not {@code INCLUDE_CREDENTIAL_SECRETS}. Map keys are
   * credential-provider types ({@link
   * org.apache.gravitino.credential.Credential#credentialType()}). Token providers are absent.
   */
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
          .put(JdbcCredential.JDBC_CREDENTIAL_TYPE, ImmutableSet.of(JDBC_USER, JDBC_PASSWORD))
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

  /**
   * Returns whether {@code key} is a static credential secret for {@code getSecrets} without {@code
   * INCLUDE_CREDENTIAL_SECRETS}.
   *
   * @param key property key
   * @return true when the key is in the derived static-credential secret set
   */
  public static boolean isStaticCredentialSecretKey(@Nullable String key) {
    return key != null && ALL_STATIC_CREDENTIAL_SECRET_KEYS.contains(key);
  }

  /**
   * Returns property keys for a static credential type, or an empty set for unknown or token types.
   *
   * @param credentialType credential-provider type
   * @return property keys for that type; never null
   */
  public static Set<String> keysForCredentialType(@Nullable String credentialType) {
    if (credentialType == null || credentialType.isEmpty()) {
      return ImmutableSet.of();
    }
    Set<String> keys = KEYS_BY_CREDENTIAL_TYPE.get(credentialType.toLowerCase(Locale.ROOT));
    return keys == null ? ImmutableSet.of() : keys;
  }

  /**
   * Returns static credential property keys implied by a comma-separated {@code
   * credential-providers} value. Token providers are ignored.
   *
   * @param credentialProviders raw {@code credential-providers} value, or null
   * @return union of keys for enabled static types; never null
   */
  public static Set<String> keysForCredentialProviders(@Nullable String credentialProviders) {
    if (StringUtils.isBlank(credentialProviders)) {
      return ImmutableSet.of();
    }
    Set<String> keys = new HashSet<>();
    for (String provider : credentialProviders.split(",")) {
      keys.addAll(keysForCredentialType(provider.trim()));
    }
    return keys;
  }

  /**
   * Returns static credential keys from {@code credential-providers} on {@code properties}.
   *
   * @param properties entity properties, or null
   * @return union of keys for enabled static types; never null
   */
  public static Set<String> keysForCredentialProviders(@Nullable Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return ImmutableSet.of();
    }
    return keysForCredentialProviders(properties.get(CredentialConstants.CREDENTIAL_PROVIDERS));
  }

  /**
   * Returns all static credential secret keys for conservative {@code getSecrets} omit (when object
   * metadata is unavailable).
   */
  public static Set<String> staticCredentialSecretKeys() {
    return ALL_STATIC_CREDENTIAL_SECRET_KEYS;
  }

  /**
   * Returns a copy of {@code secrets} with every static credential secret key removed. Prefer
   * {@link #omitStaticCredentialSecrets(Map, Set)} with an object-scoped key set.
   *
   * @param secrets plaintext secrets
   * @return filtered secrets
   */
  public static Map<String, String> omitStaticCredentialSecrets(
      @Nullable Map<String, String> secrets) {
    return omitStaticCredentialSecrets(secrets, ALL_STATIC_CREDENTIAL_SECRET_KEYS);
  }

  /**
   * Returns a copy of {@code secrets} with keys in {@code keysToOmit} removed.
   *
   * @param secrets plaintext secrets
   * @param keysToOmit keys to drop; {@code null} uses the full derived set
   * @return filtered secrets
   */
  public static Map<String, String> omitStaticCredentialSecrets(
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
