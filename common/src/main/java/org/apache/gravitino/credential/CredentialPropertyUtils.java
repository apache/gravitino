/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Helper class to generate specific credential properties for different table format and engine.
 */
public class CredentialPropertyUtils {

  // S3 / AWS Iceberg property names
  @VisibleForTesting static final String ICEBERG_S3_ACCESS_KEY_ID = "s3.access-key-id";
  @VisibleForTesting static final String ICEBERG_S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  @VisibleForTesting static final String ICEBERG_S3_TOKEN = "s3.session-token";

  @VisibleForTesting
  static final String ICEBERG_S3_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";

  // OSS Iceberg property names
  @VisibleForTesting static final String ICEBERG_OSS_ACCESS_KEY_ID = "client.access-key-id";
  @VisibleForTesting static final String ICEBERG_OSS_ACCESS_KEY_SECRET = "client.access-key-secret";
  @VisibleForTesting static final String ICEBERG_OSS_SECURITY_TOKEN = "client.security-token";

  @VisibleForTesting
  static final String ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS =
      "client.security-token-expires-at-ms";

  // GCS Iceberg property names
  @VisibleForTesting static final String ICEBERG_GCS_TOKEN = "gcs.oauth2.token";

  @VisibleForTesting
  static final String ICEBERG_GCS_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";

  // ADLS Iceberg property names
  @VisibleForTesting static final String ICEBERG_ADLS_TOKEN = "adls.sas-token";

  @VisibleForTesting
  static final String ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX = "adls.sas-token-expires-at-ms.";

  @VisibleForTesting
  static final String ICEBERG_ADLS_ACCOUNT_NAME = "adls.auth.shared-key.account.name";

  @VisibleForTesting
  static final String ICEBERG_ADLS_ACCOUNT_KEY = "adls.auth.shared-key.account.key";

  private static final Map<String, String> ICEBERG_S3_CREDENTIAL_PROPERTY_MAP =
      ImmutableMap.<String, String>builder()
          .put(S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID, ICEBERG_S3_ACCESS_KEY_ID)
          .put(
              S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
              ICEBERG_S3_SECRET_ACCESS_KEY)
          .put(S3TokenCredential.GRAVITINO_S3_TOKEN, ICEBERG_S3_TOKEN)
          .put(AwsIrsaCredential.ACCESS_KEY_ID, ICEBERG_S3_ACCESS_KEY_ID)
          .put(AwsIrsaCredential.SECRET_ACCESS_KEY, ICEBERG_S3_SECRET_ACCESS_KEY)
          .put(AwsIrsaCredential.SESSION_TOKEN, ICEBERG_S3_TOKEN)
          .build();

  private static final Map<String, String> ICEBERG_OSS_CREDENTIAL_PROPERTY_MAP =
      ImmutableMap.<String, String>builder()
          .put(OSSTokenCredential.GRAVITINO_OSS_TOKEN, ICEBERG_OSS_SECURITY_TOKEN)
          .put(OSSTokenCredential.GRAVITINO_OSS_SESSION_ACCESS_KEY_ID, ICEBERG_OSS_ACCESS_KEY_ID)
          .put(
              OSSTokenCredential.GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY,
              ICEBERG_OSS_ACCESS_KEY_SECRET)
          .build();

  private static final Map<String, String> ICEBERG_GCS_CREDENTIAL_PROPERTY_MAP =
      ImmutableMap.of(GCSTokenCredential.GCS_TOKEN_NAME, ICEBERG_GCS_TOKEN);

  private static final Map<String, String> ICEBERG_ADLS_CREDENTIAL_PROPERTY_MAP =
      ImmutableMap.<String, String>builder()
          .put(
              AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
              ICEBERG_ADLS_ACCOUNT_NAME)
          .put(
              AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
              ICEBERG_ADLS_ACCOUNT_KEY)
          .build();

  private static final Map<String, String> icebergCredentialPropertyMap =
      ImmutableMap.<String, String>builder()
          .putAll(ICEBERG_S3_CREDENTIAL_PROPERTY_MAP)
          .putAll(ICEBERG_OSS_CREDENTIAL_PROPERTY_MAP)
          .putAll(ICEBERG_GCS_CREDENTIAL_PROPERTY_MAP)
          .putAll(ICEBERG_ADLS_CREDENTIAL_PROPERTY_MAP)
          .build();

  /**
   * Transforms a specific credential into a map of Iceberg properties.
   *
   * @param credential the credential to be transformed into Iceberg properties
   * @return a map of Iceberg properties derived from the credential
   */
  public static Map<String, String> toIcebergProperties(Credential credential) {
    if (credential instanceof S3TokenCredential || credential instanceof AwsIrsaCredential) {
      return withExpireAt(
          transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap),
          ICEBERG_S3_TOKEN_EXPIRES_AT_MS,
          credential.expireTimeInMs());
    }
    if (credential instanceof S3SecretKeyCredential) {
      return transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap);
    }

    if (credential instanceof OSSTokenCredential) {
      return withExpireAt(
          transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap),
          ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS,
          credential.expireTimeInMs());
    }
    if (credential instanceof OSSSecretKeyCredential) {
      return transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap);
    }

    if (credential instanceof GCSTokenCredential) {
      return withExpireAt(
          transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap),
          ICEBERG_GCS_TOKEN_EXPIRES_AT,
          credential.expireTimeInMs());
    }

    if (credential instanceof ADLSTokenCredential) {
      return toAdlsTokenProperties((ADLSTokenCredential) credential);
    }
    if (credential instanceof AzureAccountKeyCredential) {
      return transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap);
    }

    return credential.toProperties();
  }

  private static Map<String, String> withExpireAt(
      Map<String, String> properties, String expireKey, long expireTimeMs) {
    Map<String, String> result = new HashMap<>(properties);
    result.put(expireKey, String.valueOf(expireTimeMs));
    return result;
  }

  private static Map<String, String> toAdlsTokenProperties(ADLSTokenCredential adlsCredential) {
    String sasTokenKey =
        String.format(
            "%s.%s.%s",
            ICEBERG_ADLS_TOKEN, adlsCredential.accountName(), ADLSTokenCredential.ADLS_DOMAIN);
    String sasTokenExpiresAtKey =
        ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + adlsCredential.accountName();

    Map<String, String> properties = new HashMap<>();
    properties.put(sasTokenKey, adlsCredential.sasToken());
    properties.put(sasTokenExpiresAtKey, String.valueOf(adlsCredential.expireTimeInMs()));
    return properties;
  }

  /**
   * Filters a property map down to only Iceberg credential-related keys.
   *
   * <p>This is used when an Iceberg table load response contains many table and catalog properties,
   * but the caller only needs the temporary credential properties that should be forwarded to the
   * Iceberg client.
   *
   * @param properties the source properties to filter
   * @return a map containing only credential properties recognized by Iceberg
   */
  public static Map<String, String> filterCredentialProperties(Map<String, String> properties) {
    Set<String> credentialPropertyKeys = Sets.newHashSet(icebergCredentialPropertyMap.values());
    credentialPropertyKeys.add(ICEBERG_S3_TOKEN_EXPIRES_AT_MS);
    credentialPropertyKeys.add(ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS);
    credentialPropertyKeys.add(ICEBERG_GCS_TOKEN_EXPIRES_AT);
    Map<String, String> filteredProperties = Maps.newHashMap(properties);
    filteredProperties
        .entrySet()
        .removeIf(
            entry ->
                !credentialPropertyKeys.contains(entry.getKey())
                    && !entry.getKey().startsWith(ICEBERG_ADLS_TOKEN)
                    && !entry.getKey().startsWith(ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX));
    return filteredProperties;
  }

  private static Map<String, String> transformProperties(
      Map<String, String> originProperties, Map<String, String> transformMap) {
    HashMap<String, String> properties = new HashMap();
    originProperties.forEach(
        (k, v) -> {
          if (transformMap.containsKey(k)) {
            properties.put(transformMap.get(k), v);
          }
        });
    return properties;
  }
}
