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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class to generate specific credential properties for different table format and engine.
 */
public class CredentialPropertyUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CredentialPropertyUtils.class);

  @VisibleForTesting static final String ICEBERG_S3_ACCESS_KEY_ID = "s3.access-key-id";
  @VisibleForTesting static final String ICEBERG_S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  @VisibleForTesting static final String ICEBERG_S3_TOKEN = "s3.session-token";

  @VisibleForTesting
  static final String ICEBERG_S3_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";

  @VisibleForTesting
  static final String ICEBERG_CLIENT_REFRESH_CREDENTIALS_ENDPOINT =
      "client.refresh-credentials-endpoint";

  @VisibleForTesting static final String ICEBERG_OSS_ACCESS_KEY_ID = "client.access-key-id";
  @VisibleForTesting static final String ICEBERG_OSS_ACCESS_KEY_SECRET = "client.access-key-secret";
  @VisibleForTesting static final String ICEBERG_OSS_SECURITY_TOKEN = "client.security-token";

  @VisibleForTesting
  static final String ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS =
      "client.security-token-expires-at-ms";

  @VisibleForTesting static final String ICEBERG_ADLS_TOKEN = "adls.sas-token";

  @VisibleForTesting
  static final String ICEBERG_ADLS_ACCOUNT_NAME = "adls.auth.shared-key.account.name";

  @VisibleForTesting
  static final String ICEBERG_ADLS_ACCOUNT_KEY = "adls.auth.shared-key.account.key";

  @VisibleForTesting
  static final String ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX = "adls.sas-token-expires-at-ms.";

  @VisibleForTesting
  static final String ICEBERG_ADLS_REFRESH_CREDENTIALS_ENDPOINT =
      "adls.refresh-credentials-endpoint";

  @VisibleForTesting static final String ICEBERG_GCS_TOKEN = "gcs.oauth2.token";

  @VisibleForTesting
  static final String ICEBERG_GCS_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";

  @VisibleForTesting
  static final String ICEBERG_GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT =
      "gcs.oauth2.refresh-credentials-endpoint";

  @VisibleForTesting static final String ICEBERG_JDBC_USER = "jdbc.user";
  @VisibleForTesting static final String ICEBERG_JDBC_PASSWORD = "jdbc.password";

  private static final String PAIMON_S3_ACCESS_KEY = "s3.access-key";
  private static final String PAIMON_S3_SECRET_KEY = "s3.secret-key";
  private static final String PAIMON_OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
  private static final String PAIMON_OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";

  private static Map<String, String> icebergCredentialPropertyMap =
      ImmutableMap.<String, String>builder()
          .put(GCSTokenCredential.GCS_TOKEN_NAME, ICEBERG_GCS_TOKEN)
          .put(S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID, ICEBERG_S3_ACCESS_KEY_ID)
          .put(
              S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
              ICEBERG_S3_SECRET_ACCESS_KEY)
          .put(S3TokenCredential.GRAVITINO_S3_TOKEN, ICEBERG_S3_TOKEN)
          .put(OSSTokenCredential.GRAVITINO_OSS_TOKEN, ICEBERG_OSS_SECURITY_TOKEN)
          .put(OSSTokenCredential.GRAVITINO_OSS_SESSION_ACCESS_KEY_ID, ICEBERG_OSS_ACCESS_KEY_ID)
          .put(
              OSSTokenCredential.GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY,
              ICEBERG_OSS_ACCESS_KEY_SECRET)
          .put(
              AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
              ICEBERG_ADLS_ACCOUNT_NAME)
          .put(
              AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
              ICEBERG_ADLS_ACCOUNT_KEY)
          .put(AwsIrsaCredential.ACCESS_KEY_ID, ICEBERG_S3_ACCESS_KEY_ID)
          .put(AwsIrsaCredential.SECRET_ACCESS_KEY, ICEBERG_S3_SECRET_ACCESS_KEY)
          .put(AwsIrsaCredential.SESSION_TOKEN, ICEBERG_S3_TOKEN)
          .build();

  /**
   * Returns credentials from the given catalog, or an empty array if the catalog does not support
   * credential vending.
   *
   * @param catalog the Gravitino catalog client
   * @return credentials, never null
   */
  public static Credential[] getCredentials(Catalog catalog) {
    try {
      SupportsCredentials supportsCredentials = catalog.supportsCredentials();
      if (supportsCredentials == null) {
        LOG.debug("Catalog returns null SupportsCredentials, skipping credential injection");
        return new Credential[0];
      }
      return supportsCredentials.getCredentials();
    } catch (UnsupportedOperationException e) {
      LOG.debug("Catalog does not support credential vending, skipping injection", e);
      return new Credential[0];
    }
  }

  /**
   * Injects vended credentials into Iceberg catalog properties. JDBC credentials are applied
   * directly; all other credential types are converted via {@link
   * #toIcebergProperties(Credential)}.
   *
   * @param credentials the credentials to apply
   * @param props the mutable properties map to update
   */
  public static void applyIcebergCredentials(Credential[] credentials, Map<String, String> props) {
    for (Credential credential : credentials) {
      if (credential instanceof JdbcCredential) {
        JdbcCredential jdbc = (JdbcCredential) credential;
        props.put(ICEBERG_JDBC_USER, jdbc.jdbcUser());
        props.put(ICEBERG_JDBC_PASSWORD, jdbc.jdbcPassword());
      } else {
        Map<String, String> converted = toIcebergProperties(credential);
        if (converted.isEmpty()) {
          LOG.warn(
              "Received unrecognized credential type '{}' for Iceberg catalog, skipping",
              credential.getClass().getName());
        } else {
          props.putAll(converted);
        }
      }
    }
  }

  /**
   * Injects vended credentials into Paimon catalog properties. Supports JDBC, S3, and OSS
   * credential types.
   *
   * @param credentials the credentials to apply
   * @param props the mutable properties map to update
   */
  public static void applyPaimonCredentials(Credential[] credentials, Map<String, String> props) {
    for (Credential credential : credentials) {
      if (credential instanceof JdbcCredential) {
        JdbcCredential jdbc = (JdbcCredential) credential;
        props.put(ICEBERG_JDBC_USER, jdbc.jdbcUser());
        props.put(ICEBERG_JDBC_PASSWORD, jdbc.jdbcPassword());
      } else if (credential instanceof S3SecretKeyCredential) {
        S3SecretKeyCredential s3 = (S3SecretKeyCredential) credential;
        props.put(PAIMON_S3_ACCESS_KEY, s3.accessKeyId());
        props.put(PAIMON_S3_SECRET_KEY, s3.secretAccessKey());
      } else if (credential instanceof OSSSecretKeyCredential) {
        OSSSecretKeyCredential oss = (OSSSecretKeyCredential) credential;
        props.put(PAIMON_OSS_ACCESS_KEY_ID, oss.accessKeyId());
        props.put(PAIMON_OSS_ACCESS_KEY_SECRET, oss.secretAccessKey());
      } else {
        LOG.warn(
            "Received unrecognized credential type '{}' for Paimon catalog, skipping",
            credential.getClass().getName());
      }
    }
  }

  /**
   * Transforms a specific credential into a map of Iceberg properties.
   *
   * @param credential the credential to be transformed into Iceberg properties
   * @return a map of Iceberg properties derived from the credential
   */
  public static Map<String, String> toIcebergProperties(Credential credential) {
    if (credential instanceof ADLSTokenCredential) {
      ADLSTokenCredential adlsCredential = (ADLSTokenCredential) credential;
      String adlsHost = adlsCredential.accountName() + "." + ADLSTokenCredential.ADLS_DOMAIN;

      Map<String, String> icebergADLSCredentialProperties = new HashMap<>();
      icebergADLSCredentialProperties.put(
          ICEBERG_ADLS_TOKEN + "." + adlsHost, adlsCredential.sasToken());
      icebergADLSCredentialProperties.put(
          ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + adlsHost,
          String.valueOf(adlsCredential.expireTimeInMs()));
      return icebergADLSCredentialProperties;
    }

    Map<String, String> icebergProperties =
        transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap);
    if (credential instanceof S3TokenCredential || credential instanceof AwsIrsaCredential) {
      icebergProperties.put(
          ICEBERG_S3_TOKEN_EXPIRES_AT_MS, String.valueOf(credential.expireTimeInMs()));
      return icebergProperties;
    } else if (credential instanceof OSSTokenCredential) {
      icebergProperties.put(
          ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS, String.valueOf(credential.expireTimeInMs()));
      return icebergProperties;
    } else if (credential instanceof GCSTokenCredential) {
      icebergProperties.put(
          ICEBERG_GCS_TOKEN_EXPIRES_AT, String.valueOf(credential.expireTimeInMs()));
      return icebergProperties;
    } else if (credential instanceof S3SecretKeyCredential
        || credential instanceof OSSSecretKeyCredential
        || credential instanceof AzureAccountKeyCredential) {
      return icebergProperties;
    } else {
      return credential.toProperties();
    }
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

  /**
   * Builds refresh credential endpoint properties for Iceberg credential properties.
   *
   * @param encodedCatalogName Iceberg REST encoded catalog name
   * @param encodedNamespace Iceberg REST encoded namespace
   * @param encodedTableName Iceberg REST encoded table name
   * @param credentialProperties Iceberg credential properties used to determine refresh keys
   * @return refresh endpoint properties keyed by Iceberg client config names
   */
  public static Map<String, String> buildRefreshProps(
      String encodedCatalogName,
      String encodedNamespace,
      String encodedTableName,
      Map<String, String> credentialProperties) {
    if (credentialProperties == null || credentialProperties.isEmpty()) {
      return Collections.emptyMap();
    }

    String credentialsRefreshPath =
        String.format(
            "v1/%s/namespaces/%s/tables/%s/credentials",
            encodedCatalogName, encodedNamespace, encodedTableName);
    Map<String, String> refreshEndpointProperties = Maps.newHashMap();
    if (credentialProperties.containsKey(ICEBERG_S3_TOKEN)) {
      refreshEndpointProperties.put(
          ICEBERG_CLIENT_REFRESH_CREDENTIALS_ENDPOINT, credentialsRefreshPath);
    }
    if (credentialProperties.containsKey(ICEBERG_GCS_TOKEN)) {
      refreshEndpointProperties.put(
          ICEBERG_GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, credentialsRefreshPath);
    }
    if (credentialProperties.keySet().stream()
        .anyMatch(key -> key.startsWith(ICEBERG_ADLS_TOKEN + "."))) {
      refreshEndpointProperties.put(
          ICEBERG_ADLS_REFRESH_CREDENTIALS_ENDPOINT, credentialsRefreshPath);
    }
    return ImmutableMap.copyOf(refreshEndpointProperties);
  }

  /**
   * Transforms Gravitino credential keys to Iceberg property keys.
   *
   * @param originProperties the source credential info map
   * @param transformMap mapping from Gravitino keys to Iceberg keys
   * @return a new mutable map of transformed Iceberg properties
   */
  private static Map<String, String> transformProperties(
      Map<String, String> originProperties, Map<String, String> transformMap) {
    Map<String, String> properties = new HashMap<>();
    originProperties.forEach(
        (k, v) -> {
          if (transformMap.containsKey(k)) {
            properties.put(transformMap.get(k), v);
          }
        });
    return properties;
  }
}
