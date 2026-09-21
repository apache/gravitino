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
package org.apache.gravitino.cloud.storage;

import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_ADLS_ACCOUNT_KEY;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_ADLS_ACCOUNT_NAME;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_ADLS_REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_ADLS_TOKEN;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_CLIENT_REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_GCS_TOKEN;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_GCS_TOKEN_EXPIRES_AT;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_JDBC_PASSWORD;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_JDBC_USER;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_OSS_ACCESS_KEY_ID;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_OSS_ACCESS_KEY_SECRET;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_OSS_SECURITY_TOKEN;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_S3_ACCESS_KEY_ID;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_S3_SECRET_ACCESS_KEY;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_S3_TOKEN;
import static org.apache.gravitino.credential.CredentialPropertyUtils.ICEBERG_S3_TOKEN_EXPIRES_AT_MS;
import static org.apache.gravitino.credential.CredentialPropertyUtils.PAIMON_OSS_ACCESS_KEY_ID;
import static org.apache.gravitino.credential.CredentialPropertyUtils.PAIMON_OSS_ACCESS_KEY_SECRET;
import static org.apache.gravitino.credential.CredentialPropertyUtils.PAIMON_S3_ACCESS_KEY;
import static org.apache.gravitino.credential.CredentialPropertyUtils.PAIMON_S3_SECRET_KEY;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.connector.CatalogCredentialPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.config.CredentialConfig;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

/**
 * Shared credential {@link PropertyEntry} definitions merged into every catalog's properties
 * metadata.
 *
 * <p>Cloud entries and {@link org.apache.gravitino.connector.CatalogCredentialPropertiesMetadata}
 * are the definitions those properties already have. Engine keys from {@link
 * org.apache.gravitino.credential.CredentialPropertyUtils} copy hidden from the catalog property
 * that stores the same value: an access key from that cloud's access key, a session or SAS token
 * from that cloud's secret or from {@code token}, an expiry from that cloud's {@code
 * *-token-expire-in-secs}, and a refresh endpoint from {@code s3-token-service-endpoint}. A
 * connector that already declares the same key keeps its own entry.
 */
public final class SharedCloudPropertiesMetadata {

  private static final PropertyEntry<?> S3_ACCESS_KEY_ID =
      entry(S3PropertiesMetadata.PROPERTY_ENTRIES, S3Properties.GRAVITINO_S3_ACCESS_KEY_ID);
  private static final PropertyEntry<?> S3_SECRET_ACCESS_KEY =
      entry(S3PropertiesMetadata.PROPERTY_ENTRIES, S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);
  private static final PropertyEntry<?> OSS_ACCESS_KEY_ID =
      entry(OSSPropertiesMetadata.PROPERTY_ENTRIES, OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID);
  private static final PropertyEntry<?> OSS_ACCESS_KEY_SECRET =
      entry(OSSPropertiesMetadata.PROPERTY_ENTRIES, OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET);
  private static final PropertyEntry<?> AZURE_ACCOUNT_NAME =
      entry(
          AzurePropertiesMetadata.PROPERTY_ENTRIES,
          AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
  private static final PropertyEntry<?> AZURE_ACCOUNT_KEY =
      entry(
          AzurePropertiesMetadata.PROPERTY_ENTRIES,
          AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
  private static final PropertyEntry<?> TOKEN =
      entry(
          CatalogCredentialPropertiesMetadata.PAIMON_REST_PROPERTY_ENTRIES, PaimonConstants.TOKEN);
  private static final PropertyEntry<?> S3_TOKEN_EXPIRE =
      entry(
          CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES,
          CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS);
  private static final PropertyEntry<?> OSS_TOKEN_EXPIRE =
      entry(
          CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES,
          CredentialConstants.OSS_TOKEN_EXPIRE_IN_SECS);
  private static final PropertyEntry<?> ADLS_TOKEN_EXPIRE =
      entry(
          CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES,
          CredentialConstants.ADLS_TOKEN_EXPIRE_IN_SECS);
  private static final PropertyEntry<?> COS_TOKEN_EXPIRE =
      entry(
          CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES,
          CredentialConstants.COS_TOKEN_EXPIRE_IN_SECS);
  private static final PropertyEntry<?> S3_TOKEN_SERVICE_ENDPOINT =
      entry(S3PropertiesMetadata.PROPERTY_ENTRIES, S3Properties.GRAVITINO_S3_STS_ENDPOINT);

  /** Concrete ADLS SAS token keys append the account host to this prefix. */
  private static final String ICEBERG_ADLS_SAS_TOKEN_PREFIX = ICEBERG_ADLS_TOKEN + ".";

  /**
   * Engine property names from {@link org.apache.gravitino.credential.CredentialPropertyUtils}.
   * Hidden is copied from the catalog credential entry each key corresponds to.
   */
  private static final Map<String, PropertyEntry<?>> ENGINE_CREDENTIAL_PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(ICEBERG_S3_ACCESS_KEY_ID, S3_ACCESS_KEY_ID.withName(ICEBERG_S3_ACCESS_KEY_ID))
          .put(
              ICEBERG_S3_SECRET_ACCESS_KEY,
              S3_SECRET_ACCESS_KEY.withName(ICEBERG_S3_SECRET_ACCESS_KEY))
          .put(
              ICEBERG_S3_TOKEN,
              S3_SECRET_ACCESS_KEY.asOptionalString(ICEBERG_S3_TOKEN, "Iceberg S3 session token"))
          .put(
              ICEBERG_S3_TOKEN_EXPIRES_AT_MS,
              S3_TOKEN_EXPIRE.asOptionalString(
                  ICEBERG_S3_TOKEN_EXPIRES_AT_MS,
                  "Epoch millis when the Iceberg S3 session token expires"))
          .put(
              ICEBERG_CLIENT_REFRESH_CREDENTIALS_ENDPOINT,
              S3_TOKEN_SERVICE_ENDPOINT.asOptionalString(
                  ICEBERG_CLIENT_REFRESH_CREDENTIALS_ENDPOINT,
                  "Iceberg client endpoint for refreshing S3 credentials"))
          .put(ICEBERG_OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_ID.withName(ICEBERG_OSS_ACCESS_KEY_ID))
          .put(
              ICEBERG_OSS_ACCESS_KEY_SECRET,
              OSS_ACCESS_KEY_SECRET.withName(ICEBERG_OSS_ACCESS_KEY_SECRET))
          .put(
              ICEBERG_OSS_SECURITY_TOKEN,
              OSS_ACCESS_KEY_SECRET.asOptionalString(
                  ICEBERG_OSS_SECURITY_TOKEN, "Iceberg OSS security token"))
          .put(
              ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS,
              OSS_TOKEN_EXPIRE.asOptionalString(
                  ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS,
                  "Epoch millis when the Iceberg OSS security token expires"))
          .put(
              ICEBERG_ADLS_SAS_TOKEN_PREFIX,
              TOKEN.asOptionalStringPrefix(
                  ICEBERG_ADLS_SAS_TOKEN_PREFIX, "Iceberg ADLS SAS token for an account host"))
          .put(ICEBERG_ADLS_ACCOUNT_NAME, AZURE_ACCOUNT_NAME.withName(ICEBERG_ADLS_ACCOUNT_NAME))
          .put(ICEBERG_ADLS_ACCOUNT_KEY, AZURE_ACCOUNT_KEY.withName(ICEBERG_ADLS_ACCOUNT_KEY))
          .put(
              ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX,
              ADLS_TOKEN_EXPIRE.asOptionalStringPrefix(
                  ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX,
                  "Epoch millis when an Iceberg ADLS SAS token expires"))
          .put(
              ICEBERG_ADLS_REFRESH_CREDENTIALS_ENDPOINT,
              S3_TOKEN_SERVICE_ENDPOINT.asOptionalString(
                  ICEBERG_ADLS_REFRESH_CREDENTIALS_ENDPOINT,
                  "Iceberg endpoint for refreshing ADLS credentials"))
          .put(
              ICEBERG_GCS_TOKEN,
              TOKEN.asOptionalString(
                  ICEBERG_GCS_TOKEN, "OAuth2 access token for Iceberg GCSFileIO"))
          .put(
              ICEBERG_GCS_TOKEN_EXPIRES_AT,
              PropertyEntry.stringOptionalPropertyEntry(
                  ICEBERG_GCS_TOKEN_EXPIRES_AT,
                  "Epoch millis when the Iceberg GCS OAuth2 token expires",
                  false /* immutable */,
                  null /* defaultValue */,
                  tokenExpireHidden()))
          .put(
              ICEBERG_GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
              S3_TOKEN_SERVICE_ENDPOINT.asOptionalString(
                  ICEBERG_GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
                  "Iceberg endpoint for refreshing GCS OAuth2 credentials"))
          .put(
              ICEBERG_JDBC_USER,
              CatalogCredentialPropertiesMetadata.JDBC_USER.withName(ICEBERG_JDBC_USER))
          .put(
              ICEBERG_JDBC_PASSWORD,
              CatalogCredentialPropertiesMetadata.JDBC_PASSWORD.withName(ICEBERG_JDBC_PASSWORD))
          .put(PAIMON_S3_ACCESS_KEY, S3_ACCESS_KEY_ID.withName(PAIMON_S3_ACCESS_KEY))
          .put(PAIMON_S3_SECRET_KEY, S3_SECRET_ACCESS_KEY.withName(PAIMON_S3_SECRET_KEY))
          .put(PAIMON_OSS_ACCESS_KEY_ID, OSS_ACCESS_KEY_ID.withName(PAIMON_OSS_ACCESS_KEY_ID))
          .put(
              PAIMON_OSS_ACCESS_KEY_SECRET,
              OSS_ACCESS_KEY_SECRET.withName(PAIMON_OSS_ACCESS_KEY_SECRET))
          .build();

  /** Cloud and connector credential keys merged into every catalog's properties metadata. */
  public static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .putAll(S3PropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(OSSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(AzurePropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(GCSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(COSPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(CatalogCredentialPropertiesMetadata.PROPERTY_ENTRIES)
          .putAll(ENGINE_CREDENTIAL_PROPERTY_ENTRIES)
          .build();

  private SharedCloudPropertiesMetadata() {}

  private static PropertyEntry<?> entry(Map<String, PropertyEntry<?>> entries, String name) {
    PropertyEntry<?> propertyEntry = entries.get(name);
    Preconditions.checkNotNull(propertyEntry, "Property entry is not defined: %s", name);
    return propertyEntry;
  }

  /**
   * GCS has no {@code gcs-token-expire-in-secs} property. Expiry stays visible only when every
   * existing {@code *-token-expire-in-secs} entry agrees, so one cloud's flag cannot move GCS by
   * itself.
   */
  private static boolean tokenExpireHidden() {
    boolean hidden = S3_TOKEN_EXPIRE.isHidden();
    Preconditions.checkState(
        hidden == OSS_TOKEN_EXPIRE.isHidden()
            && hidden == ADLS_TOKEN_EXPIRE.isHidden()
            && hidden == COS_TOKEN_EXPIRE.isHidden(),
        "Token expire properties disagree on hidden");
    return hidden;
  }
}
