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
package org.apache.gravitino.iceberg.service;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.ADLSTokenCredential;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.GCSTokenCredential;
import org.apache.gravitino.credential.OSSTokenCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;

/**
 * Converts Gravitino credentials into Iceberg REST vended-credential payloads.
 *
 * <p>Iceberg 1.11 clients refresh expiring storage tokens via table-scoped {@code GET
 * /v1/.../tables/{table}/credentials}. Each cloud FileIO expects credential properties and a
 * cloud-specific refresh-endpoint key (for example {@code client.refresh-credentials-endpoint} for
 * S3/OSS, {@code gcs.oauth2.refresh-credentials-endpoint} for GCS, and {@code
 * adls.refresh-credentials-endpoint} for ADLS).
 */
public final class IcebergVendedCredentials {

  private IcebergVendedCredentials() {}

  /**
   * Builds Iceberg client configuration for a vended credential, including token expiry and the
   * table-scoped refresh path when the credential type supports refresh.
   *
   * @param catalogName IRC catalog name used in the refresh path
   * @param tableIdentifier table receiving the credential
   * @param credential Gravitino credential to vend
   * @return mutable map of Iceberg table-load / credential config entries
   */
  public static Map<String, String> toClientConfig(
      String catalogName, TableIdentifier tableIdentifier, Credential credential) {
    Map<String, String> config = CredentialPropertyUtils.toIcebergProperties(credential);
    refreshEndpointProperty(credential)
        .ifPresent(
            property -> config.put(property, tableCredentialsPath(catalogName, tableIdentifier)));
    return config;
  }

  /**
   * Builds an Iceberg REST {@link org.apache.iceberg.rest.credentials.Credential} for load-table,
   * scan-plan, or credentials API responses.
   *
   * @param catalogName IRC catalog name used in the refresh path
   * @param tableIdentifier table receiving the credential
   * @param credential Gravitino credential to vend
   * @param tableMetadata table metadata used to derive the storage prefix
   * @return Iceberg REST credential with prefix and config
   */
  public static org.apache.iceberg.rest.credentials.Credential toRestCredential(
      String catalogName,
      TableIdentifier tableIdentifier,
      Credential credential,
      TableMetadata tableMetadata) {
    Map<String, String> config = toClientConfig(catalogName, tableIdentifier, credential);
    String prefix = tableLocationPrefix(tableMetadata);
    return new org.apache.iceberg.rest.credentials.Credential() {
      @Override
      public String prefix() {
        return prefix;
      }

      @Override
      public Map<String, String> config() {
        return config;
      }

      @Override
      public void validate() {}
    };
  }

  /**
   * Relative path for the Iceberg table credentials refresh endpoint (no leading slash; Trino and
   * other clients resolve it against the catalog URI).
   *
   * @param catalogName IRC catalog name
   * @param tableIdentifier table identifier
   * @return path such as {@code v1/{catalog}/namespaces/{ns}/tables/{table}/credentials}
   */
  @VisibleForTesting
  static String tableCredentialsPath(String catalogName, TableIdentifier tableIdentifier) {
    return String.format(
        "v1/%s/namespaces/%s/tables/%s/credentials",
        RESTUtil.encodeString(catalogName),
        RESTUtil.encodeNamespace(
            tableIdentifier.namespace(), IcebergRESTUtils.NAMESPACE_SEPARATOR_URLENCODED_UTF_8),
        RESTUtil.encodeString(tableIdentifier.name()));
  }

  /**
   * Storage location prefix for an Iceberg REST credential (table location with trailing slash).
   *
   * @param tableMetadata table metadata
   * @return location prefix for credential scoping
   */
  @VisibleForTesting
  static String tableLocationPrefix(TableMetadata tableMetadata) {
    String location = tableMetadata.location();
    return location.endsWith("/") ? location : location + "/";
  }

  /**
   * Iceberg catalog property name for the refresh endpoint, if this credential type expires and
   * supports catalog refresh.
   *
   * @param credential Gravitino credential
   * @return refresh-endpoint property key, or empty when refresh is not applicable
   */
  @VisibleForTesting
  static Optional<String> refreshEndpointProperty(Credential credential) {
    if (credential instanceof GCSTokenCredential) {
      return Optional.of(IcebergConstants.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT);
    }
    if (credential instanceof ADLSTokenCredential) {
      return Optional.of(IcebergConstants.ADLS_REFRESH_CREDENTIALS_ENDPOINT);
    }
    if (credential instanceof S3TokenCredential || credential instanceof AwsIrsaCredential) {
      return Optional.of(IcebergConstants.ICEBERG_S3_REFRESH_CREDENTIALS_ENDPOINT);
    }
    if (credential instanceof OSSTokenCredential) {
      return Optional.of(IcebergConstants.ICEBERG_OSS_REFRESH_CREDENTIALS_ENDPOINT);
    }
    return Optional.empty();
  }
}
