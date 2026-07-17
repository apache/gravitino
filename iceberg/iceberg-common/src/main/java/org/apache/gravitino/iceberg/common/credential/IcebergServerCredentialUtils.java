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
package org.apache.gravitino.iceberg.common.credential;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.S3TokenCredential;

/** Utilities for applying Gravitino credentials to server-side Iceberg catalog properties. */
public final class IcebergServerCredentialUtils {

  /** Iceberg property that names an AWS SDK v2 credentials provider class. */
  public static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";

  /** Prefix stripped by Iceberg before passing properties to the credentials provider. */
  public static final String CLIENT_CREDENTIALS_PROVIDER_PREFIX = "client.credentials-provider.";

  private static final String ICEBERG_S3_ACCESS_KEY_ID = "s3.access-key-id";
  private static final String ICEBERG_S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  private static final String ICEBERG_S3_SESSION_TOKEN = "s3.session-token";
  private static final String ICEBERG_S3_SESSION_TOKEN_EXPIRES_AT_MS =
      "s3.session-token-expires-at-ms";

  /**
   * Applies credentials to Iceberg catalog properties.
   *
   * <p>Expiring S3 credentials are represented by a refreshable AWS SDK credentials provider;
   * non-expiring and non-S3 credentials are converted to regular Iceberg properties.
   *
   * @param catalogName catalog name
   * @param credentials credentials to apply
   * @param credentialProviderProperties properties passed to the refreshable provider
   * @param targetProperties mutable Iceberg catalog properties
   */
  public static void applyCredentials(
      String catalogName,
      Credential[] credentials,
      Map<String, String> credentialProviderProperties,
      Map<String, String> targetProperties) {
    Map<String, String> effectiveProviderProperties = new HashMap<>();
    if (credentialProviderProperties != null) {
      effectiveProviderProperties.putAll(credentialProviderProperties);
    }
    effectiveProviderProperties.put(
        GravitinoIcebergAwsCredentialsProvider.CATALOG_NAME, catalogName);
    effectiveProviderProperties.putIfAbsent(
        GravitinoIcebergAwsCredentialsProvider.SOURCE,
        GravitinoIcebergAwsCredentialsProvider.SOURCE_LOCAL);

    boolean hasRefreshableAwsCredential = false;
    for (Credential credential : credentials) {
      if (isExpiringS3Credential(credential)) {
        hasRefreshableAwsCredential = true;
      } else {
        CredentialPropertyUtils.applyIcebergCredentials(
            new Credential[] {credential}, targetProperties);
      }
    }

    if (hasRefreshableAwsCredential) {
      configureRefreshableAwsProvider(effectiveProviderProperties, targetProperties);
    }
  }

  /**
   * Returns whether the credentials include an expiring S3 credential that needs a refreshable AWS
   * credentials provider.
   *
   * @param credentials credentials to inspect
   * @return true if a refreshable AWS credentials provider is needed
   */
  public static boolean hasRefreshableAwsCredential(Credential[] credentials) {
    for (Credential credential : credentials) {
      if (isExpiringS3Credential(credential)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isExpiringS3Credential(Credential credential) {
    return credential instanceof S3TokenCredential || credential instanceof AwsIrsaCredential;
  }

  private static void configureRefreshableAwsProvider(
      Map<String, String> credentialProviderProperties, Map<String, String> targetProperties) {
    targetProperties.remove(ICEBERG_S3_ACCESS_KEY_ID);
    targetProperties.remove(ICEBERG_S3_SECRET_ACCESS_KEY);
    targetProperties.remove(ICEBERG_S3_SESSION_TOKEN);
    targetProperties.remove(ICEBERG_S3_SESSION_TOKEN_EXPIRES_AT_MS);
    targetProperties.put(
        CLIENT_CREDENTIALS_PROVIDER, GravitinoIcebergAwsCredentialsProvider.class.getName());
    credentialProviderProperties.forEach(
        (key, value) -> targetProperties.put(CLIENT_CREDENTIALS_PROVIDER_PREFIX + key, value));
  }

  private IcebergServerCredentialUtils() {}
}
