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
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class to generate specific credential properties for different table format and engine.
 */
public class CredentialPropertyUtils {

  @VisibleForTesting static final String ICEBERG_S3_ACCESS_KEY_ID = "s3.access-key-id";
  @VisibleForTesting static final String ICEBERG_S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  @VisibleForTesting static final String ICEBERG_S3_TOKEN = "s3.session-token";
  @VisibleForTesting static final String ICEBERG_GCS_TOKEN = "gcs.oauth2.token";

  @VisibleForTesting static final String ICEBERG_OSS_ACCESS_KEY_ID = "client.access-key-id";
  @VisibleForTesting static final String ICEBERG_OSS_ACCESS_KEY_SECRET = "client.access-key-secret";
  @VisibleForTesting static final String ICEBERG_OSS_SECURITY_TOKEN = "client.security-token";

  private static Map<String, String> icebergCredentialPropertyMap =
      ImmutableMap.of(
          GCSTokenCredential.GCS_TOKEN_NAME,
          ICEBERG_GCS_TOKEN,
          S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
          ICEBERG_S3_ACCESS_KEY_ID,
          S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
          ICEBERG_S3_SECRET_ACCESS_KEY,
          S3TokenCredential.GRAVITINO_S3_TOKEN,
          ICEBERG_S3_TOKEN,
          OSSTokenCredential.GRAVITINO_OSS_TOKEN,
          ICEBERG_OSS_SECURITY_TOKEN,
          OSSTokenCredential.GRAVITINO_OSS_SESSION_ACCESS_KEY_ID,
          ICEBERG_OSS_ACCESS_KEY_ID,
          OSSTokenCredential.GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY,
          ICEBERG_OSS_ACCESS_KEY_SECRET);

  /**
   * Transforms a specific credential into a map of Iceberg properties.
   *
   * @param credential the credential to be transformed into Iceberg properties
   * @return a map of Iceberg properties derived from the credential
   */
  public static Map<String, String> toIcebergProperties(Credential credential) {
    if (credential instanceof GCSTokenCredential) {
      Map<String, String> icebergGCSCredentialProperties =
          transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap);
      icebergGCSCredentialProperties.put(
          "gcs.oauth2.token-expires-at", String.valueOf(credential.expireTimeInMs()));
      return icebergGCSCredentialProperties;
    }
    if (credential instanceof S3TokenCredential || credential instanceof S3SecretKeyCredential) {
      return transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap);
    }
    if (credential instanceof OSSTokenCredential || credential instanceof OSSSecretKeyCredential) {
      return transformProperties(credential.credentialInfo(), icebergCredentialPropertyMap);
    }
    return credential.toProperties();
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
