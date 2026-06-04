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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCredentialPropertiesUtils {

  @Test
  void testToIcebergProperties() {
    S3TokenCredential s3TokenCredential = new S3TokenCredential("key", "secret", "token", 100);
    Map<String, String> icebergProperties =
        CredentialPropertyUtils.toIcebergProperties(s3TokenCredential);
    Map<String, String> expectedProperties =
        ImmutableMap.of(
            CredentialPropertyUtils.ICEBERG_S3_ACCESS_KEY_ID,
            "key",
            CredentialPropertyUtils.ICEBERG_S3_SECRET_ACCESS_KEY,
            "secret",
            CredentialPropertyUtils.ICEBERG_S3_TOKEN,
            "token",
            CredentialPropertyUtils.ICEBERG_S3_TOKEN_EXPIRES_AT_MS,
            "100");
    Assertions.assertEquals(expectedProperties, icebergProperties);

    S3SecretKeyCredential secretKeyCredential = new S3SecretKeyCredential("key", "secret");
    icebergProperties = CredentialPropertyUtils.toIcebergProperties(secretKeyCredential);
    expectedProperties =
        ImmutableMap.of(
            CredentialPropertyUtils.ICEBERG_S3_ACCESS_KEY_ID,
            "key",
            CredentialPropertyUtils.ICEBERG_S3_SECRET_ACCESS_KEY,
            "secret");
    Assertions.assertEquals(expectedProperties, icebergProperties);
  }

  @Test
  void testToIcebergPropertiesForOSS() {
    OSSTokenCredential ossTokenCredential =
        new OSSTokenCredential("key", "secret", "security-token", 100);
    Map<String, String> icebergProperties =
        CredentialPropertyUtils.toIcebergProperties(ossTokenCredential);
    Map<String, String> expectedProperties =
        ImmutableMap.of(
            CredentialPropertyUtils.ICEBERG_OSS_ACCESS_KEY_ID,
            "key",
            CredentialPropertyUtils.ICEBERG_OSS_ACCESS_KEY_SECRET,
            "secret",
            CredentialPropertyUtils.ICEBERG_OSS_SECURITY_TOKEN,
            "security-token",
            CredentialPropertyUtils.ICEBERG_OSS_SECURITY_TOKEN_EXPIRES_AT_MS,
            "100");
    Assertions.assertEquals(expectedProperties, icebergProperties);
  }

  @Test
  void testToIcebergPropertiesForADLS() {
    String storageAccountName = "storage-account-name";
    String sasToken = "sas-token";
    long expireTimeInMS = 100;

    ADLSTokenCredential adlsTokenCredential =
        new ADLSTokenCredential(storageAccountName, sasToken, expireTimeInMS);
    Map<String, String> icebergProperties =
        CredentialPropertyUtils.toIcebergProperties(adlsTokenCredential);

    String adlsHost = storageAccountName + "." + ADLSTokenCredential.ADLS_DOMAIN;
    String sasTokenKey = CredentialPropertyUtils.ICEBERG_ADLS_TOKEN + "." + adlsHost;

    Map<String, String> expectedProperties =
        ImmutableMap.of(
            sasTokenKey,
            sasToken,
            CredentialPropertyUtils.ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + adlsHost,
            String.valueOf(expireTimeInMS));
    Assertions.assertEquals(expectedProperties, icebergProperties);
  }

  @Test
  void testBuildRefreshEndpointsForS3() {
    Map<String, String> credentialProperties =
        ImmutableMap.of(
            CredentialPropertyUtils.ICEBERG_S3_TOKEN, "token",
            CredentialPropertyUtils.ICEBERG_S3_TOKEN_EXPIRES_AT_MS, "123");

    Map<String, String> refreshEndpointProperties =
        CredentialPropertyUtils.buildRefreshProps("irc1", "db", "tbl", credentialProperties);

    Assertions.assertEquals(
        "v1/irc1/namespaces/db/tables/tbl/credentials",
        refreshEndpointProperties.get(
            CredentialPropertyUtils.ICEBERG_CLIENT_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testBuildRefreshEndpointsForGCS() {
    Map<String, String> credentialProperties =
        ImmutableMap.of(
            CredentialPropertyUtils.ICEBERG_GCS_TOKEN, "token",
            CredentialPropertyUtils.ICEBERG_GCS_TOKEN_EXPIRES_AT, "123");

    Map<String, String> refreshEndpointProperties =
        CredentialPropertyUtils.buildRefreshProps("irc1", "db", "tbl", credentialProperties);

    Assertions.assertEquals(
        "v1/irc1/namespaces/db/tables/tbl/credentials",
        refreshEndpointProperties.get(
            CredentialPropertyUtils.ICEBERG_GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testBuildRefreshEndpointsForADLS() {
    String adlsHost = "storage.dfs.core.windows.net";
    String sasTokenKey = CredentialPropertyUtils.ICEBERG_ADLS_TOKEN + "." + adlsHost;
    Map<String, String> credentialProperties =
        ImmutableMap.of(
            sasTokenKey,
            "sas-token",
            CredentialPropertyUtils.ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + adlsHost,
            "123");

    Map<String, String> refreshEndpointProperties =
        CredentialPropertyUtils.buildRefreshProps("irc1", "db", "tbl", credentialProperties);

    Assertions.assertEquals(
        "v1/irc1/namespaces/db/tables/tbl/credentials",
        refreshEndpointProperties.get(
            CredentialPropertyUtils.ICEBERG_ADLS_REFRESH_CREDENTIALS_ENDPOINT));
  }
}
