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
import java.util.HashMap;
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

    String sasTokenKey =
        String.format(
            "%s.%s.%s",
            CredentialPropertyUtils.ICEBERG_ADLS_TOKEN,
            storageAccountName,
            ADLSTokenCredential.ADLS_DOMAIN);

    Map<String, String> expectedProperties =
        ImmutableMap.of(
            sasTokenKey,
            sasToken,
            CredentialPropertyUtils.ICEBERG_ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX
                + storageAccountName,
            String.valueOf(expireTimeInMS));
    Assertions.assertEquals(expectedProperties, icebergProperties);
  }

  @Test
  void testAppendRefreshEndpointForS3Token() {
    Map<String, String> config =
        new HashMap<>(
            CredentialPropertyUtils.toIcebergProperties(
                new S3TokenCredential("key", "secret", "token", 1234L)));
    CredentialPropertyUtils.appendRefreshEndpoint(
        config,
        new S3TokenCredential("key", "secret", "token", 1234L),
        "v1/aws/namespaces/ns/tables/tbl/credentials");

    Assertions.assertEquals(
        "v1/aws/namespaces/ns/tables/tbl/credentials",
        config.get(CredentialPropertyUtils.S3_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAppendRefreshEndpointForGcsToken() {
    Credential gcsToken = new GCSTokenCredential("gcs-token", 5678L);
    Map<String, String> config =
        new HashMap<>(CredentialPropertyUtils.toIcebergProperties(gcsToken));
    CredentialPropertyUtils.appendRefreshEndpoint(
        config, gcsToken, "v1/gcs/namespaces/ns/tables/tbl/credentials");

    Assertions.assertEquals(
        "v1/gcs/namespaces/ns/tables/tbl/credentials",
        config.get(CredentialPropertyUtils.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAppendRefreshEndpointForOssToken() {
    Credential ossToken = new OSSTokenCredential("key", "secret", "oss-token", 9012L);
    Map<String, String> config =
        new HashMap<>(CredentialPropertyUtils.toIcebergProperties(ossToken));
    CredentialPropertyUtils.appendRefreshEndpoint(
        config, ossToken, "v1/oss/namespaces/ns/tables/tbl/credentials");

    Assertions.assertEquals(
        "v1/oss/namespaces/ns/tables/tbl/credentials",
        config.get(CredentialPropertyUtils.OSS_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAppendRefreshEndpointForAdlsToken() {
    Credential adlsToken = new ADLSTokenCredential("storageacct", "sas-token", 3456L);
    Map<String, String> config =
        new HashMap<>(CredentialPropertyUtils.toIcebergProperties(adlsToken));
    CredentialPropertyUtils.appendRefreshEndpoint(
        config, adlsToken, "v1/adls/namespaces/ns/tables/tbl/credentials");

    Assertions.assertEquals(
        "v1/adls/namespaces/ns/tables/tbl/credentials",
        config.get(CredentialPropertyUtils.ADLS_REFRESH_CREDENTIALS_ENDPOINT));
  }

  @Test
  void testAppendRefreshEndpointOmitsStaticSecretKey() {
    Map<String, String> config =
        new HashMap<>(
            CredentialPropertyUtils.toIcebergProperties(
                new S3SecretKeyCredential("key", "secret")));
    CredentialPropertyUtils.appendRefreshEndpoint(
        config,
        new S3SecretKeyCredential("key", "secret"),
        "v1/aws/namespaces/ns/tables/tbl/credentials");

    Assertions.assertFalse(
        config.containsKey(CredentialPropertyUtils.S3_REFRESH_CREDENTIALS_ENDPOINT));
  }
}
