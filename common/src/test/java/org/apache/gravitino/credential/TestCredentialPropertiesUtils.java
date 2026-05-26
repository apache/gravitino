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
            "token");
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
            "security-token");
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

    Map<String, String> expectedProperties = ImmutableMap.of(sasTokenKey, sasToken);
    Assertions.assertEquals(expectedProperties, icebergProperties);
  }

  @Test
  void testToLancePropertiesS3Token() {
    S3TokenCredential credential = new S3TokenCredential("key", "secret", "token", 1000);
    Map<String, String> lanceProperties = CredentialPropertyUtils.toLanceProperties(credential);

    Assertions.assertEquals(
        "key", lanceProperties.get(CredentialPropertyUtils.LANCE_S3_ACCESS_KEY_ID));
    Assertions.assertEquals(
        "secret", lanceProperties.get(CredentialPropertyUtils.LANCE_S3_SECRET_ACCESS_KEY));
    Assertions.assertEquals(
        "token", lanceProperties.get(CredentialPropertyUtils.LANCE_S3_SESSION_TOKEN));
    Assertions.assertEquals(
        "1000", lanceProperties.get(CredentialPropertyUtils.LANCE_EXPIRES_AT_MILLIS));
    Assertions.assertEquals(4, lanceProperties.size());
  }

  @Test
  void testToLancePropertiesS3SecretKey() {
    S3SecretKeyCredential credential = new S3SecretKeyCredential("key", "secret");
    Map<String, String> lanceProperties = CredentialPropertyUtils.toLanceProperties(credential);

    Assertions.assertEquals(
        "key", lanceProperties.get(CredentialPropertyUtils.LANCE_S3_ACCESS_KEY_ID));
    Assertions.assertEquals(
        "secret", lanceProperties.get(CredentialPropertyUtils.LANCE_S3_SECRET_ACCESS_KEY));
    Assertions.assertNull(lanceProperties.get(CredentialPropertyUtils.LANCE_EXPIRES_AT_MILLIS));
    Assertions.assertEquals(2, lanceProperties.size());
  }

  @Test
  void testToLancePropertiesOSSToken() {
    OSSTokenCredential credential = new OSSTokenCredential("key", "secret", "security-token", 2000);
    Map<String, String> lanceProperties = CredentialPropertyUtils.toLanceProperties(credential);

    Assertions.assertEquals(
        "key", lanceProperties.get(CredentialPropertyUtils.LANCE_OSS_ACCESS_KEY_ID));
    Assertions.assertEquals(
        "secret", lanceProperties.get(CredentialPropertyUtils.LANCE_OSS_SECRET_ACCESS_KEY));
    Assertions.assertEquals(
        "security-token", lanceProperties.get(CredentialPropertyUtils.LANCE_OSS_SECURITY_TOKEN));
    Assertions.assertEquals(
        "2000", lanceProperties.get(CredentialPropertyUtils.LANCE_EXPIRES_AT_MILLIS));
    Assertions.assertEquals(4, lanceProperties.size());
  }

  @Test
  void testToLancePropertiesOSSSecretKey() {
    OSSSecretKeyCredential credential = new OSSSecretKeyCredential("key", "secret");
    Map<String, String> lanceProperties = CredentialPropertyUtils.toLanceProperties(credential);

    Assertions.assertEquals(
        "key", lanceProperties.get(CredentialPropertyUtils.LANCE_OSS_ACCESS_KEY_ID));
    Assertions.assertEquals(
        "secret", lanceProperties.get(CredentialPropertyUtils.LANCE_OSS_SECRET_ACCESS_KEY));
    Assertions.assertNull(lanceProperties.get(CredentialPropertyUtils.LANCE_EXPIRES_AT_MILLIS));
    Assertions.assertEquals(2, lanceProperties.size());
  }

  @Test
  void testToLancePropertiesGCSToken() {
    GCSTokenCredential credential = new GCSTokenCredential("gcs-token", 3000);
    Map<String, String> lanceProperties = CredentialPropertyUtils.toLanceProperties(credential);

    Assertions.assertEquals(
        "gcs-token", lanceProperties.get(CredentialPropertyUtils.LANCE_GCS_TOKEN));
    Assertions.assertEquals(
        "3000", lanceProperties.get(CredentialPropertyUtils.LANCE_EXPIRES_AT_MILLIS));
    Assertions.assertEquals(2, lanceProperties.size());
  }

  @Test
  void testToLancePropertiesADLSToken() {
    ADLSTokenCredential credential = new ADLSTokenCredential("account", "sas-token", 4000);
    Map<String, String> lanceProperties = CredentialPropertyUtils.toLanceProperties(credential);

    Assertions.assertEquals(
        "sas-token", lanceProperties.get(CredentialPropertyUtils.LANCE_AZURE_SAS_TOKEN));
    Assertions.assertEquals(
        "account", lanceProperties.get(CredentialPropertyUtils.LANCE_AZURE_STORAGE_ACCOUNT_NAME));
    Assertions.assertEquals(
        "4000", lanceProperties.get(CredentialPropertyUtils.LANCE_EXPIRES_AT_MILLIS));
    Assertions.assertEquals(3, lanceProperties.size());
  }

  @Test
  void testToLancePropertiesAzureAccountKey() {
    AzureAccountKeyCredential credential = new AzureAccountKeyCredential("account", "key-value");
    Map<String, String> lanceProperties = CredentialPropertyUtils.toLanceProperties(credential);

    Assertions.assertEquals(
        "account", lanceProperties.get(CredentialPropertyUtils.LANCE_AZURE_STORAGE_ACCOUNT_NAME));
    Assertions.assertEquals(
        "key-value", lanceProperties.get(CredentialPropertyUtils.LANCE_AZURE_STORAGE_ACCOUNT_KEY));
    Assertions.assertNull(lanceProperties.get(CredentialPropertyUtils.LANCE_EXPIRES_AT_MILLIS));
    Assertions.assertEquals(2, lanceProperties.size());
  }

  @Test
  void testToLancePropertiesAwsIrsa() {
    AwsIrsaCredential credential = new AwsIrsaCredential("key", "secret", "session", 5000);
    Map<String, String> lanceProperties = CredentialPropertyUtils.toLanceProperties(credential);

    Assertions.assertEquals(
        "key", lanceProperties.get(CredentialPropertyUtils.LANCE_S3_ACCESS_KEY_ID));
    Assertions.assertEquals(
        "secret", lanceProperties.get(CredentialPropertyUtils.LANCE_S3_SECRET_ACCESS_KEY));
    Assertions.assertEquals(
        "session", lanceProperties.get(CredentialPropertyUtils.LANCE_S3_SESSION_TOKEN));
    Assertions.assertEquals(
        "5000", lanceProperties.get(CredentialPropertyUtils.LANCE_EXPIRES_AT_MILLIS));
    Assertions.assertEquals(4, lanceProperties.size());
  }
}
