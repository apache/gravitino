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

public class TestCredentialFactory {

  @Test
  void testS3TokenCredential() {
    Map<String, String> s3TokenCredentialInfo =
        ImmutableMap.of(
            S3TokenCredential.GRAVITINO_S3_SESSION_ACCESS_KEY_ID,
            "accessKeyId",
            S3TokenCredential.GRAVITINO_S3_SESSION_SECRET_ACCESS_KEY,
            "secretAccessKey",
            S3TokenCredential.GRAVITINO_S3_TOKEN,
            "token");
    long expireTime = 1000;
    Credential s3TokenCredential =
        CredentialFactory.create(
            S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE, s3TokenCredentialInfo, expireTime);
    Assertions.assertEquals(
        S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE, s3TokenCredential.credentialType());
    Assertions.assertTrue(s3TokenCredential instanceof S3TokenCredential);
    S3TokenCredential s3TokenCredential1 = (S3TokenCredential) s3TokenCredential;
    Assertions.assertEquals("accessKeyId", s3TokenCredential1.accessKeyId());
    Assertions.assertEquals("secretAccessKey", s3TokenCredential1.secretAccessKey());
    Assertions.assertEquals("token", s3TokenCredential1.sessionToken());
    Assertions.assertEquals(expireTime, s3TokenCredential1.expireTimeInMs());
  }

  @Test
  void testS3SecretKeyTokenCredential() {
    Map<String, String> s3SecretKeyCredentialInfo =
        ImmutableMap.of(
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
            "accessKeyId",
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
            "secretAccessKey");
    long expireTime = 0;
    Credential s3SecretKeyCredential =
        CredentialFactory.create(
            S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE,
            s3SecretKeyCredentialInfo,
            expireTime);
    Assertions.assertEquals(
        S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE,
        s3SecretKeyCredential.credentialType());
    Assertions.assertTrue(s3SecretKeyCredential instanceof S3SecretKeyCredential);
    S3SecretKeyCredential s3SecretKeyCredential1 = (S3SecretKeyCredential) s3SecretKeyCredential;
    Assertions.assertEquals("accessKeyId", s3SecretKeyCredential1.accessKeyId());
    Assertions.assertEquals("secretAccessKey", s3SecretKeyCredential1.secretAccessKey());
    Assertions.assertEquals(expireTime, s3SecretKeyCredential1.expireTimeInMs());
  }

  @Test
  void testGcsTokenCredential() {
    Map<String, String> gcsTokenCredentialInfo =
        ImmutableMap.of(GCSTokenCredential.GCS_TOKEN_NAME, "accessToken");
    long expireTime = 100;
    Credential gcsTokenCredential =
        CredentialFactory.create(
            GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE, gcsTokenCredentialInfo, expireTime);
    Assertions.assertEquals(
        GCSTokenCredential.GCS_TOKEN_CREDENTIAL_TYPE, gcsTokenCredential.credentialType());
    Assertions.assertTrue(gcsTokenCredential instanceof GCSTokenCredential);
    GCSTokenCredential gcsTokenCredential1 = (GCSTokenCredential) gcsTokenCredential;
    Assertions.assertEquals("accessToken", gcsTokenCredential1.token());
    Assertions.assertEquals(expireTime, gcsTokenCredential1.expireTimeInMs());
  }

  @Test
  void testOSSTokenCredential() {
    Map<String, String> ossTokenCredentialInfo =
        ImmutableMap.of(
            OSSTokenCredential.GRAVITINO_OSS_SESSION_ACCESS_KEY_ID,
            "access-id",
            OSSTokenCredential.GRAVITINO_OSS_SESSION_SECRET_ACCESS_KEY,
            "secret-key",
            OSSTokenCredential.GRAVITINO_OSS_TOKEN,
            "token");
    long expireTime = 100;
    Credential ossTokenCredential =
        CredentialFactory.create(
            OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE, ossTokenCredentialInfo, expireTime);
    Assertions.assertEquals(
        OSSTokenCredential.OSS_TOKEN_CREDENTIAL_TYPE, ossTokenCredential.credentialType());
    Assertions.assertTrue(ossTokenCredential instanceof OSSTokenCredential);
    OSSTokenCredential ossTokenCredential1 = (OSSTokenCredential) ossTokenCredential;
    Assertions.assertEquals("access-id", ossTokenCredential1.accessKeyId());
    Assertions.assertEquals("secret-key", ossTokenCredential1.secretAccessKey());
    Assertions.assertEquals("token", ossTokenCredential1.securityToken());
    Assertions.assertEquals(expireTime, ossTokenCredential1.expireTimeInMs());
  }

  @Test
  void testOSSSecretKeyTokenCredential() {
    Map<String, String> ossSecretKeyCredentialInfo =
        ImmutableMap.of(
            OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_ACCESS_KEY_ID,
            "accessKeyId",
            OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_SECRET_ACCESS_KEY,
            "secretAccessKey");
    long expireTime = 0;
    Credential ossSecretKeyCredential =
        CredentialFactory.create(
            OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE,
            ossSecretKeyCredentialInfo,
            expireTime);
    Assertions.assertEquals(
        OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE,
        ossSecretKeyCredential.credentialType());
    Assertions.assertTrue(ossSecretKeyCredential instanceof OSSSecretKeyCredential);
    OSSSecretKeyCredential ossSecretKeyCredential1 =
        (OSSSecretKeyCredential) ossSecretKeyCredential;
    Assertions.assertEquals("accessKeyId", ossSecretKeyCredential1.accessKeyId());
    Assertions.assertEquals("secretAccessKey", ossSecretKeyCredential1.secretAccessKey());
    Assertions.assertEquals(expireTime, ossSecretKeyCredential1.expireTimeInMs());
  }
}
