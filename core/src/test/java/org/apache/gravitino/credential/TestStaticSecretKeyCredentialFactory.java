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
package org.apache.gravitino.credential;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStaticSecretKeyCredentialFactory {

  @Test
  public void testFromPropertiesBuildsS3Pair() {
    List<Credential> credentials =
        StaticSecretKeyCredentialFactory.fromProperties(
            Map.of(
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                "ak",
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                "sk"));

    Assertions.assertEquals(1, credentials.size());
    S3SecretKeyCredential s3 = (S3SecretKeyCredential) credentials.get(0);
    Assertions.assertEquals("ak", s3.accessKeyId());
    Assertions.assertEquals("sk", s3.secretAccessKey());
  }

  @Test
  public void testFromPropertiesSkipsIncompletePair() {
    Assertions.assertTrue(
        StaticSecretKeyCredentialFactory.fromProperties(
                Map.of(S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID, "ak"))
            .isEmpty());
  }

  @Test
  public void testOverlayReplacesCatalogStaticCredentialWhenAllowed() {
    Credential catalogCredential = new S3SecretKeyCredential("catalog-ak", "catalog-sk");
    Credential tokenCredential =
        new S3TokenCredential("tok-ak", "tok-sk", "session", System.currentTimeMillis() + 60_000);

    List<Credential> overlaid =
        StaticSecretKeyCredentialFactory.overlay(
            List.of(catalogCredential, tokenCredential),
            Map.of(
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                "fileset-ak",
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                "fileset-sk"),
            Set.of(
                S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE,
                S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE));

    Assertions.assertEquals(2, overlaid.size());
    S3SecretKeyCredential s3 =
        (S3SecretKeyCredential)
            overlaid.stream()
                .filter(
                    c ->
                        S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE.equals(
                            c.credentialType()))
                .findFirst()
                .orElseThrow();
    Assertions.assertEquals("fileset-ak", s3.accessKeyId());
    Assertions.assertEquals("fileset-sk", s3.secretAccessKey());
    Assertions.assertTrue(
        overlaid.stream()
            .anyMatch(c -> S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE.equals(c.credentialType())));
  }

  @Test
  public void testOverlayDoesNotAddStaticWhenOnlyTokenAllowed() {
    Credential tokenCredential =
        new S3TokenCredential("tok-ak", "tok-sk", "session", System.currentTimeMillis() + 60_000);

    List<Credential> overlaid =
        StaticSecretKeyCredentialFactory.overlay(
            List.of(tokenCredential),
            Map.of(
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                "fileset-ak",
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                "fileset-sk"),
            Set.of(S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE));

    Assertions.assertEquals(1, overlaid.size());
    Assertions.assertEquals(
        S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE, overlaid.get(0).credentialType());
  }

  @Test
  public void testOverlayAddsStaticWhenTypeAllowedAndMissing() {
    List<Credential> overlaid =
        StaticSecretKeyCredentialFactory.overlay(
            List.of(),
            Map.of(
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                "fileset-ak",
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                "fileset-sk"),
            Set.of(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE));

    Assertions.assertEquals(1, overlaid.size());
    Assertions.assertEquals("fileset-ak", ((S3SecretKeyCredential) overlaid.get(0)).accessKeyId());
  }

  @Test
  public void testOverlayNoOpWhenAllowedTypesEmpty() {
    List<Credential> existing = List.of(new S3TokenCredential("tok-ak", "tok-sk", "session", 1L));
    List<Credential> overlaid =
        StaticSecretKeyCredentialFactory.overlay(
            existing,
            Map.of(
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                "fileset-ak",
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                "fileset-sk"),
            Set.of());

    Assertions.assertSame(existing, overlaid);
  }

  @Test
  public void testFromPropertiesBuildsOssCosAndAzurePairs() {
    List<Credential> credentials =
        StaticSecretKeyCredentialFactory.fromProperties(
            Map.of(
                OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_ACCESS_KEY_ID,
                "oss-ak",
                OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_SECRET_ACCESS_KEY,
                "oss-sk",
                COSSecretKeyCredential.GRAVITINO_COS_STATIC_ACCESS_KEY_ID,
                "cos-ak",
                COSSecretKeyCredential.GRAVITINO_COS_STATIC_SECRET_ACCESS_KEY,
                "cos-sk",
                AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME,
                "acct",
                AzureAccountKeyCredential.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY,
                "key"));

    Assertions.assertEquals(3, credentials.size());
    OSSSecretKeyCredential oss =
        (OSSSecretKeyCredential)
            credentials.stream()
                .filter(
                    c ->
                        OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE.equals(
                            c.credentialType()))
                .findFirst()
                .orElseThrow();
    Assertions.assertEquals("oss-ak", oss.accessKeyId());
    Assertions.assertEquals("oss-sk", oss.secretAccessKey());

    COSSecretKeyCredential cos =
        (COSSecretKeyCredential)
            credentials.stream()
                .filter(
                    c ->
                        COSSecretKeyCredential.COS_SECRET_KEY_CREDENTIAL_TYPE.equals(
                            c.credentialType()))
                .findFirst()
                .orElseThrow();
    Assertions.assertEquals("cos-ak", cos.accessKeyId());
    Assertions.assertEquals("cos-sk", cos.secretAccessKey());

    AzureAccountKeyCredential azure =
        (AzureAccountKeyCredential)
            credentials.stream()
                .filter(
                    c ->
                        AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE.equals(
                            c.credentialType()))
                .findFirst()
                .orElseThrow();
    Assertions.assertEquals("acct", azure.accountName());
    Assertions.assertEquals("key", azure.accountKey());
  }

  @Test
  public void testOverlayAddsOssWhenTypeAllowed() {
    List<Credential> overlaid =
        StaticSecretKeyCredentialFactory.overlay(
            List.of(),
            Map.of(
                OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_ACCESS_KEY_ID,
                "oss-ak",
                OSSSecretKeyCredential.GRAVITINO_OSS_STATIC_SECRET_ACCESS_KEY,
                "oss-sk"),
            Set.of(OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE));

    Assertions.assertEquals(1, overlaid.size());
    Assertions.assertEquals("oss-ak", ((OSSSecretKeyCredential) overlaid.get(0)).accessKeyId());
  }
}
