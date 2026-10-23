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
package org.apache.gravitino.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.credential.AwsSecretKeyCredential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.Test;

public class TestGlueCatalogCredentials {

  @Test
  void testAwsKeysAutoDetectAwsAndS3Providers() {
    Map<String, String> props =
        Map.of(
            GlueConstants.AWS_ACCESS_KEY_ID,
            "AKIA",
            GlueConstants.AWS_SECRET_ACCESS_KEY,
            "secret",
            GlueConstants.AWS_REGION,
            "us-east-1");

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("glue")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("glue")
            .withProperties(props)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    GlueCatalog catalog = new GlueCatalog().withCatalogEntity(entity).withCatalogConf(props);
    Map<String, String> withProviders = catalog.propertiesWithCredentialProviders();

    List<String> providers = splitProviders(withProviders);
    assertTrue(providers.contains(AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE));
    assertTrue(providers.contains(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE));
    assertEquals("AKIA", withProviders.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
    assertEquals("secret", withProviders.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY));
    assertEquals("AKIA", withProviders.get(GlueConstants.AWS_ACCESS_KEY_ID));
  }

  @Test
  void testExplicitCredentialProvidersStillGetsAwsButNotS3SecretKey() {
    Map<String, String> props =
        Map.of(
            GlueConstants.AWS_ACCESS_KEY_ID,
            "AKIA",
            GlueConstants.AWS_SECRET_ACCESS_KEY,
            "secret",
            GlueConstants.AWS_REGION,
            "us-east-1",
            CredentialConstants.CREDENTIAL_PROVIDERS,
            "custom-provider");

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(2L)
            .withName("glue-explicit")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("glue")
            .withProperties(props)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    GlueCatalog catalog = new GlueCatalog().withCatalogEntity(entity).withCatalogConf(props);
    Map<String, String> withProviders = catalog.propertiesWithCredentialProviders();

    List<String> providers = splitProviders(withProviders);
    assertEquals(
        List.of("custom-provider", AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE),
        providers);
    assertFalse(providers.contains(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE));
    assertEquals("AKIA", withProviders.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID));
  }

  @Test
  void testExplicitS3TokenDoesNotAppendS3SecretKey() {
    Map<String, String> props =
        Map.of(
            GlueConstants.AWS_ACCESS_KEY_ID,
            "AKIA",
            GlueConstants.AWS_SECRET_ACCESS_KEY,
            "secret",
            GlueConstants.AWS_REGION,
            "us-east-1",
            CredentialConstants.CREDENTIAL_PROVIDERS,
            S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE);

    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(3L)
            .withName("glue-s3-token")
            .withNamespace(Namespace.of("metalake"))
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("glue")
            .withProperties(props)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    GlueCatalog catalog = new GlueCatalog().withCatalogEntity(entity).withCatalogConf(props);
    Map<String, String> withProviders = catalog.propertiesWithCredentialProviders();

    assertEquals(
        List.of(
            S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE,
            AwsSecretKeyCredential.AWS_SECRET_KEY_CREDENTIAL_TYPE),
        splitProviders(withProviders));
  }

  private static List<String> splitProviders(Map<String, String> withProviders) {
    String providers = withProviders.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    return Arrays.stream(providers.split(",")).map(String::trim).collect(Collectors.toList());
  }
}
