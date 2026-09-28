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
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCredentialUtils {

  @Test
  void testLoadCredentialProviders() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            CredentialConstants.CREDENTIAL_PROVIDERS,
            DummyCredentialProvider.CREDENTIAL_TYPE
                + ","
                + Dummy2CredentialProvider.CREDENTIAL_TYPE);
    Map<String, CredentialProvider> providers =
        CredentialUtils.loadCredentialProviders(catalogProperties);
    Assertions.assertTrue(providers.size() == 2);

    Assertions.assertTrue(providers.containsKey(DummyCredentialProvider.CREDENTIAL_TYPE));
    Assertions.assertTrue(
        DummyCredentialProvider.CREDENTIAL_TYPE.equals(
            providers.get(DummyCredentialProvider.CREDENTIAL_TYPE).credentialType()));
    Assertions.assertTrue(providers.containsKey(Dummy2CredentialProvider.CREDENTIAL_TYPE));
    Assertions.assertTrue(
        Dummy2CredentialProvider.CREDENTIAL_TYPE.equals(
            providers.get(Dummy2CredentialProvider.CREDENTIAL_TYPE).credentialType()));
  }

  @Test
  void testGetCredentialProviders() {
    Map<String, String> filesetProperties = ImmutableMap.of();
    Map<String, String> schemaProperties =
        ImmutableMap.of(CredentialConstants.CREDENTIAL_PROVIDERS, "a,b");
    Map<String, String> catalogProperties =
        ImmutableMap.of(CredentialConstants.CREDENTIAL_PROVIDERS, "a,b,c");

    Set<String> credentialProviders =
        CredentialUtils.getCredentialProvidersByOrder(
            () -> filesetProperties, () -> schemaProperties, () -> catalogProperties);
    Assertions.assertEquals(credentialProviders, ImmutableSet.of("a", "b"));
  }

  @Test
  void testInferWhenEntireChainOmitsCredentialProviders() {
    Map<String, String> filesetProperties =
        ImmutableMap.of(
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
            "fileset-ak",
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
            "fileset-sk");
    Map<String, String> schemaProperties = ImmutableMap.of();
    Map<String, String> catalogProperties = ImmutableMap.of();

    Set<String> providers =
        CredentialUtils.getCredentialProvidersByOrderOrInfer(
            () -> filesetProperties, () -> schemaProperties, () -> catalogProperties);
    Assertions.assertEquals(
        ImmutableSet.of(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE), providers);
  }

  @Test
  void testExplicitCatalogProvidersWinOverFilesetKeys() {
    Map<String, String> filesetProperties =
        ImmutableMap.of(
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
            "fileset-ak",
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
            "fileset-sk");
    Map<String, String> schemaProperties = ImmutableMap.of();
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            CredentialConstants.CREDENTIAL_PROVIDERS, S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE);

    Set<String> providers =
        CredentialUtils.getCredentialProvidersByOrderOrInfer(
            () -> filesetProperties, () -> schemaProperties, () -> catalogProperties);
    Assertions.assertEquals(ImmutableSet.of(S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE), providers);
  }

  @Test
  void testSchemaKeysInferredWhenChainOmitsProviders() {
    Map<String, String> filesetProperties = ImmutableMap.of();
    Map<String, String> schemaProperties =
        ImmutableMap.of(
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
            "schema-ak",
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
            "schema-sk");
    Map<String, String> catalogProperties = ImmutableMap.of();

    Set<String> providers =
        CredentialUtils.getCredentialProvidersByOrderOrInfer(
            () -> filesetProperties, () -> schemaProperties, () -> catalogProperties);
    Assertions.assertEquals(
        ImmutableSet.of(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE), providers);
  }

  @Test
  void testFilesetKeysWinOverCatalogWhenInferring() {
    Map<String, String> filesetProperties =
        ImmutableMap.of(
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
            "fileset-ak",
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
            "fileset-sk");
    Map<String, String> schemaProperties = ImmutableMap.of();
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
            "catalog-ak",
            S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
            "catalog-sk");

    Set<String> providers =
        CredentialUtils.getCredentialProvidersByOrderOrInfer(
            () -> filesetProperties, () -> schemaProperties, () -> catalogProperties);
    Assertions.assertEquals(
        ImmutableSet.of(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE), providers);
  }

  @Test
  void testInferEmptyWhenNoKeysAndNoProviders() {
    Set<String> providers =
        CredentialUtils.getCredentialProvidersByOrderOrInfer(
            ImmutableMap::of, ImmutableMap::of, ImmutableMap::of);
    Assertions.assertTrue(providers.isEmpty());
  }

  @Test
  void testAddStorageCredentialProvidersNullSafe() {
    Assertions.assertDoesNotThrow(
        () -> CredentialUtils.addStorageCredentialProviders(null, new ArrayList<>()));
    Assertions.assertDoesNotThrow(
        () -> CredentialUtils.addStorageCredentialProviders(ImmutableMap.of(), null));
    Assertions.assertTrue(CredentialUtils.inferStorageCredentialProviders(null).isEmpty());
  }
}
