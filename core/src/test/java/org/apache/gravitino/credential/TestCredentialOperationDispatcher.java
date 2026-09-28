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

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.TestOperationDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.credential.PathContext;
import org.apache.gravitino.connector.credential.SupportsPathBasedCredentials;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCredentialOperationDispatcher extends TestOperationDispatcher {

  @Test
  public void testMergePathContextsWithSameCredentialType() {
    List<PathContext> pathContexts =
        Arrays.asList(new PathContext("path1", "dummy"), new PathContext("path2", "dummy"));

    Map<String, CredentialContext> contexts =
        CredentialOperationDispatcher.getPathBasedCredentialContexts(
            CredentialPrivilege.WRITE, pathContexts);

    Assertions.assertEquals(1, contexts.size());
    PathBasedCredentialContext context = (PathBasedCredentialContext) contexts.get("dummy");
    Assertions.assertEquals(Set.of("path1", "path2"), context.getWritePaths());
    Assertions.assertTrue(context.getReadPaths().isEmpty());
  }

  @Test
  public void testMergePathContextsWithSameCredentialTypeAndSamePath() {
    List<PathContext> pathContexts =
        Arrays.asList(new PathContext("path1", "dummy"), new PathContext("path1", "dummy"));

    Map<String, CredentialContext> contexts =
        CredentialOperationDispatcher.getPathBasedCredentialContexts(
            CredentialPrivilege.WRITE, pathContexts);

    Assertions.assertEquals(1, contexts.size());
    PathBasedCredentialContext context = (PathBasedCredentialContext) contexts.get("dummy");
    Assertions.assertEquals(Set.of("path1"), context.getWritePaths());
    Assertions.assertTrue(context.getReadPaths().isEmpty());
  }

  @Test
  public void testFilterContextByProviderScheme() {
    CredentialProvider s3OnlyProvider =
        new CredentialProvider() {
          @Override
          public void initialize(Map<String, String> properties) {}

          @Override
          public String credentialType() {
            return "dummy";
          }

          @Override
          public Credential getCredential(CredentialContext context) {
            return null;
          }

          @Override
          public void close() {}

          @Override
          public boolean supportsScheme(String scheme) {
            return "s3".equalsIgnoreCase(scheme) || "s3a".equalsIgnoreCase(scheme);
          }
        };

    PathBasedCredentialContext context =
        new PathBasedCredentialContext(
            "user", Set.of("s3://bucket/a", "gs://bucket/b"), Set.of("s3a://bucket/c"));

    Optional<CredentialContext> filtered =
        CredentialOperationDispatcher.filterContextByProvider(s3OnlyProvider, context);
    Assertions.assertTrue(filtered.isPresent());
    PathBasedCredentialContext filteredPathBased = (PathBasedCredentialContext) filtered.get();
    Assertions.assertEquals(Set.of("s3://bucket/a"), filteredPathBased.getWritePaths());
    Assertions.assertEquals(Set.of("s3a://bucket/c"), filteredPathBased.getReadPaths());
  }

  @Test
  public void testGetCredentialsSkipsNullCredential() throws Exception {
    String nullType = "null-type";
    String validType = "valid-type";

    CredentialProvider nullProvider = mockCredentialProvider(nullType);
    CredentialProvider validProvider = mockCredentialProvider(validType);

    Credential validCredential = Mockito.mock(Credential.class);

    CatalogCredentialManager credentialManager = Mockito.mock(CatalogCredentialManager.class);
    Mockito.when(credentialManager.getCredentialProvider(nullType))
        .thenReturn(Optional.of(nullProvider));
    Mockito.when(credentialManager.getCredentialProvider(validType))
        .thenReturn(Optional.of(validProvider));
    Mockito.when(
            credentialManager.getCredential(
                Mockito.eq(nullType), Mockito.any(CredentialContext.class)))
        .thenReturn(Optional.empty());
    Mockito.when(
            credentialManager.getCredential(
                Mockito.eq(validType), Mockito.any(CredentialContext.class)))
        .thenReturn(Optional.of(validCredential));

    CatalogOperations ops =
        Mockito.mock(
            CatalogOperations.class,
            Mockito.withSettings().extraInterfaces(SupportsPathBasedCredentials.class));
    Mockito.when(
            ((SupportsPathBasedCredentials) ops).getPathContext(Mockito.any(NameIdentifier.class)))
        .thenReturn(
            Arrays.asList(
                new PathContext("s3://bucket/a", nullType),
                new PathContext("s3://bucket/b", validType)));

    BaseCatalog<?> baseCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(baseCatalog.catalogCredentialManager()).thenReturn(credentialManager);
    Mockito.when(baseCatalog.ops()).thenReturn(ops);

    CredentialOperationDispatcher dispatcher =
        new CredentialOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);

    List<Credential> credentials =
        dispatcher.getCredentials(
            baseCatalog,
            NameIdentifier.of(metalake, catalog, "schema", "fileset"),
            CredentialPrivilege.READ);

    Assertions.assertEquals(1, credentials.size());
    Assertions.assertSame(validCredential, credentials.get(0));
  }

  @Test
  public void testFilesetGetCredentialsUsesMergedStaticKeys() throws Exception {
    String schemaName = "cred_schema";
    String filesetName = "cred_fileset";
    NameIdentifier filesetIdent = NameIdentifier.of(metalake, catalog, schemaName, filesetName);

    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    entityStore.put(
        SchemaEntity.builder()
            .withId(idGenerator.nextId())
            .withName(schemaName)
            .withNamespace(Namespace.of(metalake, catalog))
            .withAuditInfo(audit)
            .withProperties(
                ImmutableMap.of(
                    S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                    "schema-ak",
                    S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                    "schema-sk"))
            .build(),
        true);
    entityStore.put(
        FilesetEntity.builder()
            .withId(idGenerator.nextId())
            .withName(filesetName)
            .withNamespace(Namespace.of(metalake, catalog, schemaName))
            .withFilesetType(Fileset.Type.EXTERNAL)
            .withStorageLocations(ImmutableMap.of("location1", "s3://bucket/path"))
            .withAuditInfo(audit)
            .withProperties(
                ImmutableMap.of(
                    S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                    "fileset-ak",
                    S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                    "fileset-sk"))
            .build(),
        true);

    String s3Type = S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE;
    CredentialProvider s3Provider = mockCredentialProvider(s3Type);
    CatalogCredentialManager credentialManager = Mockito.mock(CatalogCredentialManager.class);
    Mockito.when(credentialManager.getCredentialProvider(s3Type))
        .thenReturn(Optional.of(s3Provider));
    Mockito.when(
            credentialManager.getCredential(
                Mockito.eq(s3Type), Mockito.any(CredentialContext.class)))
        .thenReturn(Optional.of(new S3SecretKeyCredential("catalog-ak", "catalog-sk")));

    CatalogOperations ops =
        Mockito.mock(
            CatalogOperations.class,
            Mockito.withSettings().extraInterfaces(SupportsPathBasedCredentials.class));
    Mockito.when(
            ((SupportsPathBasedCredentials) ops).getPathContext(Mockito.any(NameIdentifier.class)))
        .thenReturn(Collections.singletonList(new PathContext("s3://bucket/path", s3Type)));

    BaseCatalog<?> baseCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(baseCatalog.catalogCredentialManager()).thenReturn(credentialManager);
    Mockito.when(baseCatalog.ops()).thenReturn(ops);
    Mockito.when(baseCatalog.propertiesWithCredentialProviders())
        .thenReturn(
            ImmutableMap.of(
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                "catalog-ak",
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                "catalog-sk",
                "credential-providers",
                s3Type));

    CredentialOperationDispatcher dispatcher =
        new CredentialOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);

    List<Credential> credentials =
        dispatcher.getCredentials(baseCatalog, filesetIdent, CredentialPrivilege.READ);

    Assertions.assertEquals(1, credentials.size());
    S3SecretKeyCredential s3 = (S3SecretKeyCredential) credentials.get(0);
    Assertions.assertEquals("fileset-ak", s3.accessKeyId());
    Assertions.assertEquals("fileset-sk", s3.secretAccessKey());
  }

  @Test
  public void testFilesetOverlaySkipsStaticWhenOnlyTokenSelected() throws Exception {
    String schemaName = "token_schema";
    String filesetName = "token_fileset";
    NameIdentifier filesetIdent = NameIdentifier.of(metalake, catalog, schemaName, filesetName);

    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    entityStore.put(
        SchemaEntity.builder()
            .withId(idGenerator.nextId())
            .withName(schemaName)
            .withNamespace(Namespace.of(metalake, catalog))
            .withAuditInfo(audit)
            .withProperties(ImmutableMap.of())
            .build(),
        true);
    entityStore.put(
        FilesetEntity.builder()
            .withId(idGenerator.nextId())
            .withName(filesetName)
            .withNamespace(Namespace.of(metalake, catalog, schemaName))
            .withFilesetType(Fileset.Type.EXTERNAL)
            .withStorageLocations(ImmutableMap.of("location1", "s3://bucket/path"))
            .withAuditInfo(audit)
            .withProperties(
                ImmutableMap.of(
                    S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                    "fileset-ak",
                    S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                    "fileset-sk"))
            .build(),
        true);

    String tokenType = S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE;
    CredentialProvider tokenProvider = mockCredentialProvider(tokenType);
    Credential tokenCredential =
        new S3TokenCredential("tok-ak", "tok-sk", "session", System.currentTimeMillis() + 60_000);
    CatalogCredentialManager credentialManager = Mockito.mock(CatalogCredentialManager.class);
    Mockito.when(credentialManager.getCredentialProvider(tokenType))
        .thenReturn(Optional.of(tokenProvider));
    Mockito.when(
            credentialManager.getCredential(
                Mockito.eq(tokenType), Mockito.any(CredentialContext.class)))
        .thenReturn(Optional.of(tokenCredential));

    CatalogOperations ops =
        Mockito.mock(
            CatalogOperations.class,
            Mockito.withSettings().extraInterfaces(SupportsPathBasedCredentials.class));
    Mockito.when(
            ((SupportsPathBasedCredentials) ops).getPathContext(Mockito.any(NameIdentifier.class)))
        .thenReturn(Collections.singletonList(new PathContext("s3://bucket/path", tokenType)));

    BaseCatalog<?> baseCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(baseCatalog.catalogCredentialManager()).thenReturn(credentialManager);
    Mockito.when(baseCatalog.ops()).thenReturn(ops);
    Mockito.when(baseCatalog.propertiesWithCredentialProviders())
        .thenReturn(
            ImmutableMap.of(
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                "catalog-ak",
                S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                "catalog-sk",
                "credential-providers",
                tokenType));

    CredentialOperationDispatcher dispatcher =
        new CredentialOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);

    List<Credential> credentials =
        dispatcher.getCredentials(baseCatalog, filesetIdent, CredentialPrivilege.READ);

    Assertions.assertEquals(1, credentials.size());
    Assertions.assertSame(tokenCredential, credentials.get(0));
  }

  @Test
  public void testFilesetInferredStaticWhenCatalogHasNoProvider() throws Exception {
    String schemaName = "infer_schema";
    String filesetName = "infer_fileset";
    NameIdentifier filesetIdent = NameIdentifier.of(metalake, catalog, schemaName, filesetName);

    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();
    entityStore.put(
        SchemaEntity.builder()
            .withId(idGenerator.nextId())
            .withName(schemaName)
            .withNamespace(Namespace.of(metalake, catalog))
            .withAuditInfo(audit)
            .withProperties(ImmutableMap.of())
            .build(),
        true);
    entityStore.put(
        FilesetEntity.builder()
            .withId(idGenerator.nextId())
            .withName(filesetName)
            .withNamespace(Namespace.of(metalake, catalog, schemaName))
            .withFilesetType(Fileset.Type.EXTERNAL)
            .withStorageLocations(ImmutableMap.of("location1", "s3://bucket/path"))
            .withAuditInfo(audit)
            .withProperties(
                ImmutableMap.of(
                    S3SecretKeyCredential.GRAVITINO_S3_STATIC_ACCESS_KEY_ID,
                    "fileset-ak",
                    S3SecretKeyCredential.GRAVITINO_S3_STATIC_SECRET_ACCESS_KEY,
                    "fileset-sk"))
            .build(),
        true);

    String s3Type = S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE;
    CatalogCredentialManager credentialManager = Mockito.mock(CatalogCredentialManager.class);
    Mockito.when(credentialManager.getCredentialProvider(s3Type)).thenReturn(Optional.empty());

    CatalogOperations ops =
        Mockito.mock(
            CatalogOperations.class,
            Mockito.withSettings().extraInterfaces(SupportsPathBasedCredentials.class));
    // Simulates getCredentialProvidersByOrderOrInfer when the chain omits credential-providers.
    Mockito.when(
            ((SupportsPathBasedCredentials) ops).getPathContext(Mockito.any(NameIdentifier.class)))
        .thenReturn(Collections.singletonList(new PathContext("s3://bucket/path", s3Type)));

    BaseCatalog<?> baseCatalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(baseCatalog.catalogCredentialManager()).thenReturn(credentialManager);
    Mockito.when(baseCatalog.ops()).thenReturn(ops);
    Mockito.when(baseCatalog.propertiesWithCredentialProviders()).thenReturn(ImmutableMap.of());

    CredentialOperationDispatcher dispatcher =
        new CredentialOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);

    List<Credential> credentials =
        dispatcher.getCredentials(baseCatalog, filesetIdent, CredentialPrivilege.READ);

    Assertions.assertEquals(1, credentials.size());
    S3SecretKeyCredential s3 = (S3SecretKeyCredential) credentials.get(0);
    Assertions.assertEquals("fileset-ak", s3.accessKeyId());
    Assertions.assertEquals("fileset-sk", s3.secretAccessKey());
  }

  private static CredentialProvider mockCredentialProvider(String credentialType) {
    CredentialProvider provider = Mockito.mock(CredentialProvider.class);
    Mockito.when(provider.credentialType()).thenReturn(credentialType);
    Mockito.when(provider.supportsScheme(Mockito.anyString())).thenReturn(true);
    return provider;
  }
}
