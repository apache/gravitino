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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.TestOperationDispatcher;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.credential.PathContext;
import org.apache.gravitino.connector.credential.SupportsPathBasedCredentials;
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
          public Optional<Credential> getCredential(CredentialContext context) {
            return Optional.empty();
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
        new CredentialOperationDispatcher(catalogManager, entityStore, idGenerator);

    Method method =
        CredentialOperationDispatcher.class.getDeclaredMethod(
            "getCredentials", BaseCatalog.class, NameIdentifier.class, CredentialPrivilege.class);
    method.setAccessible(true);

    @SuppressWarnings("unchecked")
    List<Credential> credentials =
        (List<Credential>)
            method.invoke(
                dispatcher,
                baseCatalog,
                NameIdentifier.of(metalake, catalog, "schema", "fileset"),
                CredentialPrivilege.READ);

    Assertions.assertEquals(1, credentials.size());
    Assertions.assertSame(validCredential, credentials.get(0));
  }

  private static CredentialProvider mockCredentialProvider(String credentialType) {
    CredentialProvider provider = Mockito.mock(CredentialProvider.class);
    Mockito.when(provider.credentialType()).thenReturn(credentialType);
    Mockito.when(provider.supportsScheme(Mockito.anyString())).thenReturn(true);
    return provider;
  }
}
