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
package org.apache.gravitino.secret;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSecretPropertiesOperationDispatcher {

  @Test
  public void testGetSecretPropertiesResolvesOnlySecretKeys() throws Exception {
    EntityStore store = mock(EntityStore.class);
    SecretManager secretManager = mock(SecretManager.class);
    CatalogManager catalogManager = mock(CatalogManager.class);
    IdGenerator idGenerator = mock(IdGenerator.class);

    String urn = "urn:gravitino-secret:memory:default:jdbc-password";
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(1L)
            .withName("c1")
            .withNamespace(Namespace.of("ml"))
            .withType(org.apache.gravitino.Catalog.Type.RELATIONAL)
            .withProvider("jdbc-mysql")
            .withProperties(
                Map.of("jdbc-url", "jdbc:mysql://localhost:3306/db", "jdbc-password", urn))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("test")
                    .withCreateTime(java.time.Instant.now())
                    .build())
            .build();

    when(store.get(any(), any(), any())).thenReturn(entity);
    when(secretManager.toPlaintextProperties(any()))
        .thenAnswer(
            invocation -> {
              Map<String, String> input = invocation.getArgument(0);
              Assertions.assertEquals(1, input.size());
              Assertions.assertEquals(urn, input.get("jdbc-password"));
              return Map.of("jdbc-password", "plaintext-password");
            });

    SecretPropertiesOperationDispatcher dispatcher =
        new SecretPropertiesOperationDispatcher(catalogManager, store, idGenerator, secretManager);

    Map<String, String> result =
        dispatcher.getSecretProperties(NameIdentifier.of("ml", "c1"), Entity.EntityType.CATALOG);

    Assertions.assertEquals(Map.of("jdbc-password", "plaintext-password"), result);
  }
}
