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
package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.Test;

public class TestSemanticModelMetricDispatchers {
  private static final Namespace NAMESPACE = Namespace.of("lake", "catalog", "schema");

  @Test
  void testStoreFailuresAreNotConvertedToEmptyResults() throws Exception {
    EntityStore store = mock(EntityStore.class);
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            store, RandomIdGenerator.INSTANCE, (ident, definition) -> {});
    when(store.listSemanticModelMetrics(NAMESPACE)).thenThrow(new NoSuchEntityException("missing"));
    when(store.listSemanticModelMetricInfos(NAMESPACE))
        .thenThrow(new NoSuchEntityException("missing"));
    assertThrows(NoSuchSchemaException.class, () -> operations.listMetrics(NAMESPACE));
    assertThrows(NoSuchSchemaException.class, () -> operations.listMetricInfos(NAMESPACE));
    IOException failure = new IOException("storage unavailable");
    doThrow(failure).when(store).listSemanticModelMetrics(NAMESPACE);
    doThrow(failure).when(store).listSemanticModelMetricInfos(NAMESPACE);
    assertSame(
        failure,
        assertThrows(RuntimeException.class, () -> operations.listMetrics(NAMESPACE)).getCause());
    assertSame(
        failure,
        assertThrows(RuntimeException.class, () -> operations.listMetricInfos(NAMESPACE))
            .getCause());
  }

  @Test
  void testOperationDispatcherChecksCatalogAndSchemaBeforeReading() throws Exception {
    EntityStore store = mock(EntityStore.class);
    CatalogManager catalogs = mock(CatalogManager.class);
    SchemaDispatcher schemas = mock(SchemaDispatcher.class);
    Catalog catalog = mock(Catalog.class);
    when(catalogs.loadCatalog(NameIdentifier.of("lake", "catalog"))).thenReturn(catalog);
    SemanticModelOperationDispatcher dispatcher =
        new SemanticModelOperationDispatcher(
            catalogs, schemas, store, RandomIdGenerator.INSTANCE, mock(SecretManager.class));
    when(catalog.type()).thenReturn(Catalog.Type.FILESET);
    assertThrows(UnsupportedOperationException.class, () -> dispatcher.listMetrics(NAMESPACE));
    assertThrows(UnsupportedOperationException.class, () -> dispatcher.listMetricInfos(NAMESPACE));
    verifyNoInteractions(store, schemas);
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(schemas.loadSchema(NameIdentifier.of(NAMESPACE.levels())))
        .thenThrow(new NoSuchSchemaException("missing"));
    assertThrows(NoSuchSchemaException.class, () -> dispatcher.listMetrics(NAMESPACE));
    assertThrows(NoSuchSchemaException.class, () -> dispatcher.listMetricInfos(NAMESPACE));
    verifyNoInteractions(store);
    doReturn(null).when(schemas).loadSchema(NameIdentifier.of(NAMESPACE.levels()));
    when(store.listSemanticModelMetrics(NAMESPACE)).thenReturn(List.of());
    when(store.listSemanticModelMetricInfos(NAMESPACE)).thenReturn(List.of());
    assertArrayEquals(new NameIdentifier[0], dispatcher.listMetrics(NAMESPACE));
    assertArrayEquals(new SemanticModelMetric[0], dispatcher.listMetricInfos(NAMESPACE));
    verify(store).listSemanticModelMetrics(NAMESPACE);
    verify(store).listSemanticModelMetricInfos(NAMESPACE);
  }
}
