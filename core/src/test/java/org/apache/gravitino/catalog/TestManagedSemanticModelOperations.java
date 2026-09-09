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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore.InMemoryEntityStore;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class TestManagedSemanticModelOperations {

  private static final Namespace NAMESPACE = Namespace.of("metalake", "catalog", "schema");
  private static final NameIdentifier IDENT = NameIdentifier.of(NAMESPACE, "sales_model");

  @Test
  public void testCreateThenLoadFromMemoryStore() {
    InMemoryEntityStore store = new InMemoryEntityStore();
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            store, new RandomIdGenerator(), (ident, definition) -> {});

    SemanticModel created =
        operations.createSemanticModel(
            IDENT, "Sales model", definition("orders"), Map.of("domain", "sales"));
    SemanticModel loaded = operations.loadSemanticModel(IDENT);

    assertSame(created, loaded);
    assertEquals("sales_model", loaded.name());
    assertEquals("Sales model", loaded.comment());
    assertEquals(definition("orders"), loaded.definition());
    assertEquals(Map.of("domain", "sales"), loaded.properties());
    assertNotNull(loaded.auditInfo().creator());
    assertNotNull(loaded.auditInfo().createTime());
  }

  @Test
  public void testValidationAndPersistenceOrder() throws IOException {
    EntityStore store = mock(EntityStore.class);
    IdGenerator idGenerator = mock(IdGenerator.class);
    @SuppressWarnings("unchecked")
    BiConsumer<NameIdentifier, SemanticModelDefinition> writeValidator = mock(BiConsumer.class);
    when(idGenerator.nextId()).thenReturn(7L);
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(store, idGenerator, writeValidator);
    SemanticModelDefinition definition = definition("orders");

    SemanticModel created =
        operations.createSemanticModel(IDENT, null, definition, Map.of("key", "value"));

    InOrder order = inOrder(writeValidator, idGenerator, store);
    order.verify(writeValidator).accept(IDENT, definition);
    order.verify(idGenerator).nextId();
    order.verify(store).put(any(SemanticModelEntity.class), eq(false));
    assertEquals(7L, ((SemanticModelEntity) created).id());
  }

  @Test
  public void testWriteValidationPrecedesEntityConstructionAndPersistence() {
    @SuppressWarnings("unchecked")
    BiConsumer<NameIdentifier, SemanticModelDefinition> writeValidator = mock(BiConsumer.class);
    IllegalSemanticModelException failure =
        new IllegalSemanticModelException("Semantic Model definition is invalid");
    doThrow(failure).when(writeValidator).accept(eq(IDENT), any(SemanticModelDefinition.class));
    EntityStore store = mock(EntityStore.class);
    IdGenerator idGenerator = mock(IdGenerator.class);
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(store, idGenerator, writeValidator);

    assertSame(
        failure,
        assertThrows(
            IllegalSemanticModelException.class,
            () -> operations.createSemanticModel(IDENT, null, definition("orders"), Map.of())));
    verifyNoInteractions(idGenerator, store);
  }

  @Test
  public void testLoadDoesNotRunAnyValidation() throws IOException {
    EntityStore store = mock(EntityStore.class);
    @SuppressWarnings("unchecked")
    BiConsumer<NameIdentifier, SemanticModelDefinition> writeValidator = mock(BiConsumer.class);
    SemanticModelEntity entity = entity();
    when(store.get(IDENT, Entity.EntityType.SEMANTIC_MODEL, SemanticModelEntity.class))
        .thenReturn(entity);
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(store, mock(IdGenerator.class), writeValidator);

    assertSame(entity, operations.loadSemanticModel(IDENT));

    verifyNoInteractions(writeValidator);
  }

  @Test
  public void testCreateExceptionMapping() throws IOException {
    assertCreateFailure(new NoSuchEntityException("Missing parent"), NoSuchSchemaException.class);
    assertCreateFailure(
        new EntityAlreadyExistsException("Already exists"),
        SemanticModelAlreadyExistsException.class);

    IOException ioFailure = new IOException("Write failed");
    RuntimeException wrapped = assertCreateFailure(ioFailure, RuntimeException.class);
    assertSame(ioFailure, wrapped.getCause());
  }

  @Test
  public void testLoadExceptionMapping() throws IOException {
    EntityStore store = mock(EntityStore.class);
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            store, mock(IdGenerator.class), (ident, definition) -> {});

    when(store.get(IDENT, Entity.EntityType.SEMANTIC_MODEL, SemanticModelEntity.class))
        .thenThrow(new NoSuchEntityException("Missing model"));
    assertThrows(NoSuchSemanticModelException.class, () -> operations.loadSemanticModel(IDENT));

    IOException ioFailure = new IOException("Read failed");
    doThrow(ioFailure)
        .when(store)
        .get(IDENT, Entity.EntityType.SEMANTIC_MODEL, SemanticModelEntity.class);
    RuntimeException wrapped =
        assertThrows(RuntimeException.class, () -> operations.loadSemanticModel(IDENT));
    assertSame(ioFailure, wrapped.getCause());
  }

  @Test
  public void testRemainingCapabilitiesAreExplicitlyUnsupported() {
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            mock(EntityStore.class), mock(IdGenerator.class), (ident, definition) -> {});

    assertThrows(
        UnsupportedOperationException.class, () -> operations.listSemanticModels(NAMESPACE));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            operations.alterSemanticModel(
                IDENT, SemanticModelChange.updateComment("Not implemented")));
    assertThrows(UnsupportedOperationException.class, () -> operations.dropSemanticModel(IDENT));
  }

  private static RuntimeException assertCreateFailure(
      Exception failure, Class<? extends RuntimeException> expectedType) throws IOException {
    EntityStore store = mock(EntityStore.class);
    doThrow(failure).when(store).put(any(SemanticModelEntity.class), eq(false));
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(store, () -> 1L, (ident, definition) -> {});

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> operations.createSemanticModel(IDENT, null, definition("orders"), Map.of()));
    assertInstanceOf(expectedType, thrown);
    return thrown;
  }

  private static SemanticModelEntity entity() {
    return SemanticModelEntity.builder()
        .withId(1L)
        .withName(IDENT.name())
        .withNamespace(IDENT.namespace())
        .withDefinition(definition("orders"))
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  private static SemanticModelDefinition definition(String datasetName) {
    return SemanticModelDefinition.builder()
        .withDatasets(new Dataset[] {dataset(datasetName)})
        .build();
  }

  private static Dataset dataset(String name) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", name))
        .build();
  }
}
