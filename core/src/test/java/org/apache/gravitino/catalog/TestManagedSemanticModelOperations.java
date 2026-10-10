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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
  public void testListAlterAndDropFromMemoryStore() {
    AtomicInteger writeValidationCount = new AtomicInteger();
    InMemoryEntityStore store = new InMemoryEntityStore();
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            store,
            new RandomIdGenerator(),
            (ident, definition) -> writeValidationCount.incrementAndGet());
    SemanticModel first =
        operations.createSemanticModel(
            IDENT, "Original", definition("orders"), Map.of("domain", "sales"));
    NameIdentifier secondIdent = NameIdentifier.of(NAMESPACE, "inventory_model");
    operations.createSemanticModel(secondIdent, null, definition("inventory"), Map.of());

    NameIdentifier renamedIdent = NameIdentifier.of(NAMESPACE, "renamed_sales_model");
    SemanticModelDefinition replacement = definition("invoices");
    SemanticModel altered =
        operations.alterSemanticModel(
            IDENT,
            SemanticModelChange.rename(renamedIdent.name()),
            SemanticModelChange.updateComment("Updated"),
            SemanticModelChange.setProperty("tier", "gold"),
            SemanticModelChange.removeProperty("domain"),
            SemanticModelChange.replaceDefinition(replacement));

    assertEquals(renamedIdent.name(), altered.name());
    assertEquals("Updated", altered.comment());
    assertEquals(replacement, altered.definition());
    assertEquals(Map.of("tier", "gold"), altered.properties());
    assertEquals(first.auditInfo().creator(), altered.auditInfo().creator());
    assertEquals(first.auditInfo().createTime(), altered.auditInfo().createTime());
    assertEquals(first.auditInfo().creator(), altered.auditInfo().lastModifier());
    assertNotNull(altered.auditInfo().lastModifiedTime());
    assertEquals(3, writeValidationCount.get());
    assertThrows(NoSuchSemanticModelException.class, () -> operations.loadSemanticModel(IDENT));
    assertSame(altered, operations.loadSemanticModel(renamedIdent));
    assertEquals(
        Set.of(renamedIdent, secondIdent), Set.of(operations.listSemanticModels(NAMESPACE)));

    assertTrue(operations.dropSemanticModel(renamedIdent));
    assertFalse(operations.dropSemanticModel(renamedIdent));
    assertEquals(Set.of(secondIdent), Set.of(operations.listSemanticModels(NAMESPACE)));
  }

  @Test
  public void testSelectiveValidationAndAtomicFailures() {
    AtomicInteger writeValidationCount = new AtomicInteger();
    IllegalSemanticModelException sourceFailure =
        new IllegalSemanticModelException("Replacement source is unavailable");
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            new InMemoryEntityStore(),
            new RandomIdGenerator(),
            (ident, definition) -> {
              SemanticModelValidator.validateDefinition(definition);
              writeValidationCount.incrementAndGet();
              if (definition.datasets()[0].name().equals("blocked")) {
                throw sourceFailure;
              }
            });
    operations.createSemanticModel(IDENT, "Original", definition("orders"), Map.of());

    operations.alterSemanticModel(IDENT, SemanticModelChange.setProperty("owner", "analytics"));
    operations.alterSemanticModel(IDENT, SemanticModelChange.updateComment("Comment only"));
    assertEquals(1, writeValidationCount.get());

    SemanticModelDefinition accepted = definition("accepted");
    operations.alterSemanticModel(IDENT, SemanticModelChange.replaceDefinition(accepted));
    assertEquals(2, writeValidationCount.get());

    SemanticModelDefinition duplicateDatasets =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {dataset("duplicate"), dataset("duplicate")})
            .build();
    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            operations.alterSemanticModel(
                IDENT, SemanticModelChange.replaceDefinition(duplicateDatasets)));
    assertEquals(2, writeValidationCount.get());
    assertEquals(accepted, operations.loadSemanticModel(IDENT).definition());

    assertSame(
        sourceFailure,
        assertThrows(
            IllegalSemanticModelException.class,
            () ->
                operations.alterSemanticModel(
                    IDENT, SemanticModelChange.replaceDefinition(definition("blocked")))));
    assertEquals(3, writeValidationCount.get());
    assertEquals(accepted, operations.loadSemanticModel(IDENT).definition());
  }

  @Test
  public void testIndividualMetadataAndPropertyChangesPreserveCreationAudit() {
    AtomicInteger writeValidationCount = new AtomicInteger();
    InMemoryEntityStore store = new InMemoryEntityStore();
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            store,
            new RandomIdGenerator(),
            (ident, definition) -> writeValidationCount.incrementAndGet());
    SemanticModel original =
        operations.createSemanticModel(
            IDENT,
            "Initial comment",
            definition("orders"),
            Map.of("overwrite", "old", "remove", "present"));

    SemanticModel propertyUpdated =
        operations.alterSemanticModel(IDENT, SemanticModelChange.setProperty("overwrite", "new"));
    assertEquals(Map.of("overwrite", "new", "remove", "present"), propertyUpdated.properties());
    assertCreationAuditPreserved(original, propertyUpdated);

    SemanticModel propertiesRemoved =
        operations.alterSemanticModel(
            IDENT,
            SemanticModelChange.removeProperty("remove"),
            SemanticModelChange.removeProperty("absent"));
    assertEquals(Map.of("overwrite", "new"), propertiesRemoved.properties());
    assertCreationAuditPreserved(original, propertiesRemoved);

    SemanticModel commentRemoved =
        operations.alterSemanticModel(IDENT, SemanticModelChange.updateComment(null));
    assertNull(commentRemoved.comment());
    assertCreationAuditPreserved(original, commentRemoved);

    NameIdentifier renamedIdent = NameIdentifier.of(NAMESPACE, "individually_renamed");
    SemanticModel renamed =
        operations.alterSemanticModel(IDENT, SemanticModelChange.rename(renamedIdent.name()));
    assertEquals(renamedIdent.name(), renamed.name());
    assertEquals(((SemanticModelEntity) original).id(), ((SemanticModelEntity) renamed).id());
    assertCreationAuditPreserved(original, renamed);
    assertThrows(NoSuchSemanticModelException.class, () -> operations.loadSemanticModel(IDENT));
    assertSame(renamed, operations.loadSemanticModel(renamedIdent));
    assertEquals(1, writeValidationCount.get());
  }

  @Test
  public void testLaterMetadataChangesDoNotSkipDefinitionReplacementValidation() {
    AtomicInteger writeValidationCount = new AtomicInteger();
    AtomicReference<NameIdentifier> validatedIdent = new AtomicReference<>();
    AtomicReference<SemanticModelDefinition> validatedDefinition = new AtomicReference<>();
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            new InMemoryEntityStore(),
            new RandomIdGenerator(),
            (ident, definition) -> {
              writeValidationCount.incrementAndGet();
              validatedIdent.set(ident);
              validatedDefinition.set(definition);
            });
    operations.createSemanticModel(IDENT, "Original", definition("orders"), Map.of());
    SemanticModelDefinition replacement = definition("invoices");
    NameIdentifier renamedIdent = NameIdentifier.of(NAMESPACE, "validated_after_replace");

    SemanticModel altered =
        operations.alterSemanticModel(
            IDENT,
            SemanticModelChange.replaceDefinition(replacement),
            SemanticModelChange.setProperty("owner", "analytics"),
            SemanticModelChange.updateComment("Updated after replacement"),
            SemanticModelChange.rename(renamedIdent.name()));

    assertEquals(2, writeValidationCount.get());
    assertEquals(renamedIdent, validatedIdent.get());
    assertSame(replacement, validatedDefinition.get());
    assertEquals(replacement, altered.definition());
    assertEquals("Updated after replacement", altered.comment());
    assertEquals(Map.of("owner", "analytics"), altered.properties());
  }

  @Test
  public void testNullInMixedChangeBatchIsRejectedBeforeMutation() {
    InMemoryEntityStore store = new InMemoryEntityStore();
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            store, new RandomIdGenerator(), (ident, definition) -> {});
    SemanticModel original =
        operations.createSemanticModel(IDENT, null, definition("orders"), Map.of("key", "old"));

    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            operations.alterSemanticModel(
                IDENT,
                SemanticModelChange.setProperty("key", "must-not-persist"),
                (SemanticModelChange) null));

    assertSame(original, operations.loadSemanticModel(IDENT));
    assertEquals(Map.of("key", "old"), operations.loadSemanticModel(IDENT).properties());
  }

  @Test
  public void testMetadataAndPropertyChangesDoNotRunWriteValidation() throws IOException {
    InMemoryEntityStore store = new InMemoryEntityStore();
    SemanticModelDefinition malformedDefinition = mock(SemanticModelDefinition.class);
    store.put(entity(malformedDefinition), false);
    @SuppressWarnings("unchecked")
    BiConsumer<NameIdentifier, SemanticModelDefinition> writeValidator = mock(BiConsumer.class);
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(store, new RandomIdGenerator(), writeValidator);

    SemanticModel propertyUpdated =
        operations.alterSemanticModel(IDENT, SemanticModelChange.setProperty("safe", "true"));
    assertEquals(Map.of("safe", "true"), propertyUpdated.properties());

    SemanticModel commentUpdated =
        operations.alterSemanticModel(IDENT, SemanticModelChange.updateComment("Metadata only"));
    assertEquals("Metadata only", commentUpdated.comment());

    NameIdentifier renamedIdent = NameIdentifier.of(NAMESPACE, "metadata_only_rename");
    SemanticModel renamed =
        operations.alterSemanticModel(IDENT, SemanticModelChange.rename(renamedIdent.name()));
    assertEquals(renamedIdent.name(), renamed.name());
    assertEquals(malformedDefinition, renamed.definition());
    assertThrows(NoSuchSemanticModelException.class, () -> operations.loadSemanticModel(IDENT));
    assertSame(renamed, operations.loadSemanticModel(renamedIdent));
    verifyNoInteractions(writeValidator);
  }

  @Test
  public void testInvalidChangesAreTypedAndDoNotReachTheStore() {
    EntityStore store = mock(EntityStore.class);
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            store, mock(IdGenerator.class), (ident, definition) -> {});
    SemanticModelChange unsupported = new SemanticModelChange() {};

    assertThrows(
        IllegalSemanticModelException.class,
        () -> operations.alterSemanticModel(IDENT, (SemanticModelChange[]) null));
    assertThrows(IllegalSemanticModelException.class, () -> operations.alterSemanticModel(IDENT));
    assertThrows(
        IllegalSemanticModelException.class,
        () -> operations.alterSemanticModel(IDENT, (SemanticModelChange) null));
    assertThrows(
        IllegalSemanticModelException.class,
        () -> operations.alterSemanticModel(IDENT, unsupported));
    verifyNoInteractions(store);
  }

  @Test
  public void testLifecycleExceptionMapping() throws IOException {
    EntityStore store = mock(EntityStore.class);
    ManagedSemanticModelOperations operations =
        new ManagedSemanticModelOperations(
            store, mock(IdGenerator.class), (ident, definition) -> {});

    when(store.list(NAMESPACE, SemanticModelEntity.class, Entity.EntityType.SEMANTIC_MODEL))
        .thenThrow(new NoSuchEntityException("Missing schema"));
    assertThrows(NoSuchSchemaException.class, () -> operations.listSemanticModels(NAMESPACE));
    IOException listFailure = new IOException("List failed");
    doThrow(listFailure)
        .when(store)
        .list(NAMESPACE, SemanticModelEntity.class, Entity.EntityType.SEMANTIC_MODEL);
    assertSame(
        listFailure,
        assertThrows(RuntimeException.class, () -> operations.listSemanticModels(NAMESPACE))
            .getCause());

    doThrow(new NoSuchEntityException("Missing model"))
        .when(store)
        .update(
            eq(IDENT), eq(SemanticModelEntity.class), eq(Entity.EntityType.SEMANTIC_MODEL), any());
    assertThrows(
        NoSuchSemanticModelException.class,
        () ->
            operations.alterSemanticModel(IDENT, SemanticModelChange.setProperty("key", "value")));
    doThrow(new EntityAlreadyExistsException("Rename conflict"))
        .when(store)
        .update(
            eq(IDENT), eq(SemanticModelEntity.class), eq(Entity.EntityType.SEMANTIC_MODEL), any());
    assertThrows(
        SemanticModelAlreadyExistsException.class,
        () -> operations.alterSemanticModel(IDENT, SemanticModelChange.rename("conflict")));
    IOException updateFailure = new IOException("Update failed");
    doThrow(updateFailure)
        .when(store)
        .update(
            eq(IDENT), eq(SemanticModelEntity.class), eq(Entity.EntityType.SEMANTIC_MODEL), any());
    assertSame(
        updateFailure,
        assertThrows(
                RuntimeException.class,
                () ->
                    operations.alterSemanticModel(IDENT, SemanticModelChange.removeProperty("key")))
            .getCause());

    when(store.delete(IDENT, Entity.EntityType.SEMANTIC_MODEL))
        .thenThrow(new NoSuchEntityException("Missing model"));
    assertFalse(operations.dropSemanticModel(IDENT));
    IOException dropFailure = new IOException("Drop failed");
    doThrow(dropFailure).when(store).delete(IDENT, Entity.EntityType.SEMANTIC_MODEL);
    assertSame(
        dropFailure,
        assertThrows(RuntimeException.class, () -> operations.dropSemanticModel(IDENT)).getCause());
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

  private static void assertCreationAuditPreserved(SemanticModel original, SemanticModel altered) {
    assertEquals(original.auditInfo().creator(), altered.auditInfo().creator());
    assertEquals(original.auditInfo().createTime(), altered.auditInfo().createTime());
    assertEquals(original.auditInfo().creator(), altered.auditInfo().lastModifier());
    assertNotNull(altered.auditInfo().lastModifiedTime());
  }

  private static SemanticModelEntity entity() {
    return entity(definition("orders"));
  }

  private static SemanticModelEntity entity(SemanticModelDefinition definition) {
    return SemanticModelEntity.builder()
        .withId(1L)
        .withName(IDENT.name())
        .withNamespace(IDENT.namespace())
        .withDefinition(definition)
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
