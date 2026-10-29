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
package org.apache.gravitino.ontology;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestOntologyOperationDispatcher {

  private static final String METALAKE = "metalake";
  private static final NameIdentifier METALAKE_IDENT = NameIdentifier.of(METALAKE);

  private EntityStore store;
  private OntologyOperationDispatcher dispatcher;

  @BeforeEach
  public void setUp() throws IOException {
    store = mock(EntityStore.class);
    IdGenerator idGenerator = mock(IdGenerator.class);
    when(idGenerator.nextId()).thenReturn(42L);
    dispatcher = new OntologyOperationDispatcher(store, idGenerator);
  }

  private void givenMetalakeExists() throws IOException {
    BaseMetalake metalake = mock(BaseMetalake.class);
    PropertiesMetadata propertiesMetadata = mock(PropertiesMetadata.class);
    when(propertiesMetadata.getOrDefault(any(), anyString())).thenReturn(true);
    when(metalake.propertiesMetadata()).thenReturn(propertiesMetadata);
    when(metalake.properties()).thenReturn(Collections.emptyMap());
    when(store.get(eq(METALAKE_IDENT), eq(Entity.EntityType.METALAKE), eq(BaseMetalake.class)))
        .thenReturn(metalake);
  }

  private static OntologyDefinition definition(String conceptName) {
    return OntologyDefinition.builder()
        .withConcepts(
            OntologyConcept.builder()
                .withConcept(conceptName)
                .withType(ConceptType.ENTITY_TYPE)
                .build())
        .build();
  }

  private static OntologyEntity entity(
      String name, Ontology.Status status, Map<String, String> properties) {
    return OntologyEntity.builder()
        .withId(7L)
        .withName(name)
        .withNamespace(Namespace.of(METALAKE))
        .withStatus(status)
        .withDefinition(definition("Account"))
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.EPOCH).build())
        .build();
  }

  @Test
  public void testListOntologies() throws IOException {
    givenMetalakeExists();
    when(store.list(
            eq(Namespace.of(METALAKE)), eq(OntologyEntity.class), eq(Entity.EntityType.ONTOLOGY)))
        .thenReturn(
            Arrays.asList(
                entity("Finance", Ontology.Status.DRAFT, Collections.emptyMap()),
                entity("Sales", Ontology.Status.PUBLISHED, Collections.emptyMap())));

    Assertions.assertArrayEquals(
        new String[] {"Finance", "Sales"}, dispatcher.listOntologies(METALAKE));
    Assertions.assertEquals(2, dispatcher.listOntologiesInfo(METALAKE).length);
  }

  @Test
  public void testListOntologiesWhenListIsNull() throws IOException {
    givenMetalakeExists();
    when(store.list(
            eq(Namespace.of(METALAKE)), eq(OntologyEntity.class), eq(Entity.EntityType.ONTOLOGY)))
        .thenReturn(null);

    Assertions.assertEquals(0, dispatcher.listOntologies(METALAKE).length);
  }

  @Test
  public void testCreateOntologyStartsWithDraftStatus() throws IOException {
    givenMetalakeExists();

    Ontology created =
        dispatcher.createOntology(METALAKE, "Finance", "a comment", null, definition("Account"));

    ArgumentCaptor<OntologyEntity> captor = ArgumentCaptor.forClass(OntologyEntity.class);
    verify(store).put(captor.capture(), eq(false));

    OntologyEntity stored = captor.getValue();
    Assertions.assertEquals(42L, stored.id());
    Assertions.assertEquals("Finance", stored.name());
    Assertions.assertEquals(Namespace.of(METALAKE), stored.namespace());
    Assertions.assertEquals(Ontology.Status.DRAFT, stored.status());
    Assertions.assertNotNull(stored.auditInfo().creator());
    Assertions.assertEquals("Finance", created.name());
  }

  @Test
  public void testCreateOntologyConflict() throws IOException {
    givenMetalakeExists();
    Mockito.doThrow(new EntityAlreadyExistsException("conflict"))
        .when(store)
        .put(any(), anyBoolean());

    Assertions.assertThrows(
        OntologyAlreadyExistsException.class,
        () -> dispatcher.createOntology(METALAKE, "Finance", null, null, definition("Account")));
  }

  @Test
  public void testGetOntology() throws IOException {
    givenMetalakeExists();
    OntologyEntity stored = entity("Finance", Ontology.Status.DRAFT, Collections.emptyMap());
    when(store.get(
            eq(NameIdentifier.of(METALAKE, "Finance")),
            eq(Entity.EntityType.ONTOLOGY),
            eq(OntologyEntity.class)))
        .thenReturn(stored);

    Assertions.assertEquals("Finance", dispatcher.getOntology(METALAKE, "Finance").name());
  }

  @Test
  public void testGetMissingOntology() throws IOException {
    givenMetalakeExists();
    when(store.get(any(), eq(Entity.EntityType.ONTOLOGY), eq(OntologyEntity.class)))
        .thenThrow(new NoSuchEntityException("missing"));

    Assertions.assertThrows(
        NoSuchOntologyException.class, () -> dispatcher.getOntology(METALAKE, "missing"));
  }

  @SuppressWarnings("unchecked")
  private OntologyEntity applyAlter(OntologyEntity existing, OntologyChange... changes)
      throws IOException {
    givenMetalakeExists();
    when(store.update(
            eq(NameIdentifier.of(METALAKE, existing.name())),
            eq(OntologyEntity.class),
            eq(Entity.EntityType.ONTOLOGY),
            any(Function.class)))
        .thenAnswer(
            invocation -> {
              Function<OntologyEntity, OntologyEntity> updater = invocation.getArgument(3);
              return updater.apply(existing);
            });
    return (OntologyEntity) dispatcher.alterOntology(METALAKE, existing.name(), changes);
  }

  @Test
  public void testAlterAppliesRenameCommentAndProperties() throws IOException {
    OntologyEntity existing =
        entity("Finance", Ontology.Status.DRAFT, Collections.singletonMap("keep", "1"));

    OntologyEntity altered =
        applyAlter(
            existing,
            OntologyChange.rename("FinanceV2"),
            OntologyChange.updateComment("new comment"),
            OntologyChange.setProperty("added", "2"),
            OntologyChange.removeProperty("keep"));

    Assertions.assertEquals("FinanceV2", altered.name());
    Assertions.assertEquals("new comment", altered.comment());
    Assertions.assertEquals(Collections.singletonMap("added", "2"), altered.properties());
    Assertions.assertEquals(7L, altered.id());
    Assertions.assertEquals(Namespace.of(METALAKE), altered.namespace());
  }

  @Test
  public void testAlterStatusAndDefinition() throws IOException {
    OntologyEntity existing = entity("Finance", Ontology.Status.DRAFT, Collections.emptyMap());

    OntologyEntity altered =
        applyAlter(
            existing,
            OntologyChange.replaceDefinition(definition("Customer")),
            OntologyChange.updateStatus(Ontology.Status.PUBLISHED));

    Assertions.assertEquals(Ontology.Status.PUBLISHED, altered.status());
    Assertions.assertEquals(1, altered.definition().concepts().length);
    Assertions.assertEquals("Customer", altered.definition().concepts()[0].concept());
  }

  @Test
  public void testAlterOnMissingOntology() throws IOException {
    givenMetalakeExists();
    when(store.update(any(), eq(OntologyEntity.class), eq(Entity.EntityType.ONTOLOGY), any()))
        .thenThrow(new NoSuchEntityException("missing"));

    Assertions.assertThrows(
        NoSuchOntologyException.class,
        () -> dispatcher.alterOntology(METALAKE, "missing", OntologyChange.rename("other")));
  }

  @Test
  public void testAlterRejectsUnknownChange() throws IOException {
    OntologyEntity existing = entity("Finance", Ontology.Status.DRAFT, Collections.emptyMap());
    OntologyChange unknown = new OntologyChange() {};

    Assertions.assertThrows(IllegalArgumentException.class, () -> applyAlter(existing, unknown));
  }

  @Test
  public void testDeleteOntology() throws IOException {
    givenMetalakeExists();
    when(store.delete(eq(NameIdentifier.of(METALAKE, "Finance")), eq(Entity.EntityType.ONTOLOGY)))
        .thenReturn(true);

    Assertions.assertTrue(dispatcher.deleteOntology(METALAKE, "Finance"));
  }

  @Test
  public void testDeleteAbsentOntology() throws IOException {
    givenMetalakeExists();
    when(store.delete(any(), eq(Entity.EntityType.ONTOLOGY))).thenReturn(false);

    Assertions.assertFalse(dispatcher.deleteOntology(METALAKE, "absent"));
  }

  @Test
  public void testUnknownMetalakeIsRejected() throws IOException {
    when(store.get(eq(METALAKE_IDENT), eq(Entity.EntityType.METALAKE), eq(BaseMetalake.class)))
        .thenThrow(new NoSuchEntityException("no metalake"));

    Assertions.assertThrows(
        NoSuchMetalakeException.class, () -> dispatcher.listOntologies(METALAKE));
    verify(store, never())
        .list(any(Namespace.class), eq(OntologyEntity.class), eq(Entity.EntityType.ONTOLOGY));
  }

  @Test
  public void testExportUsesTheStoredOntology() throws IOException {
    givenMetalakeExists();
    when(store.get(any(), eq(Entity.EntityType.ONTOLOGY), eq(OntologyEntity.class)))
        .thenReturn(entity("Finance", Ontology.Status.PUBLISHED, Collections.emptyMap()));

    List<OntologyConcept> concepts =
        Arrays.asList(dispatcher.getOntology(METALAKE, "Finance").definition().concepts());
    Assertions.assertEquals(1, concepts.size());
  }

  @Test
  public void testGetOntologyEntityWrapsIoFailure() throws IOException {
    when(store.get(any(), eq(Entity.EntityType.ONTOLOGY), eq(OntologyEntity.class)))
        .thenThrow(new IOException("io"));

    Assertions.assertThrows(
        RuntimeException.class,
        () -> dispatcher.getOntologyEntity(NameIdentifier.of(METALAKE, "Finance")));
  }
}
