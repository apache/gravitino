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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.ontology;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.CatalogTestUtils;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.IllegalOntologyMappingException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.OntologyMappingAlreadyExistsException;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.ontology.OntologyEvents;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.service.OntologyMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class TestOntologyDispatcher {
  private static final NameIdentifier METALAKE = NameIdentifier.of("lake");
  private static final NameIdentifier MODEL = NameIdentifier.of("lake", "sales", "public", "model");

  private final BaseMetalake metalake = mock(BaseMetalake.class);
  private final BaseCatalog<?> catalog = mock(BaseCatalog.class);
  private final EventBus eventBus = mock(EventBus.class);
  private final OntologyMetaService storage = mock(OntologyMetaService.class);
  private OntologyDispatcher dispatcher;

  @BeforeEach
  void setUp() throws IOException {
    EntityStore store = mock(EntityStore.class);
    GravitinoEnv env = mock(GravitinoEnv.class);
    CatalogManager catalogs = mock(CatalogManager.class);
    Capability capability = mock(Capability.class);
    when(env.entityStore()).thenReturn(store);
    when(env.catalogManager()).thenReturn(catalogs);
    when(env.eventBus()).thenReturn(eventBus);
    when(env.idGenerator()).thenReturn(RandomIdGenerator.INSTANCE);
    when(store.get(METALAKE, Entity.EntityType.METALAKE, BaseMetalake.class)).thenReturn(metalake);
    when(metalake.propertiesMetadata()).thenReturn(BaseMetalake.PROPERTIES_METADATA);
    when(metalake.properties()).thenReturn(Map.of());
    CatalogTestUtils.mockDoWithCatalog(catalogs, catalog);
    when(catalog.capability()).thenReturn(capability);
    when(capability.normalizeName(Capability.Scope.SCHEMA, "public")).thenReturn("public");
    try (MockedStatic<OntologyMetaService> stores = mockStatic(OntologyMetaService.class)) {
      stores.when(OntologyMetaService::getInstance).thenReturn(storage);
      dispatcher = new OntologyDispatcher(env);
    }
  }

  @Test
  void testDisabledCatalogRejectsMappingOperations() {
    doThrow(new CatalogNotInUseException("The catalog sales is not in use"))
        .when(catalog)
        .checkMetalakeAndCatalogInUse();

    assertThrows(
        CatalogNotInUseException.class,
        () -> dispatcher.loadMapping(MODEL, "customers", ignored -> true));
    assertThrows(CatalogNotInUseException.class, () -> dispatcher.dropMapping(MODEL, "customers"));
    verifyNoInteractions(storage);
  }

  @Test
  void testDisabledMetalakeRejectsOntologyAndMappingOperations() {
    when(metalake.properties()).thenReturn(Map.of(Metalake.PROPERTY_IN_USE, "false"));

    assertThrows(MetalakeNotInUseException.class, () -> dispatcher.listOntologies(METALAKE.name()));
    assertThrows(
        MetalakeNotInUseException.class,
        () -> dispatcher.dropOntology(METALAKE.name(), "Sales", (mapping, published) -> true));
    assertThrows(
        MetalakeNotInUseException.class,
        () -> dispatcher.listModelMappings(MODEL, ignored -> true));
    verifyNoInteractions(storage);
  }

  @Test
  void testImportSuccessEventsIdentifyTheCreatedObjects() {
    OntologyEntity ontology = mock(OntologyEntity.class);
    when(ontology.name()).thenReturn("Sales");
    when(storage.createOntology(
            any(), eq(METALAKE.name()), eq("Sales"), isNull(), any(), anyMap(), any()))
        .thenReturn(ontology);
    OntologyMapping mapping = mock(OntologyMapping.class);
    when(mapping.name()).thenReturn("customers");
    when(storage.createMapping(
            any(), eq(MODEL), eq("customers"), isNull(), eq("Sales"), any(), anyMap(), any()))
        .thenReturn(mapping);

    dispatcher.importOntology(
        METALAKE.name(),
        OssieDocument.yaml(
            """
            version: 0.3.0.dev0
            name: Sales
            ontology:
              - concept: Customer
                type: EntityType
            """));
    dispatcher.importMapping(
        MODEL,
        OssieDocument.yaml(
            """
            version: 0.3.0.dev0
            name: customers
            ontology_ref: {name: Sales}
            semantic_model_ref: {name: model}
            concept_mappings:
              - concept: Customer
                object_mappings:
                  - expression: customer.id
            """),
        ignored -> {});

    ArgumentCaptor<BaseEvent> events = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus, times(4)).dispatchEvent(events.capture());
    List<BaseEvent> recorded = events.getAllValues();
    assertInstanceOf(OntologyEvents.Before.class, recorded.get(0));
    assertInstanceOf(OntologyEvents.Success.class, recorded.get(1));
    assertInstanceOf(OntologyEvents.Before.class, recorded.get(2));
    assertInstanceOf(OntologyEvents.Success.class, recorded.get(3));
    assertEquals(
        List.of(
            METALAKE,
            NameIdentifierUtil.ofOntology(METALAKE.name(), "Sales"),
            MODEL,
            NameIdentifier.of("lake", "sales", "public", "model", "customers")),
        recorded.stream().map(BaseEvent::identifier).toList());
  }

  @Test
  void testImportFailureEventsIdentifyTheParsedObject() {
    when(storage.createMapping(
            any(), eq(MODEL), eq("customers"), isNull(), eq("Sales"), any(), anyMap(), any()))
        .thenThrow(new OntologyMappingAlreadyExistsException("Mapping customers already exists"));

    assertThrows(
        OntologyMappingAlreadyExistsException.class,
        () ->
            dispatcher.importMapping(
                MODEL,
                OssieDocument.yaml(
                    """
                    version: 0.3.0.dev0
                    name: customers
                    ontology_ref: {name: Sales}
                    semantic_model_ref: {name: model}
                    concept_mappings:
                      - concept: Customer
                        object_mappings:
                          - expression: customer.id
                    """),
                ignored -> {}));
    assertThrows(
        IllegalOntologyMappingException.class,
        () -> dispatcher.importMapping(MODEL, OssieDocument.yaml("name: [broken"), ignored -> {}));

    ArgumentCaptor<BaseEvent> events = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus, times(4)).dispatchEvent(events.capture());
    List<BaseEvent> recorded = events.getAllValues();
    assertInstanceOf(OntologyEvents.Failure.class, recorded.get(1));
    assertInstanceOf(OntologyEvents.Failure.class, recorded.get(3));
    assertEquals(
        List.of(
            MODEL,
            NameIdentifier.of("lake", "sales", "public", "model", "customers"),
            MODEL,
            MODEL),
        recorded.stream().map(BaseEvent::identifier).toList());
  }
}
