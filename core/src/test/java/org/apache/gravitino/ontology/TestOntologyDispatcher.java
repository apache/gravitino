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
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
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

  private final BaseMetalake metalake = mock(BaseMetalake.class);
  private final EventBus eventBus = mock(EventBus.class);
  private final OntologyMetaService storage = mock(OntologyMetaService.class);
  private OntologyDispatcher dispatcher;

  @BeforeEach
  void setUp() throws IOException {
    EntityStore store = mock(EntityStore.class);
    GravitinoEnv env = mock(GravitinoEnv.class);
    when(env.entityStore()).thenReturn(store);
    when(env.eventBus()).thenReturn(eventBus);
    when(env.idGenerator()).thenReturn(RandomIdGenerator.INSTANCE);
    when(store.get(METALAKE, Entity.EntityType.METALAKE, BaseMetalake.class)).thenReturn(metalake);
    when(metalake.propertiesMetadata()).thenReturn(BaseMetalake.PROPERTIES_METADATA);
    when(metalake.properties()).thenReturn(Map.of());
    try (MockedStatic<OntologyMetaService> stores = mockStatic(OntologyMetaService.class)) {
      stores.when(OntologyMetaService::getInstance).thenReturn(storage);
      dispatcher = new OntologyDispatcher(env);
    }
  }

  @Test
  void testDisabledMetalakeRejectsOntologyOperations() {
    when(metalake.properties()).thenReturn(Map.of(Metalake.PROPERTY_IN_USE, "false"));

    assertThrows(MetalakeNotInUseException.class, () -> dispatcher.listOntologies(METALAKE.name()));
    assertThrows(
        MetalakeNotInUseException.class, () -> dispatcher.dropOntology(METALAKE.name(), "Sales"));
    verifyNoInteractions(storage);
  }

  @Test
  void testImportSuccessEventIdentifiesTheCreatedOntology() {
    OntologyEntity ontology = mock(OntologyEntity.class);
    when(ontology.name()).thenReturn("Sales");
    when(storage.createOntology(
            any(), eq(METALAKE.name()), eq("Sales"), isNull(), any(), anyMap(), any()))
        .thenReturn(ontology);
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
    ArgumentCaptor<BaseEvent> events = ArgumentCaptor.forClass(BaseEvent.class);
    verify(eventBus, times(2)).dispatchEvent(events.capture());
    List<BaseEvent> recorded = events.getAllValues();
    assertInstanceOf(OntologyEvents.Before.class, recorded.get(0));
    assertInstanceOf(OntologyEvents.Success.class, recorded.get(1));
    assertEquals(
        List.of(METALAKE, NameIdentifierUtil.ofOntology(METALAKE.name(), "Sales")),
        recorded.stream().map(BaseEvent::identifier).toList());
  }
}
