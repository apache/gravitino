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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Schema;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.file.FileInfo;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.ThrowableFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

class TestOperationDispatcherSnapshots {

  @Test
  void testConnectorTableIsDetachedBeforeLeaseCallbackReturns() throws Exception {
    AtomicBoolean callbackActive = new AtomicBoolean(false);
    CatalogManager catalogManager = mock(CatalogManager.class);
    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    doAnswer(
            invocation -> {
              ThrowableFunction<CatalogManager.CatalogWrapper, Object> operation =
                  invocation.getArgument(1);
              callbackActive.set(true);
              try {
                return operation.apply(wrapper);
              } finally {
                callbackActive.set(false);
              }
            })
        .when(catalogManager)
        .doWithCatalogWrapper(any(), any());

    CatalogTestUtils.mockDetachConnectorResult(wrapper);

    Table connectorTable = mock(Table.class);
    when(connectorTable.name()).thenAnswer(requireActive(callbackActive, "table"));
    when(connectorTable.comment()).thenAnswer(requireActive(callbackActive, "comment"));
    when(connectorTable.columns()).thenAnswer(requireActive(callbackActive, new Column[0]));
    when(connectorTable.properties())
        .thenAnswer(requireActive(callbackActive, Collections.emptyMap()));
    when(connectorTable.sortOrder()).thenAnswer(requireActive(callbackActive, new SortOrder[0]));
    when(connectorTable.distribution())
        .thenAnswer(requireActive(callbackActive, Distributions.NONE));
    when(connectorTable.partitioning()).thenAnswer(requireActive(callbackActive, new Transform[0]));
    when(connectorTable.index()).thenAnswer(requireActive(callbackActive, new Index[0]));
    when(connectorTable.auditInfo())
        .thenAnswer(
            requireActive(
                callbackActive,
                AuditInfo.builder().withCreator("user").withCreateTime(Instant.EPOCH).build()));

    SnapshotDispatcher dispatcher = new SnapshotDispatcher(catalogManager);
    Table result = dispatcher.returnTable(connectorTable);

    Assertions.assertFalse(callbackActive.get());
    Assertions.assertInstanceOf(TableDTO.class, result);
    Assertions.assertEquals("table", result.name());
    Assertions.assertEquals("comment", result.comment());
  }

  @Test
  void testFilesetSnapshotCopiesMapsWithoutChangingMutability() {
    Map<String, String> locations = new HashMap<>();
    locations.put(Fileset.LOCATION_NAME_UNKNOWN, "file:/data");
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");

    Fileset fileset = mock(Fileset.class);
    when(fileset.name()).thenReturn("fileset");
    when(fileset.type()).thenReturn(Fileset.Type.MANAGED);
    when(fileset.storageLocations()).thenReturn(locations);
    when(fileset.properties()).thenReturn(properties);
    when(fileset.auditInfo())
        .thenReturn(AuditInfo.builder().withCreator("user").withCreateTime(Instant.EPOCH).build());

    Fileset snapshot = ConnectorObjectSnapshot.detach(fileset);
    locations.put("archive", "file:/archive");
    properties.put("late", "change");

    Assertions.assertFalse(snapshot.storageLocations().containsKey("archive"));
    Assertions.assertFalse(snapshot.properties().containsKey("late"));
    Assertions.assertDoesNotThrow(() -> snapshot.properties().put("consumer", "value"));
  }

  /**
   * Every value a dispatcher returns leaves the catalog lease, so it must either carry no connector
   * state or be converted by {@link ConnectorObjectSnapshot#detach}. That method is a manual type
   * switch with a pass-through default, so nothing else stops a newly added return type from
   * silently escaping with connector classes attached. This test is that stop.
   */
  @Test
  void testEveryDispatcherReturnTypeIsDetachedOrCarriesNoConnectorState() {
    List<Class<?>> dispatchers =
        List.of(
            TableDispatcher.class,
            ViewDispatcher.class,
            SchemaDispatcher.class,
            FilesetDispatcher.class,
            TopicDispatcher.class,
            ModelDispatcher.class,
            PartitionDispatcher.class,
            FunctionDispatcher.class);

    List<String> escaping = new ArrayList<>();
    for (Class<?> dispatcher : dispatchers) {
      for (Method method : dispatcher.getMethods()) {
        Class<?> returnType = method.getReturnType();
        if (!DETACHED_TYPES.contains(returnType) && !carriesNoConnectorState(returnType)) {
          escaping.add(dispatcher.getSimpleName() + "#" + method.getName() + " -> " + returnType);
        }
      }
    }

    Assertions.assertTrue(
        escaping.isEmpty(),
        "These dispatcher results leave the catalog lease without being detached. Either add the "
            + "type to ConnectorObjectSnapshot.detach or confirm it holds no connector state and "
            + "list it here: "
            + escaping);
  }

  /** Types ConnectorObjectSnapshot.detach knows how to convert. Keep in sync with that method. */
  private static final Set<Class<?>> DETACHED_TYPES =
      Set.of(
          Table.class,
          View.class,
          Schema.class,
          Fileset.class,
          Topic.class,
          Model.class,
          ModelVersion.class,
          Partition.class,
          ModelVersion[].class,
          Partition[].class,
          FileInfo[].class);

  /**
   * Returns whether values of this type are built by Gravitino itself rather than by a connector,
   * so they never reference a catalog ClassLoader.
   *
   * <p>{@code Function} is on this list because functions are managed-only: {@code CatalogWrapper}
   * exposes no function operations at all and every {@code FunctionDispatcher} method goes through
   * {@code ManagedFunctionOperations}, which builds its results from {@code FunctionEntity}. Give a
   * catalog connector-backed functions and that stops being true, which is when this test should
   * start failing.
   */
  private static boolean carriesNoConnectorState(Class<?> type) {
    if (type.isArray()) {
      Class<?> component = type.getComponentType();
      return component.isPrimitive() || carriesNoConnectorState(component);
    }
    return type.isPrimitive()
        || type == String.class
        || type == NameIdentifier.class
        || type == Function.class;
  }

  private static <T> Answer<T> requireActive(AtomicBoolean callbackActive, T value) {
    return invocation -> {
      Assertions.assertTrue(
          callbackActive.get(), "connector metadata must be read while the lease is active");
      return value;
    };
  }

  private static final class SnapshotDispatcher extends OperationDispatcher {

    private SnapshotDispatcher(CatalogManager catalogManager) {
      super(catalogManager, mock(EntityStore.class), mock(IdGenerator.class));
    }

    private Table returnTable(Table table) {
      return doWithCatalog(
          NameIdentifier.of("metalake", "catalog"), wrapper -> table, RuntimeException.class);
    }
  }
}
