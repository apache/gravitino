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

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.secret.SecretManager;
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

  private static <T> Answer<T> requireActive(AtomicBoolean callbackActive, T value) {
    return invocation -> {
      Assertions.assertTrue(
          callbackActive.get(), "connector metadata must be read while the lease is active");
      return value;
    };
  }

  private static final class SnapshotDispatcher extends OperationDispatcher {

    private SnapshotDispatcher(CatalogManager catalogManager) {
      super(
          catalogManager,
          mock(EntityStore.class),
          mock(IdGenerator.class),
          mock(SecretManager.class));
    }

    private Table returnTable(Table table) {
      return doWithCatalog(
          NameIdentifier.of("metalake", "catalog"), wrapper -> table, RuntimeException.class);
    }
  }
}
