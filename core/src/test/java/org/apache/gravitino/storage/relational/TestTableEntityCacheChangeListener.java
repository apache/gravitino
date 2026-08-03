/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.SupportsEntityCacheInvalidation;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.junit.jupiter.api.Test;

/** Tests table-cache invalidation from peer entity-change records. */
public class TestTableEntityCacheChangeListener {

  @Test
  void testInvalidatesOnlyTableChanges() {
    SupportsEntityCacheInvalidation invalidation = mock(SupportsEntityCacheInvalidation.class);
    TableEntityCacheChangeListener listener = new TableEntityCacheChangeListener(invalidation);

    listener.onEntityChange(
        List.of(
            change("TABLE", "metalake.catalog.schema.table"),
            change("SCHEMA", "metalake.catalog.schema")));

    verify(invalidation)
        .invalidateEntityCache(
            NameIdentifier.of("metalake", "catalog", "schema", "table"), Entity.EntityType.TABLE);
    verifyNoMoreInteractions(invalidation);
  }

  private static EntityChangeRecord change(String entityType, String fullName) {
    return new EntityChangeRecord(1L, "metalake", entityType, fullName, OperateType.DROP, 0L);
  }
}
