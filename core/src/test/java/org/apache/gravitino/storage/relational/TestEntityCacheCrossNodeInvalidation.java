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
package org.apache.gravitino.storage.relational;

import java.io.IOException;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.service.SchemaMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/**
 * End-to-end test of the caffeine cross-node invalidation chain: a write on one node emits an
 * {@code entity_change_log} row, and a second node's poller replays it into that node's own {@link
 * CaffeineEntityCache}. "Node A" is the shared {@link #backend}; "node B" is a separate cache kept
 * fresh by an {@link EntityCacheChangeLogListener} on its own {@link EntityChangeLogPoller}.
 */
public class TestEntityCacheCrossNodeInvalidation extends TestJDBCBackend {

  private static final String METALAKE = "metalake_cross_node";
  private static final String CATALOG = "catalog_cross_node";
  private static final String SCHEMA = "schema_cross_node";

  // A large poll interval so the background scheduler never fires during the test; the test drives
  // node B's poll explicitly via pollChanges().
  private EntityChangeLogPoller newIdlePoller(CaffeineEntityCache nodeBCache) {
    EntityChangeLogPoller poller = new EntityChangeLogPoller(3600);
    poller.registerListener(new EntityCacheChangeLogListener(nodeBCache));
    // start() seeds the cursor with the current DB tail, modelling a node whose cache is already
    // warm: only changes written after this point are replayed.
    poller.start();
    return poller;
  }

  @TestTemplate
  void testAlterOnNodeAIsReflectedOnNodeBAfterOnePoll() throws IOException {
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
    Namespace namespace = Namespace.of(METALAKE, CATALOG, SCHEMA);
    TableEntity table =
        createTableEntity(RandomIdGenerator.INSTANCE.nextId(), namespace, "table1", AUDIT_INFO);
    backend.insert(table, false);

    // Node B has already read and cached the table.
    CaffeineEntityCache nodeBCache = new CaffeineEntityCache(new Config() {});
    nodeBCache.put(table);
    Assertions.assertTrue(nodeBCache.contains(table.nameIdentifier(), EntityType.TABLE));

    EntityChangeLogPoller nodeBPoller = newIdlePoller(nodeBCache);
    try {
      // Node A renames the table, which emits an ALTER row in the shared change log.
      backend.update(
          table.nameIdentifier(),
          EntityType.TABLE,
          entity -> createTableEntity(table.id(), namespace, "table2", AUDIT_INFO));

      // Before node B polls, it still serves the stale copy.
      Assertions.assertTrue(nodeBCache.contains(table.nameIdentifier(), EntityType.TABLE));

      // One poll later, node B has dropped the stale key.
      nodeBPoller.pollChanges();
      Assertions.assertFalse(nodeBCache.contains(table.nameIdentifier(), EntityType.TABLE));
    } finally {
      nodeBPoller.close();
    }
  }

  @TestTemplate
  void testSchemaDropOnNodeAClearsChildTablesOnNodeB() throws IOException {
    createParentEntities(METALAKE, CATALOG, SCHEMA, AUDIT_INFO);
    Namespace tableNamespace = Namespace.of(METALAKE, CATALOG, SCHEMA);
    SchemaEntity schema =
        SchemaMetaService.getInstance()
            .getSchemaByIdentifier(NameIdentifierUtil.ofSchema(METALAKE, CATALOG, SCHEMA));
    TableEntity table =
        createTableEntity(
            RandomIdGenerator.INSTANCE.nextId(), tableNamespace, "table1", AUDIT_INFO);
    backend.insert(table, false);

    // Node B has cached both the schema and its child table.
    CaffeineEntityCache nodeBCache = new CaffeineEntityCache(new Config() {});
    nodeBCache.put(schema);
    nodeBCache.put(table);
    Assertions.assertEquals(2, nodeBCache.size());

    EntityChangeLogPoller nodeBPoller = newIdlePoller(nodeBCache);
    try {
      // Node A drops the schema with cascade, emitting a single SCHEMA DROP row.
      Assertions.assertTrue(
          SchemaMetaService.getInstance().deleteSchema(schema.nameIdentifier(), true));

      nodeBPoller.pollChanges();

      // The schema drop cascades to its cached child table on node B via the forward prefix scan.
      Assertions.assertFalse(nodeBCache.contains(schema.nameIdentifier(), EntityType.SCHEMA));
      Assertions.assertFalse(nodeBCache.contains(table.nameIdentifier(), EntityType.TABLE));
    } finally {
      nodeBPoller.close();
    }
  }
}
