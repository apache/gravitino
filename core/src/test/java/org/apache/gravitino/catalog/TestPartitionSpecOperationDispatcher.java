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

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestPartitionSpecOperationDispatcher extends TestOperationDispatcher {

  private static PartitionSpecOperationDispatcher partitionSpecOperationDispatcher;
  private static final String SCHEMA = "test_partition_spec_schema";
  private static final String TABLE = "test_partition_spec_table";
  private static final NameIdentifier TABLE_IDENT =
      NameIdentifierUtil.ofTable(metalake, catalog, SCHEMA, TABLE);

  @BeforeAll
  public static void initialize() throws IllegalAccessException {
    SchemaOperationDispatcher schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    TableOperationDispatcher tableOperationDispatcher =
        new TableOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    partitionSpecOperationDispatcher =
        new PartitionSpecOperationDispatcher(
            catalogManager, entityStore, idGenerator, secretManager);

    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalSchemaDispatcher", schemaOperationDispatcher, true);

    NameIdentifier schemaIdent = NameIdentifierUtil.ofSchema(metalake, catalog, SCHEMA);
    schemaOperationDispatcher.createSchema(schemaIdent, "comment", ImmutableMap.of("k1", "v1"));
    Column[] columns =
        new Column[] {
          Column.of("col1", Types.StringType.get()), Column.of("col2", Types.StringType.get())
        };
    tableOperationDispatcher.createTable(TABLE_IDENT, columns, "comment", null);
  }

  @Test
  public void testUnsupportedPartitionSpecs() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> partitionSpecOperationDispatcher.partitionSpecs(TABLE_IDENT));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> partitionSpecOperationDispatcher.defaultPartitionSpecId(TABLE_IDENT));
  }
}
