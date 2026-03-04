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

package org.apache.gravitino.maintenance.optimizer.integration.test;

import org.apache.gravitino.maintenance.optimizer.recommender.table.GravitinoTableMetadataProvider;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class GravitinoTableMetaIT extends GravitinoOptimizerEnvIT {
  private GravitinoTableMetadataProvider tableMetadataProvider;

  @BeforeAll
  void init() {
    this.tableMetadataProvider = new GravitinoTableMetadataProvider();
    tableMetadataProvider.initialize(optimizerEnv);
  }

  @AfterAll
  void closeResources() throws Exception {
    if (tableMetadataProvider != null) {
      tableMetadataProvider.close();
    }
  }

  @Test
  void testTableStatisticsUpdaterAndProvider() {
    String tableName = "test_table_metadata";
    createPartitionTable(tableName);
    Table table = tableMetadataProvider.tableMetadata(getTableIdentifier(tableName));
    Assertions.assertNotNull(table);

    // check table name, column ,partition information
    Assertions.assertEquals(tableName, table.name());
    Assertions.assertEquals(3, table.columns().length);
    Column[] columns = table.columns();
    Assertions.assertEquals("col1", columns[0].name());
    Assertions.assertEquals("col2", columns[1].name());
    Assertions.assertEquals("col3", columns[2].name());

    Assertions.assertEquals(2, table.partitioning().length);
    Transform[] partitioning = table.partitioning();
    Assertions.assertEquals(Transforms.NAME_OF_IDENTITY, partitioning[0].name());
    Assertions.assertEquals(Transforms.NAME_OF_BUCKET, partitioning[1].name());
  }
}
