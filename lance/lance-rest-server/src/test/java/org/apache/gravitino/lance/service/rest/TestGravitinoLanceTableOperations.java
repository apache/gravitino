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

package org.apache.gravitino.lance.service.rest;

import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_VERSION;

import com.lancedb.lance.namespace.model.AlterTableAlterColumnsRequest;
import com.lancedb.lance.namespace.model.AlterTableAlterColumnsResponse;
import com.lancedb.lance.namespace.model.AlterTableDropColumnsRequest;
import com.lancedb.lance.namespace.model.AlterTableDropColumnsResponse;
import com.lancedb.lance.namespace.model.ColumnAlteration;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.lance.common.ops.gravitino.GravitinoLanceTableOperations;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestGravitinoLanceTableOperations {

  @Test
  void testDropColumnsHandlerBuildsChangesAndSetsVersion() {
    AlterTableDropColumnsRequest request = new AlterTableDropColumnsRequest();
    request.setColumns(List.of("col1", "col2"));

    GravitinoLanceTableOperations.DropColumns handler =
        new GravitinoLanceTableOperations.DropColumns();

    TableChange[] changes = handler.buildGravitinoTableChange(request);
    Assertions.assertEquals(2, changes.length);

    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(Map.of(LANCE_TABLE_VERSION, "7"));

    AlterTableDropColumnsResponse response = handler.handle(table, request);
    Assertions.assertEquals(7L, response.getVersion());
  }

  @Test
  void testAlterColumnsHandlerBuildsChangesAndSetsVersion() {
    AlterTableAlterColumnsRequest request = new AlterTableAlterColumnsRequest();
    ColumnAlteration alteration = new ColumnAlteration();
    alteration.setColumn("c1");
    alteration.setRename("c1_new");
    request.setAlterations(List.of(alteration));

    GravitinoLanceTableOperations.AlterColumns handler =
        new GravitinoLanceTableOperations.AlterColumns();

    TableChange[] changes = handler.buildGravitinoTableChange(request);
    Assertions.assertEquals(1, changes.length);

    Table table = Mockito.mock(Table.class);
    Mockito.when(table.properties()).thenReturn(Map.of(LANCE_TABLE_VERSION, "3"));

    AlterTableAlterColumnsResponse response = handler.handle(table, request);
    Assertions.assertEquals(3L, response.getVersion());
  }
}
