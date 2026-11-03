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
package org.apache.gravitino.catalog.doris.operation;

import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.catalog.doris.converter.DorisColumnDefaultValueConverter;
import org.apache.gravitino.catalog.doris.converter.DorisExceptionConverter;
import org.apache.gravitino.catalog.doris.converter.DorisTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.catalog.jdbc.utils.ConnectionCountingDataSource;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestDorisPartitionSqlGeneration {
  @ParameterizedTest
  @ValueSource(strings = {"range", "list", "multi-list"})
  void testCreateAndAddPartitionEscapeValues(String kind) {
    Literal<String> value = Literals.stringLiteral("say \"hi\" C:\\temp\\; --");
    String quoted = "\"say \"\"hi\"\" C:\\\\temp\\\\; --\"";
    Partition partition;
    Transform transform;
    String fragment;
    if ("range".equals(kind)) {
      RangePartition range = Partitions.range("p", value, Literals.NULL, Collections.emptyMap());
      partition = range;
      transform = Transforms.range(new String[] {"col1"}, new RangePartition[] {range});
      fragment = "PARTITION `p` VALUES LESS THAN (" + quoted + ")";
    } else {
      boolean multiColumn = "multi-list".equals(kind);
      ListPartition list =
          Partitions.list(
              "p",
              multiColumn ? new Literal<?>[][] {{value, value}} : new Literal<?>[][] {{value}},
              Collections.emptyMap());
      partition = list;
      transform =
          Transforms.list(
              multiColumn ? new String[][] {{"col1"}, {"col2"}} : new String[][] {{"col1"}},
              new ListPartition[] {list});
      fragment =
          "PARTITION `p` VALUES IN ("
              + (multiColumn ? "(" + quoted + "," + quoted + ")" : quoted)
              + ")";
    }
    JdbcColumn[] columns = {
      JdbcColumn.builder().withName("col1").withType(Types.VarCharType.of(128)).build(),
      JdbcColumn.builder().withName("col2").withType(Types.VarCharType.of(128)).build()
    };
    ConnectionCountingDataSource connections = new ConnectionCountingDataSource();
    DorisTableOperations operations = new DorisTableOperations();
    DorisExceptionConverter exceptions = new DorisExceptionConverter();
    DorisTypeConverter types = new DorisTypeConverter();
    operations.initialize(
        connections.dataSource(),
        exceptions,
        types,
        new DorisColumnDefaultValueConverter(),
        Collections.emptyMap());
    operations.create(
        "database",
        "test_table",
        columns,
        null,
        Map.of("replication_num", "1"),
        new Transform[] {transform},
        Distributions.hash(1, NamedReference.field("col1")),
        Indexes.EMPTY_INDEXES);
    Assertions.assertTrue(connections.executedSql().get(0).contains(fragment));
    JdbcTable table =
        JdbcTable.builder()
            .withName("test_table")
            .withDatabaseName("database")
            .withColumns(columns)
            .withPartitioning(new Transform[] {transform})
            .build();
    DorisTablePartitionOperations partitions =
        new DorisTablePartitionOperations(connections.dataSource(), table, exceptions, types);
    partitions.addPartition(partition);
    Assertions.assertEquals(2, connections.executedSql().size());
    Assertions.assertEquals(
        "ALTER TABLE `test_table` ADD " + fragment, connections.executedSql().get(1));
    Assertions.assertEquals(0, connections.borrowed());
  }
}
