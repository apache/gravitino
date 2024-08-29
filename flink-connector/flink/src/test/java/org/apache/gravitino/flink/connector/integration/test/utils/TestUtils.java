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
package org.apache.gravitino.flink.connector.integration.test.utils;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.gravitino.rel.Column;
import org.junit.jupiter.api.Assertions;

public class TestUtils {

  public static void assertTableResult(
      TableResult actual, ResultKind expectedResultKind, Row... expected) {
    ResultKind actualRowKind = actual.getResultKind();
    Assertions.assertEquals(expectedResultKind, actualRowKind);

    if (actualRowKind == ResultKind.SUCCESS_WITH_CONTENT) {
      List<Row> actualRows = Lists.newArrayList(actual.collect());
      Assertions.assertEquals(expected.length, actualRows.size());
      for (int i = 0; i < expected.length; i++) {
        Row expectedRow = expected[i];
        Row actualRow = actualRows.get(i);
        Assertions.assertEquals(expectedRow.getKind(), actualRow.getKind());
        Assertions.assertEquals(expectedRow, actualRow);
      }
    }
  }

  public static void assertColumns(Column[] expected, Column[] actual) {
    Assertions.assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      Assertions.assertEquals(expected[i].name(), actual[i].name());
      Assertions.assertEquals(expected[i].comment(), actual[i].comment());
      Assertions.assertEquals(
          expected[i].dataType().simpleString(), actual[i].dataType().simpleString());
      Assertions.assertEquals(expected[i].defaultValue(), actual[i].defaultValue());
      Assertions.assertEquals(expected[i].autoIncrement(), actual[i].autoIncrement());
      Assertions.assertEquals(expected[i].nullable(), actual[i].nullable());
    }
  }

  public static org.apache.flink.table.catalog.Column[] toFlinkPhysicalColumn(
      List<Schema.UnresolvedColumn> unresolvedPhysicalColumns) {
    return unresolvedPhysicalColumns.stream()
        .map(
            column -> {
              Schema.UnresolvedPhysicalColumn unresolvedPhysicalColumn =
                  (Schema.UnresolvedPhysicalColumn) column;
              return org.apache.flink.table.catalog.Column.physical(
                      unresolvedPhysicalColumn.getName(),
                      (DataType) unresolvedPhysicalColumn.getDataType())
                  .withComment(unresolvedPhysicalColumn.getComment().orElse(null));
            })
        .toArray(org.apache.flink.table.catalog.Column[]::new);
  }
}
