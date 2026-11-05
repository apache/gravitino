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
package org.apache.gravitino.rel;

import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableCatalog {

  private static TableCatalog minimalCatalog(Function<NameIdentifier, Table> loader) {
    return new TableCatalog() {
      @Override
      public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
        return new NameIdentifier[0];
      }

      @Override
      public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
        return loader.apply(ident);
      }

      @Override
      public Table createTable(
          NameIdentifier ident,
          Column[] columns,
          String comment,
          Map<String, String> properties,
          Transform[] partitions,
          Distribution distribution,
          SortOrder[] sortOrders,
          Index[] indexes) {
        return null;
      }

      @Override
      public Table alterTable(NameIdentifier ident, TableChange... changes)
          throws NoSuchTableException {
        return null;
      }

      @Override
      public boolean dropTable(NameIdentifier ident) {
        return false;
      }
    };
  }

  @Test
  void testTableExistsReturnsFalseForMissingSchemaOrTable() {
    NameIdentifier ident = NameIdentifier.of("schema", "table");
    TableCatalog missingSchema =
        minimalCatalog(
            ignored -> {
              throw new NoSuchSchemaException("schema missing");
            });
    TableCatalog missingTable =
        minimalCatalog(
            ignored -> {
              throw new NoSuchTableException("table missing");
            });
    Assertions.assertFalse(missingSchema.tableExists(ident));
    Assertions.assertFalse(missingTable.tableExists(ident));
    IllegalStateException failure = new IllegalStateException("backend unavailable");
    TableCatalog unavailable =
        minimalCatalog(
            ignored -> {
              throw failure;
            });
    Assertions.assertSame(
        failure,
        Assertions.assertThrows(IllegalStateException.class, () -> unavailable.tableExists(ident)));
  }
}
