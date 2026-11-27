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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.paimon.compatibility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;

/** Runs the compatibility checks on a classpath containing only the Paimon 1.2 client. */
public final class Paimon12TestClient {

  private Paimon12TestClient() {}

  /**
   * Creates a table through REST and exercises an existing Paimon 1.2 filesystem table.
   *
   * @param args REST catalog URI and filesystem warehouse URI
   * @throws Exception if the client cannot create, commit to, or read either table
   */
  public static void main(String[] args) throws Exception {
    Options restOptions =
        Options.fromMap(
            Map.of(
                "metastore", "rest",
                "uri", args[0],
                "warehouse", "default_catalog",
                "token.provider", "bear",
                "token", System.getenv("PAIMON_COMPATIBILITY_TEST_TOKEN")));
    Options filesystemOptions =
        Options.fromMap(Map.of("metastore", "filesystem", "warehouse", args[1]));
    Schema schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT().notNull())
            .column("status", DataTypes.STRING())
            .primaryKey("id")
            .option("bucket", "1")
            .build();

    try (Catalog rest = CatalogFactory.createCatalog(CatalogContext.create(restOptions));
        Catalog filesystem =
            CatalogFactory.createCatalog(CatalogContext.create(filesystemOptions))) {
      String database = "paimon12_compatibility";
      Identifier newTable = Identifier.create(database, "new_orders");
      Identifier existingTable = Identifier.create(database, "existing_orders");
      rest.createDatabase(database, false);
      rest.createTable(newTable, schema, false);
      writeRow(rest.getTable(newTable), 1, "created");
      checkSnapshot(rest, newTable, 1, List.of("1:created"));

      filesystem.createTable(existingTable, schema, false);
      writeRow(filesystem.getTable(existingTable), 1, "existing");
      checkSnapshot(rest, existingTable, 1, List.of("1:existing"));
      writeRow(rest.getTable(existingTable), 2, "appended");
      checkSnapshot(rest, existingTable, 2, List.of("1:existing", "2:appended"));
    }
  }

  private static void writeRow(Table table, int id, String status) throws Exception {
    BatchWriteBuilder builder = table.newBatchWriteBuilder();
    try (BatchTableWrite writer = builder.newWrite();
        BatchTableCommit commit = builder.newCommit()) {
      writer.write(GenericRow.of(id, BinaryString.fromString(status)));
      commit.commit(writer.prepareCommit());
    }
  }

  private static void checkSnapshot(
      Catalog catalog, Identifier identifier, long expectedSnapshotId, List<String> expectedRows)
      throws Exception {
    long snapshotId = catalog.loadSnapshot(identifier).orElseThrow().snapshot().id();
    if (snapshotId != expectedSnapshotId) {
      throw new IllegalStateException(
          String.format(
              "Expected snapshot %d for %s, got %d", expectedSnapshotId, identifier, snapshotId));
    }

    ReadBuilder read = catalog.getTable(identifier).newReadBuilder();
    List<String> rows = new ArrayList<>();
    try (RecordReader<InternalRow> reader = read.newRead().createReader(read.newScan().plan())) {
      reader.forEachRemaining(row -> rows.add(row.getInt(0) + ":" + row.getString(1)));
    }
    Collections.sort(rows);
    if (!expectedRows.equals(rows)) {
      throw new IllegalStateException(
          String.format("Expected rows %s for %s, got %s", expectedRows, identifier, rows));
    }
  }
}
