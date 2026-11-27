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
package org.apache.gravitino.paimon.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.gravitino.paimon.RESTService;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.responses.GetVersionSnapshotResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * End-to-end test that starts the Paimon REST service on a local port and drives it with a real
 * Paimon {@code RESTCatalog} client: database and table CRUD, data write (which goes through the
 * REST commit endpoint) and read back.
 */
public class PaimonRESTCatalogE2EIT {

  private static final String TABLE = "e2e_table";

  private static RESTService service;
  private static int port;
  private static String warehouse;
  private static Catalog restCatalog;

  @BeforeAll
  static void startServer() throws Exception {
    warehouse = Files.createTempDirectory("paimon-rest-e2e").toUri().toString();
    port = findRandomPort();
    service = new RESTService();
    Map<String, String> config =
        ImmutableMap.of(
            "host",
            "127.0.0.1",
            "httpPort",
            String.valueOf(port),
            "catalog-backend",
            "filesystem",
            "warehouse",
            warehouse,
            // Exercise the table token flow end to end: clients wrap their file IO in
            // RESTTokenFileIO and fetch storage credentials from the token endpoint.
            "data-token.enabled",
            "true",
            // Validate the bearer token shared by the clients below.
            // Per-principal token entry: the client below authenticates as principal "e2e".
            "token.e2e",
            "e2e-token");
    service.serviceInit(config, false);
    service.serviceStart();

    Options clientOptions = new Options();
    clientOptions.set(CatalogOptionsForClient.METASTORE, "rest");
    clientOptions.set(CatalogOptionsForClient.URI, "http://127.0.0.1:" + port + "/paimon/");
    clientOptions.set(CatalogOptionsForClient.WAREHOUSE, "default_catalog");
    clientOptions.set(CatalogOptionsForClient.TOKEN_PROVIDER, "bear");
    clientOptions.set(CatalogOptionsForClient.TOKEN, "e2e-token");
    restCatalog = CatalogFactory.createCatalog(CatalogContext.create(clientOptions));
  }

  @AfterAll
  static void stopServer() throws Exception {
    if (restCatalog != null) {
      ((AutoCloseable) restCatalog).close();
    }
    if (service != null) {
      service.serviceStop();
    }
  }

  @Test
  void testDatabaseCrud() throws Exception {
    String database = "db_" + System.nanoTime();
    restCatalog.createDatabase(database, false, Maps.newHashMap());
    Assertions.assertTrue(restCatalog.listDatabases().contains(database));
    Assertions.assertNotNull(restCatalog.getDatabase(database));

    restCatalog.dropDatabase(database, false, false);
    Assertions.assertFalse(restCatalog.listDatabases().contains(database));
  }

  @Test
  void testTableCrudAndSchemaChange() throws Exception {
    String database = "db_" + System.nanoTime();
    restCatalog.createDatabase(database, false, Maps.newHashMap());
    Identifier tableId = Identifier.create(database, TABLE);
    restCatalog.createTable(tableId, newTableSchema(), false);

    Assertions.assertTrue(restCatalog.listTables(database).contains(TABLE));
    Table table = restCatalog.getTable(tableId);
    Assertions.assertEquals(2, table.rowType().getFieldCount());

    restCatalog.alterTable(
        tableId, Lists.newArrayList(SchemaChange.addColumn("extra", DataTypes.STRING())), false);
    Assertions.assertEquals(3, restCatalog.getTable(tableId).rowType().getFieldCount());

    restCatalog.renameTable(tableId, Identifier.create(database, TABLE + "_renamed"), false);
    Assertions.assertTrue(restCatalog.listTables(database).contains(TABLE + "_renamed"));
    Assertions.assertFalse(restCatalog.listTables(database).contains(TABLE));

    Identifier renamed = Identifier.create(database, TABLE + "_renamed");
    restCatalog.dropTable(renamed, false);
    Assertions.assertFalse(restCatalog.listTables(database).contains(TABLE + "_renamed"));
  }

  @Test
  void testIdentityChangesRejectWholeBatchAndKeepWriterUsable() throws Exception {
    String database = "identity_" + System.nanoTime();
    restCatalog.createDatabase(database, false, Maps.newHashMap());
    Identifier identifier = Identifier.create(database, TABLE);
    restCatalog.createTable(identifier, newTableSchema(), false);
    FileStoreTable writer = (FileStoreTable) restCatalog.getTable(identifier);
    Assertions.assertTrue(restCatalog.loadSnapshot(identifier, "LATEST").isEmpty());
    Assertions.assertTrue(restCatalog.loadSnapshot(identifier, "EARLIEST").isEmpty());
    Assertions.assertTrue(restCatalog.loadSnapshot(identifier, "1").isEmpty());
    for (SchemaChange protectedChange :
        List.of(
            SchemaChange.setOption("rest.uuid", "forged"),
            SchemaChange.removeOption("rest.uuid"))) {
      HttpResponse<String> rejected =
          requestTable(
              database,
              TABLE,
              "",
              "POST",
              RESTApi.OBJECT_MAPPER.writeValueAsString(
                  Map.of(
                      "changes",
                      List.of(
                          SchemaChange.addColumn("must_not_exist", DataTypes.STRING()),
                          protectedChange))));
      Assertions.assertEquals(400, rejected.statusCode(), rejected.body());
      Assertions.assertEquals(2, restCatalog.getTable(identifier).rowType().getFieldCount());
    }
    writeRows(writer, GenericRow.of(1, BinaryString.fromString("still-valid")));
    Assertions.assertEquals(1, readAllRows(writer).size());
    Schema schema = newTableSchema();
    schema.options().put("rest.uuid", "forged");
    HttpResponse<String> rejected =
        requestTable(
            database,
            "",
            "",
            "POST",
            RESTApi.OBJECT_MAPPER.writeValueAsString(
                Map.of("identifier", Identifier.create(database, "forged"), "schema", schema)));
    Assertions.assertEquals(400, rejected.statusCode(), rejected.body());
    Assertions.assertFalse(restCatalog.listTables(database).contains("forged"));
  }

  @Test
  void testWriteAndReadThroughRestCommit() throws Exception {
    String database = "db_" + System.nanoTime();
    restCatalog.createDatabase(database, false, Maps.newHashMap());
    Identifier tableId = Identifier.create(database, TABLE);
    restCatalog.createTable(tableId, newTableSchema(), false);

    FileStoreTable table = (FileStoreTable) restCatalog.getTable(tableId);
    String commitUser = UUID.randomUUID().toString();
    TableWriteImpl<?> writer = table.newWrite(commitUser);
    writer.write(GenericRow.of(1, BinaryString.fromString("alice")));
    writer.write(GenericRow.of(2, BinaryString.fromString("bob")));
    List<CommitMessage> messages = writer.prepareCommit();
    writer.close();

    // This commit goes through the REST catalog's commit endpoint, which atomically persists
    // the snapshot server side.
    TableCommitImpl commit = table.newCommit(commitUser);
    commit.commit(messages);
    commit.close();

    Assertions.assertNotNull(table.snapshotManager().latestSnapshotId());
    TableSnapshot snapshot = restCatalog.loadSnapshot(tableId).orElseThrow();
    Assertions.assertEquals(2, snapshot.recordCount());
    Assertions.assertEquals(1, snapshot.fileCount());
    Assertions.assertTrue(snapshot.fileSizeInBytes() > 0);
    Assertions.assertTrue(snapshot.lastFileCreationTime() > 0);

    // Read back through the client table.
    List<InternalRow> rows = readAllRows(table);
    Assertions.assertEquals(2, rows.size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"regular", "daily report", "daily+report", "literal%2B"})
  void testBranchesTagsAndRollback(String name) throws Exception {
    String database = name + "_db_" + System.nanoTime();
    restCatalog.createDatabase(database, false, Maps.newHashMap());
    Identifier tableId = Identifier.create(database, name);
    restCatalog.createTable(tableId, newTableSchema(), false);

    FileStoreTable table = (FileStoreTable) restCatalog.getTable(tableId);
    long firstSnapshotId = writeRows(table, GenericRow.of(1, BinaryString.fromString("alice")));
    Assertions.assertEquals(1, readAllRows(table).size());

    // Tags
    restCatalog.createTag(tableId, name + "_tag", firstSnapshotId, null, false);
    Assertions.assertTrue(
        restCatalog.listTagsPaged(tableId, null, null, null).getElements().contains(name + "_tag"));
    Assertions.assertNotNull(restCatalog.getTag(tableId, name + "_tag"));
    HttpResponse<String> tagged =
        requestTable(
            database, name, "/snapshots/" + RESTUtil.encodeString(name + "_tag"), "GET", "");
    Assertions.assertEquals(200, tagged.statusCode(), tagged.body());
    Assertions.assertEquals(
        firstSnapshotId,
        RESTApi.OBJECT_MAPPER
            .readValue(tagged.body(), GetVersionSnapshotResponse.class)
            .getSnapshot()
            .id());
    Assertions.assertEquals(
        404,
        requestTable(
                database, name, "/snapshots/" + RESTUtil.encodeString(name + "_missing"), "GET", "")
            .statusCode());

    // Second write creates a second snapshot.
    writeRows(
        table,
        GenericRow.of(2, BinaryString.fromString("bob")),
        GenericRow.of(3, BinaryString.fromString("carol")));
    Assertions.assertEquals(3, readAllRows(table).size());

    // Branches
    restCatalog.createBranch(tableId, name + "_branch", name + "_tag", false);
    Assertions.assertTrue(restCatalog.listBranches(tableId).contains(name + "_branch"));
    Identifier branchId = new Identifier(database, name, name + "_branch");
    FileStoreTable branchTable = (FileStoreTable) restCatalog.getTable(branchId);
    writeRows(branchTable, GenericRow.of(4, BinaryString.fromString("branch-only")));
    Assertions.assertEquals(2, readAllRows(branchTable).size());
    Assertions.assertEquals(3, readAllRows(table).size());
    Assertions.assertTrue(restCatalog.loadSnapshot(branchId).isPresent());
    HttpResponse<String> renamed =
        HttpClient.newHttpClient()
            .send(
                HttpRequest.newBuilder(
                        URI.create(
                            "http://127.0.0.1:"
                                + port
                                + "/paimon/v1/default_catalog/databases/"
                                + RESTUtil.encodeString(database)
                                + "/tables/"
                                + RESTUtil.encodeString(name)
                                + "/branches/"
                                + RESTUtil.encodeString(name + "_branch")
                                + "/rename"))
                    .header("Authorization", "Bearer e2e-token")
                    .header("Content-Type", "application/json")
                    .POST(
                        HttpRequest.BodyPublishers.ofString(
                            "{\"toBranch\":\"" + name + "_renamed\"}"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
    Assertions.assertEquals(200, renamed.statusCode(), renamed.body());
    Assertions.assertTrue(restCatalog.listBranches(tableId).contains(name + "_renamed"));
    restCatalog.dropBranch(tableId, name + "_renamed");
    Assertions.assertFalse(restCatalog.listBranches(tableId).contains(name + "_renamed"));

    // A caller that has not seen the second write cannot undo it.
    Assertions.assertThrows(
        BadRequestException.class,
        () -> restCatalog.rollbackTo(tableId, Instant.snapshot(firstSnapshotId), firstSnapshotId));
    Assertions.assertThrows(
        BadRequestException.class,
        () -> restCatalog.rollbackTo(tableId, Instant.tag(name + "_tag"), firstSnapshotId));
    Assertions.assertEquals(3, readAllRows(table).size());

    long latest = restCatalog.loadSnapshot(tableId).orElseThrow().snapshot().id();
    restCatalog.rollbackTo(tableId, Instant.tag(name + "_tag"), latest);
    Assertions.assertEquals(1, readAllRows(table).size());

    restCatalog.deleteTag(tableId, name + "_tag");
    restCatalog.dropTable(tableId, false);
    restCatalog.dropDatabase(database, false, false);
  }

  @Test
  void testCommitRejectsStaleSnapshotUuidAfterRollback() throws Exception {
    String database = "snapshot_uuid";
    restCatalog.createDatabase(database, false);
    Identifier identifier = Identifier.create(database, TABLE);
    restCatalog.createTable(identifier, newTableSchema(), false);

    FileStoreTable table = (FileStoreTable) restCatalog.getTable(identifier);
    long firstId = writeRows(table, GenericRow.of(1, BinaryString.fromString("first")));
    long secondId = writeRows(table, GenericRow.of(2, BinaryString.fromString("discarded")));
    Snapshot oldHead = restCatalog.loadSnapshot(identifier).orElseThrow().snapshot();
    Assertions.assertNotNull(oldHead.uuid());

    restCatalog.rollbackTo(identifier, Instant.snapshot(firstId), secondId);
    Assertions.assertEquals(
        secondId, writeRows(table, GenericRow.of(3, BinaryString.fromString("replacement"))));
    Snapshot head = restCatalog.loadSnapshot(identifier).orElseThrow().snapshot();
    Assertions.assertNotNull(head.uuid());
    Assertions.assertNotEquals(oldHead.uuid(), head.uuid());

    ObjectNode nextJson = (ObjectNode) RESTApi.OBJECT_MAPPER.readTree(head.toJson());
    nextJson.put("id", secondId + 1);
    nextJson.put("uuid", UUID.randomUUID().toString());
    nextJson.put("operation", "WRITE");
    nextJson.put("writerVersion", "2.1.0");
    Snapshot next = Snapshot.fromJson(nextJson.toString());
    String tableUuid = table.catalogEnvironment().uuid();
    Assertions.assertFalse(
        restCatalog.commitSnapshot(identifier, tableUuid, oldHead.uuid(), next, List.of()));
    Assertions.assertEquals(
        head.uuid(), restCatalog.loadSnapshot(identifier).orElseThrow().snapshot().uuid());

    Assertions.assertTrue(
        restCatalog.commitSnapshot(identifier, tableUuid, head.uuid(), next, List.of()));
    Snapshot stored = restCatalog.loadSnapshot(identifier).orElseThrow().snapshot();
    Assertions.assertEquals(next.uuid(), stored.uuid());
    Assertions.assertEquals(next.operation(), stored.operation());
    Assertions.assertEquals(next.writerVersion(), stored.writerVersion());
    Assertions.assertEquals(next.uuid(), table.snapshotManager().snapshot(next.id()).uuid());
  }

  private HttpResponse<String> requestTable(
      String database, String table, String suffix, String method, String body) throws Exception {
    return HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(
                    URI.create(
                        "http://127.0.0.1:"
                            + port
                            + "/paimon/v1/default_catalog/databases/"
                            + RESTUtil.encodeString(database)
                            + "/tables"
                            + (table.isEmpty() ? "" : "/" + RESTUtil.encodeString(table))
                            + suffix))
                .header("Authorization", "Bearer e2e-token")
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body))
                .build(),
            HttpResponse.BodyHandlers.ofString());
  }

  /** Writes the given rows and commits them; returns the committed snapshot id. */
  private long writeRows(FileStoreTable table, GenericRow... rows) throws Exception {
    String commitUser = UUID.randomUUID().toString();
    TableWriteImpl<?> writer = table.newWrite(commitUser);
    for (GenericRow row : rows) {
      writer.write(row);
    }
    List<CommitMessage> messages = writer.prepareCommit();
    writer.close();
    TableCommitImpl commit = table.newCommit(commitUser);
    commit.commit(messages);
    commit.close();
    return table.snapshotManager().latestSnapshotId();
  }

  private static List<InternalRow> readAllRows(Table table) throws Exception {
    ReadBuilder readBuilder = table.newReadBuilder();
    TableScan.Plan plan = readBuilder.newScan().plan();
    List<InternalRow> rows = new ArrayList<>();
    try (RecordReader<InternalRow> reader = readBuilder.newRead().createReader(plan)) {
      reader.forEachRemaining(rows::add);
    }
    return rows;
  }

  private static Schema newTableSchema() {
    List<DataField> columns =
        Lists.newArrayList(
            new DataField(0, "id", DataTypes.INT(), "id column"),
            new DataField(1, "name", DataTypes.STRING(), "name column"));
    return new Schema(columns, Lists.newArrayList(), Lists.newArrayList(), Maps.newHashMap(), null);
  }

  private static int findRandomPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  /** Client-side option keys, kept separate from server-side keys for readability. */
  private static final class CatalogOptionsForClient {
    private static final String METASTORE = "metastore";
    private static final String URI = "uri";
    private static final String WAREHOUSE = "warehouse";
    private static final String TOKEN_PROVIDER = "token.provider";
    private static final String TOKEN = "token";
  }
}
