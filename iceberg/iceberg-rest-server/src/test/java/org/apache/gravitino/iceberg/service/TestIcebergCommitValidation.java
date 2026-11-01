/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefType;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestIcebergCommitValidation {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final TableIdentifier IDENT = TableIdentifier.of(Namespace.of("db"), "t");

  private InMemoryCatalog catalog;
  private long firstSnapshot;
  private long secondSnapshot;

  @BeforeEach
  public void setUp() {
    catalog = new InMemoryCatalog();
    catalog.initialize("memory", ImmutableMap.of());
    catalog.createNamespace(Namespace.of("db"));
    Table table =
        catalog.createTable(
            IDENT, SCHEMA, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "2"));
    append(table, "/data/f1.parquet");
    firstSnapshot = table.currentSnapshot().snapshotId();
    append(table, "/data/f2.parquet");
    secondSnapshot = table.currentSnapshot().snapshotId();
  }

  @Test
  public void testMainCannotBecomeATag() {
    commitFails(
        ValidationException.class,
        new MetadataUpdate.SetSnapshotRef(
            SnapshotRef.MAIN_BRANCH, secondSnapshot, SnapshotRefType.TAG, null, null, null));
    Assertions.assertTrue(catalog.loadTable(IDENT).refs().get(SnapshotRef.MAIN_BRANCH).isBranch());

    // Other tags, and main as a branch, are unaffected.
    commit(
        new MetadataUpdate.SetSnapshotRef(
            "v1", firstSnapshot, SnapshotRefType.TAG, null, null, null));
    Assertions.assertTrue(catalog.loadTable(IDENT).refs().get("v1").isTag());
  }

  @Test
  public void testAddSnapshotIsChecked() {
    // An operation outside the spec's values.
    commitFails(
        ValidationException.class, addSnapshot(", \"summary\": {\"operation\": \"bogus\"}"));
    // No summary on a v2 table.
    commitFails(ValidationException.class, addSnapshot(""));
    // A schema id the table does not have.
    commitFails(
        ValidationException.class,
        addSnapshot(", \"summary\": {\"operation\": \"append\"}, \"schema-id\": 99"));

    Assertions.assertEquals(
        secondSnapshot, catalog.loadTable(IDENT).currentSnapshot().snapshotId());
    Assertions.assertNull(catalog.loadTable(IDENT).snapshot(999L));

    // A valid snapshot is still accepted.
    commit(addSnapshot(", \"summary\": {\"operation\": \"append\"}, \"schema-id\": 0"));
    Assertions.assertNotNull(catalog.loadTable(IDENT).snapshot(999L));
  }

  @Test
  public void testRemovingReferencedSnapshotIsRefused() {
    // The current snapshot of main.
    commitFails(CommitFailedException.class, new MetadataUpdate.RemoveSnapshots(secondSnapshot));
    // A snapshot a tag references.
    commit(
        new MetadataUpdate.SetSnapshotRef(
            "keep", firstSnapshot, SnapshotRefType.TAG, null, null, null));
    commitFails(CommitFailedException.class, new MetadataUpdate.RemoveSnapshots(firstSnapshot));

    Table table = catalog.loadTable(IDENT);
    Assertions.assertEquals(secondSnapshot, table.currentSnapshot().snapshotId());
    Assertions.assertTrue(table.refs().containsKey("keep"));
    Assertions.assertNotNull(table.snapshot(firstSnapshot));

    // Removing the tag in the same commit makes the removal valid.
    commit(
        new MetadataUpdate.RemoveSnapshotRef("keep"),
        new MetadataUpdate.RemoveSnapshots(ImmutableSet.of(firstSnapshot)));
    Assertions.assertNull(catalog.loadTable(IDENT).snapshot(firstSnapshot));
  }

  @Test
  public void testRefCreatedAfterTheClientReadIsProtected() {
    // An expire job read the table before a user tagged the snapshot it is about to remove: the
    // commit is checked against the metadata it is applied to, so the tag is not lost.
    UpdateTableRequest expire =
        new UpdateTableRequest(
            ImmutableList.of(
                new UpdateRequirement.AssertTableUUID(catalog.loadTable(IDENT).uuid().toString())),
            ImmutableList.of(new MetadataUpdate.RemoveSnapshots(firstSnapshot)));
    catalog.loadTable(IDENT).manageSnapshots().createTag("audit", firstSnapshot).commit();

    Assertions.assertThrows(
        CommitFailedException.class,
        () ->
            CatalogHandlers.updateTable(
                catalog, IDENT, IcebergCommitValidation.withValidation(expire)));
    Assertions.assertTrue(catalog.loadTable(IDENT).refs().containsKey("audit"));
  }

  @Test
  public void testWithValidationKeepsTheRequest() {
    UpdateTableRequest request =
        new UpdateTableRequest(
            ImmutableList.of(new UpdateRequirement.AssertTableUUID("uuid")),
            ImmutableList.of(new MetadataUpdate.RemoveSnapshots(firstSnapshot)));
    UpdateTableRequest validated = IcebergCommitValidation.withValidation(request);

    Assertions.assertEquals(1, request.requirements().size());
    Assertions.assertEquals(2, validated.requirements().size());
    Assertions.assertEquals(request.updates(), validated.updates());
    Assertions.assertInstanceOf(IcebergCommitValidation.class, validated.requirements().get(1));
  }

  private MetadataUpdate addSnapshot(String extraFields) {
    String json =
        "{\"snapshot-id\": 999, \"parent-snapshot-id\": "
            + secondSnapshot
            + ", \"sequence-number\": 3, \"timestamp-ms\": "
            + System.currentTimeMillis()
            + ", \"manifest-list\": \"/data/snap-999.avro\""
            + extraFields
            + "}";
    return new MetadataUpdate.AddSnapshot(SnapshotParser.fromJson(json));
  }

  private void commit(MetadataUpdate... updates) {
    CatalogHandlers.updateTable(catalog, IDENT, request(updates));
  }

  private void commitFails(Class<? extends RuntimeException> expected, MetadataUpdate... updates) {
    Assertions.assertThrows(
        expected, () -> CatalogHandlers.updateTable(catalog, IDENT, request(updates)));
  }

  private static UpdateTableRequest request(MetadataUpdate... updates) {
    List<MetadataUpdate> list = ImmutableList.copyOf(updates);
    return IcebergCommitValidation.withValidation(new UpdateTableRequest(ImmutableList.of(), list));
  }

  private static void append(Table table, String path) {
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath(path)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
        .commit();
  }
}
