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
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.MetadataUpdateParser;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefType;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateTableRequest;
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
  public void testAddingSchemaDoesNotBypassSnapshotSchemaValidation() {
    Schema schema =
        new Schema(
            99,
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));
    commitFails(
        ValidationException.class,
        new MetadataUpdate.AddSchema(schema),
        addSnapshot(", \"summary\": {\"operation\": \"append\"}, \"schema-id\": 99"));
    Assertions.assertEquals(1, catalog.loadTable(IDENT).schemas().size());
    Assertions.assertNull(catalog.loadTable(IDENT).snapshot(999L));

    // Iceberg assigns id 1 to the new schema, regardless of the id supplied in add-schema.
    commit(
        new MetadataUpdate.AddSchema(schema),
        addSnapshot(", \"summary\": {\"operation\": \"append\"}, \"schema-id\": 1"));
    Assertions.assertNotNull(catalog.loadTable(IDENT).schemas().get(1));
    Assertions.assertEquals(1, catalog.loadTable(IDENT).snapshot(999L).schemaId());
  }

  @Test
  public void testStaleSnapshotRemainsACommitConflict() {
    MetadataUpdate snapshot =
        new MetadataUpdate.AddSnapshot(
            SnapshotParser.fromJson(
                "{\"snapshot-id\": 999, \"parent-snapshot-id\": "
                    + secondSnapshot
                    + ", \"sequence-number\": 1, \"timestamp-ms\": "
                    + System.currentTimeMillis()
                    + ", \"manifest-list\": \"/data/snap-999.avro\","
                    + " \"summary\": {\"operation\": \"append\"}, \"schema-id\": 0}"));
    commitFails(CommitFailedException.class, snapshot);
    Assertions.assertEquals(
        secondSnapshot, catalog.loadTable(IDENT).currentSnapshot().snapshotId());
    Assertions.assertNull(catalog.loadTable(IDENT).snapshot(999L));
  }

  @Test
  public void testChangingRefMustActuallyMoveItOffRemovedSnapshot() {
    commit(
        new MetadataUpdate.SetSnapshotRef(
            "keep", firstSnapshot, SnapshotRefType.TAG, null, null, null));
    commitFails(
        CommitFailedException.class,
        new MetadataUpdate.SetSnapshotRef(
            "keep", firstSnapshot, SnapshotRefType.TAG, null, null, null),
        new MetadataUpdate.RemoveSnapshots(firstSnapshot));
    Assertions.assertNotNull(catalog.loadTable(IDENT).snapshot(firstSnapshot));
    Assertions.assertEquals(
        firstSnapshot, catalog.loadTable(IDENT).refs().get("keep").snapshotId());

    commit(
        new MetadataUpdate.SetSnapshotRef(
            "keep", secondSnapshot, SnapshotRefType.TAG, null, null, null),
        new MetadataUpdate.RemoveSnapshots(firstSnapshot));
    Assertions.assertNull(catalog.loadTable(IDENT).snapshot(firstSnapshot));
    Assertions.assertEquals(
        secondSnapshot, catalog.loadTable(IDENT).refs().get("keep").snapshotId());
  }

  @Test
  public void testStagedCreateCommitIsAccepted() {
    TableIdentifier staged = TableIdentifier.of("db", "staged");
    UpdateTableRequest request = stagedCreateRequest(staged);
    Assertions.assertFalse(catalog.tableExists(staged));

    CatalogHandlers.updateTable(catalog, staged, IcebergCommitValidation.withValidation(request));
    Assertions.assertTrue(catalog.tableExists(staged));
    Assertions.assertTrue(SCHEMA.sameSchema(catalog.loadTable(staged).schema()));

    // Keep the original existence requirement: retrying the create cannot replace the table.
    Assertions.assertThrows(
        CommitFailedException.class,
        () ->
            CatalogHandlers.updateTable(
                catalog, staged, IcebergCommitValidation.withValidation(request)));
  }

  @Test
  public void testStagedCreateIsValidatedBeforeCommit() {
    TableIdentifier staged = TableIdentifier.of("db", "invalid_staged");
    UpdateTableRequest create = stagedCreateRequest(staged);
    List<MetadataUpdate> updates =
        ImmutableList.<MetadataUpdate>builder()
            .addAll(create.updates())
            .add(
                new MetadataUpdate.SetSnapshotRef(
                    SnapshotRef.MAIN_BRANCH, 999L, SnapshotRefType.TAG, null, null, null))
            .build();
    UpdateTableRequest invalid = new UpdateTableRequest(create.requirements(), updates);
    Assertions.assertThrows(
        ValidationException.class,
        () ->
            CatalogHandlers.updateTable(
                catalog, staged, IcebergCommitValidation.withValidation(invalid)));
    Assertions.assertFalse(catalog.tableExists(staged));
  }

  @Test
  public void testStagedCreateRejectsSnapshotWithoutSummary() {
    TableIdentifier staged = TableIdentifier.of("db", "invalid_snapshot");
    UpdateTableRequest create = stagedCreateRequest(staged);
    MetadataUpdate snapshot =
        new MetadataUpdate.AddSnapshot(
            SnapshotParser.fromJson(
                "{\"snapshot-id\": 999, \"sequence-number\": 1, \"timestamp-ms\": "
                    + System.currentTimeMillis()
                    + ", \"manifest-list\": \"/data/snap-999.avro\", \"schema-id\": 0}"));
    List<MetadataUpdate> updates =
        ImmutableList.<MetadataUpdate>builder().addAll(create.updates()).add(snapshot).build();
    UpdateTableRequest invalid = new UpdateTableRequest(create.requirements(), updates);

    ValidationException thrown =
        Assertions.assertThrows(
            ValidationException.class,
            () ->
                CatalogHandlers.updateTable(
                    catalog, staged, IcebergCommitValidation.withValidation(invalid)));
    Assertions.assertTrue(thrown.getMessage().contains("summary"));
    Assertions.assertFalse(catalog.tableExists(staged));
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

  @Test
  public void testSchemaChangesMustKeepDataReadable() {
    // id 1 is "id long" and the table has data.
    commitFails(
        ValidationException.class,
        addSchema("{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"int\"}"));
    commitFails(
        ValidationException.class,
        addSchema("{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"string\"}"));
    commitFails(
        ValidationException.class,
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":{\"type\":\"struct\","
                + "\"fields\":[{\"id\":9,\"name\":\"z\",\"required\":false,\"type\":\"long\"}]}}"));
    // A new required field without a default.
    commitFails(
        ValidationException.class,
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"},"
                + "{\"id\":2,\"name\":\"r\",\"required\":true,\"type\":\"string\"}"));
    Assertions.assertEquals(1, catalog.loadTable(IDENT).schemas().size());

    // Allowed: an optional new field with a new id.
    commit(
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"},"
                + "{\"id\":2,\"name\":\"s\",\"required\":false,\"type\":\"string\"}"));
    Assertions.assertEquals(2, catalog.loadTable(IDENT).schemas().size());
  }

  @Test
  public void testOptionalFieldCannotBecomeRequired() {
    // Add optional s (id 2) and make it current; the table already has data without it.
    commit(
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"},"
                + "{\"id\":2,\"name\":\"s\",\"required\":false,\"type\":\"string\"}"),
        new MetadataUpdate.SetCurrentSchema(-1));

    commitFails(
        ValidationException.class,
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"},"
                + "{\"id\":2,\"name\":\"s\",\"required\":true,\"type\":\"string\"}"),
        new MetadataUpdate.SetCurrentSchema(-1));
    Assertions.assertTrue(catalog.loadTable(IDENT).schema().findField(2).isOptional());

    // Allowed: required to optional.
    commit(
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"},"
                + "{\"id\":2,\"name\":\"s\",\"required\":false,\"type\":\"string\"}"),
        new MetadataUpdate.SetCurrentSchema(-1));
    Assertions.assertTrue(catalog.loadTable(IDENT).schema().findField(1).isOptional());
  }

  @Test
  public void testDecimalChangesFollowPromotionRules() {
    commit(
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"},"
                + "{\"id\":2,\"name\":\"d\",\"required\":false,\"type\":\"decimal(10, 2)\"}"),
        new MetadataUpdate.SetCurrentSchema(-1));

    for (String type : new String[] {"decimal(10, 3)", "decimal(9, 2)"}) {
      commitFails(
          ValidationException.class,
          addSchema(
              "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"},"
                  + "{\"id\":2,\"name\":\"d\",\"required\":false,\"type\":\""
                  + type
                  + "\"}"),
          new MetadataUpdate.SetCurrentSchema(-1));
    }

    // Allowed: widening precision at the same scale.
    commit(
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"},"
                + "{\"id\":2,\"name\":\"d\",\"required\":false,\"type\":\"decimal(12, 2)\"}"),
        new MetadataUpdate.SetCurrentSchema(-1));
    Assertions.assertEquals(
        Types.DecimalType.of(12, 2), catalog.loadTable(IDENT).schema().findType("d"));
  }

  @Test
  public void testDatePromotionsAreAllowedOnlyFromV3() {
    // The v3 table spec allows date to timestamp and timestamp_ns; v2 does not.
    datePromotion("3", "timestamp", true);
    datePromotion("3", "timestamp_ns", true);
    datePromotion("2", "timestamp", false);
    // A promotion to a type with a time zone is never allowed.
    datePromotion("3", "timestamptz", false);
  }

  private void datePromotion(String version, String type, boolean allowed) {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("db"), "d_v" + version + "_" + type);
    append(
        catalog.createTable(
            ident,
            new Schema(Types.NestedField.optional(1, "d", Types.DateType.get())),
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", version)),
        "/data/" + ident.name() + ".parquet");
    UpdateTableRequest request =
        request(
            addSchema("{\"id\":1,\"name\":\"d\",\"required\":false,\"type\":\"" + type + "\"}"));
    if (allowed) {
      CatalogHandlers.updateTable(catalog, ident, request);
      Assertions.assertEquals(2, catalog.loadTable(ident).schemas().size(), type);
    } else {
      Assertions.assertThrows(
          ValidationException.class,
          () -> CatalogHandlers.updateTable(catalog, ident, request),
          version + " " + type);
    }
  }

  @Test
  public void testReusingADroppedFieldIdIsRefused() {
    // Schema 1 adds b (id 2); schema 2 drops it; a later schema may not give id 2 to another field.
    commit(
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"},"
                + "{\"id\":2,\"name\":\"b\",\"required\":false,\"type\":\"int\"}"),
        new MetadataUpdate.SetCurrentSchema(-1));
    // Going back to the original schema drops b again.
    commit(new MetadataUpdate.SetCurrentSchema(0));
    commitFails(
        ValidationException.class,
        addSchema(
            "{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"},"
                + "{\"id\":2,\"name\":\"c\",\"required\":false,\"type\":\"string\"}"));
  }

  @Test
  public void testReplaceMayRedefineColumns() {
    // A replace removes main, so no existing data is read with the new schema.
    commit(
        new MetadataUpdate.RemoveSnapshotRef(SnapshotRef.MAIN_BRANCH),
        addSchema("{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"string\"}"),
        new MetadataUpdate.SetCurrentSchema(-1));
    Assertions.assertEquals(
        Types.StringType.get(), catalog.loadTable(IDENT).schema().findType("id"));
  }

  @Test
  public void testPartitionFieldIdsAreChecked() {
    Table partitioned = createPartitionedTable("p");
    TableIdentifier ident = TableIdentifier.of(Namespace.of("db"), "p");
    // Spec 0 has 1000: s identity(2). The same id for another source field is refused.
    Assertions.assertThrows(
        ValidationException.class,
        () ->
            CatalogHandlers.updateTable(
                catalog,
                ident,
                request(
                    addSpec(
                        "{\"name\":\"id\",\"transform\":\"identity\",\"source-id\":1,\"field-id\":1000}"))));
    // A new field with an id below last-partition-id is refused.
    Assertions.assertThrows(
        ValidationException.class,
        () ->
            CatalogHandlers.updateTable(
                catalog,
                ident,
                request(
                    addSpec(
                        "{\"name\":\"id\",\"transform\":\"identity\",\"source-id\":1,\"field-id\":999}"))));
    // Keeping 1000 for the same field and adding 1001 is valid.
    CatalogHandlers.updateTable(
        catalog,
        ident,
        request(
            addSpec(
                "{\"name\":\"s\",\"transform\":\"identity\",\"source-id\":2,\"field-id\":1000},"
                    + "{\"name\":\"id\",\"transform\":\"identity\",\"source-id\":1,\"field-id\":1001}")));
    Assertions.assertEquals(2, catalog.loadTable(ident).specs().size());
    Assertions.assertNotNull(partitioned);
  }

  @Test
  public void testAddSpecWithoutFieldIdsGetsTheTablesIds() {
    createPartitionedTable("noid");
    TableIdentifier ident = TableIdentifier.of(Namespace.of("db"), "noid");
    // Spec 0 has 1000: s identity(2). Neither field below carries a field-id.
    UpdateTableRequest request =
        IcebergCommitValidation.assignPartitionFieldIds(
            request(
                addSpec(
                    "{\"name\":\"s\",\"transform\":\"identity\",\"source-id\":2},"
                        + "{\"name\":\"id_b\",\"transform\":\"bucket[4]\",\"source-id\":1}"),
                new MetadataUpdate.SetDefaultPartitionSpec(-1)),
            () -> ((HasTableOperations) catalog.loadTable(ident)).operations().current());

    CatalogHandlers.updateTable(catalog, ident, IcebergCommitValidation.withValidation(request));

    PartitionSpec spec = catalog.loadTable(ident).spec();
    // The field already partitioned on keeps its id; the new field gets the next one.
    Assertions.assertEquals(1000, spec.fields().get(0).fieldId());
    Assertions.assertEquals(1001, spec.fields().get(1).fieldId());
  }

  @Test
  public void testAddSpecWithFieldIdsIsUnchanged() {
    UpdateTableRequest request =
        request(
            addSpec(
                "{\"name\":\"id\",\"transform\":\"identity\",\"source-id\":1,\"field-id\":1000}"));
    Assertions.assertSame(
        request,
        IcebergCommitValidation.assignPartitionFieldIds(
            request,
            () -> {
              throw new AssertionError("metadata must not be loaded");
            }));
  }

  @Test
  public void testRemovingASpecWithLiveManifestsIsRefused() {
    Table partitioned = createPartitionedTable("rm");
    TableIdentifier ident = TableIdentifier.of(Namespace.of("db"), "rm");
    append(partitioned, "/data/rm1.parquet", "s=a");
    // Make a new default spec, so spec 0 is no longer the default but is still used by data.
    CatalogHandlers.updateTable(
        catalog,
        ident,
        request(
            addSpec(
                "{\"name\":\"id\",\"transform\":\"identity\",\"source-id\":1,\"field-id\":1001}"),
            new MetadataUpdate.SetDefaultPartitionSpec(-1)));
    Assertions.assertThrows(
        ValidationException.class,
        () ->
            CatalogHandlers.updateTable(
                catalog,
                ident,
                IcebergCommitValidation.withValidation(
                    new UpdateTableRequest(
                        ImmutableList.of(),
                        ImmutableList.of(
                            new MetadataUpdate.RemovePartitionSpecs(ImmutableSet.of(0)))),
                    () -> catalog.loadTable(ident).io())));
    Assertions.assertNotNull(catalog.loadTable(ident).specs().get(0));

    // An unused spec can be removed.
    CatalogHandlers.updateTable(
        catalog,
        ident,
        request(
            addSpec(
                "{\"name\":\"s_b\",\"transform\":\"bucket[4]\",\"source-id\":2,\"field-id\":1002}")));
    CatalogHandlers.updateTable(
        catalog,
        ident,
        IcebergCommitValidation.withValidation(
            new UpdateTableRequest(
                ImmutableList.of(),
                ImmutableList.of(new MetadataUpdate.RemovePartitionSpecs(ImmutableSet.of(2)))),
            () -> catalog.loadTable(ident).io()));
    Assertions.assertNull(catalog.loadTable(ident).specs().get(2));
  }

  @Test
  public void testUnknownTransformCannotBecomeTheDefault() {
    MetadataUpdate zorder =
        MetadataUpdateParser.fromJson(
            "{\"action\":\"add-sort-order\",\"sort-order\":{\"order-id\":1,\"fields\":["
                + "{\"transform\":\"zorder\",\"source-id\":1,\"direction\":\"asc\","
                + "\"null-order\":\"nulls-first\"}]}}");
    commitFails(ValidationException.class, zorder, new MetadataUpdate.SetDefaultSortOrder(-1));
    Assertions.assertTrue(catalog.loadTable(IDENT).sortOrder().isUnsorted());
    // Adding it without making it the default is allowed by the table spec.
    commit(zorder);
  }

  @Test
  public void testPartitionSpecWithoutSpecIdIsAccepted() throws Exception {
    // The REST spec makes spec-id read-only; the server assigns the id.
    CreateTableRequest create =
        IcebergObjectMapper.getInstance()
            .readValue(
                "{\"name\":\"t\",\"schema\":"
                    + SchemaParser.toJson(SCHEMA)
                    + ",\"partition-spec\":{\"fields\":[{\"name\":\"id_b\","
                    + "\"transform\":\"bucket[4]\",\"source-id\":1}]}}",
                CreateTableRequest.class);
    Assertions.assertEquals(1, create.spec().fields().size());

    UpdateTableRequest update =
        IcebergObjectMapper.getInstance()
            .readValue(
                "{\"requirements\":[],\"updates\":[{\"action\":\"add-spec\",\"spec\":"
                    + "{\"fields\":[{\"name\":\"id_b\",\"transform\":\"bucket[4]\","
                    + "\"source-id\":1,\"field-id\":1000}]}}]}",
                UpdateTableRequest.class);
    CatalogHandlers.updateTable(catalog, IDENT, IcebergCommitValidation.withValidation(update));
    // The new spec gets the next free id, not the placeholder.
    Assertions.assertEquals(2, catalog.loadTable(IDENT).specs().size());
    Assertions.assertTrue(catalog.loadTable(IDENT).specs().containsKey(1));
  }

  @Test
  void testPartitionIdsCannotCollideWithinOneCommit() {
    commitFails(
        ValidationException.class,
        addSpec("{\"name\":\"id\",\"transform\":\"identity\",\"source-id\":1,\"field-id\":1000}"),
        addSpec(
            "{\"name\":\"id_bucket\",\"transform\":\"bucket[4]\",\"source-id\":1,\"field-id\":1000}"));
    Assertions.assertEquals(1, catalog.loadTable(IDENT).specs().size());
  }

  @Test
  void testUnusedUnknownSortOrderDoesNotBlockSettingKnownDefault() {
    MetadataUpdate unknown =
        MetadataUpdateParser.fromJson(
            "{\"action\":\"add-sort-order\",\"sort-order\":{\"order-id\":1,\"fields\":["
                + "{\"transform\":\"zorder\",\"source-id\":1,\"direction\":\"asc\","
                + "\"null-order\":\"nulls-first\"}]}}");
    commit(unknown, new MetadataUpdate.SetDefaultSortOrder(0));
    Assertions.assertTrue(catalog.loadTable(IDENT).sortOrder().isUnsorted());
    Assertions.assertEquals(2, catalog.loadTable(IDENT).sortOrders().size());
  }

  @Test
  void testStagedCreateCannotUseUnknownDefaultTransform() {
    TableIdentifier staged = TableIdentifier.of("db", "unknown_transform");
    UpdateTableRequest create = stagedCreateRequest(staged);
    List<MetadataUpdate> updates =
        ImmutableList.<MetadataUpdate>builder()
            .addAll(create.updates())
            .add(
                addSpec(
                    "{\"name\":\"unknown\",\"transform\":\"custom\",\"source-id\":1,\"field-id\":1000}"))
            .add(new MetadataUpdate.SetDefaultPartitionSpec(-1))
            .build();
    UpdateTableRequest request = new UpdateTableRequest(create.requirements(), updates);

    Assertions.assertThrows(
        ValidationException.class,
        () ->
            CatalogHandlers.updateTable(
                catalog, staged, IcebergCommitValidation.withValidation(request)));
    Assertions.assertFalse(catalog.tableExists(staged));
  }

  @Test
  void testRemovingAndRestoringMainCannotBypassSchemaChecks() {
    commitFails(
        ValidationException.class,
        new MetadataUpdate.RemoveSnapshotRef(SnapshotRef.MAIN_BRANCH),
        addSchema("{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"string\"}"),
        new MetadataUpdate.SetCurrentSchema(-1),
        new MetadataUpdate.SetSnapshotRef(
            SnapshotRef.MAIN_BRANCH, secondSnapshot, SnapshotRefType.BRANCH, null, null, null));
    Assertions.assertEquals(Types.LongType.get(), catalog.loadTable(IDENT).schema().findType("id"));
    Assertions.assertEquals(
        secondSnapshot, catalog.loadTable(IDENT).currentSnapshot().snapshotId());
  }

  private Table createPartitionedTable(String name) {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "s", Types.StringType.get()));
    return catalog.createTable(
        TableIdentifier.of(Namespace.of("db"), name),
        schema,
        PartitionSpec.builderFor(schema).identity("s").build(),
        ImmutableMap.of("format-version", "2"));
  }

  private static MetadataUpdate addSchema(String fields) {
    return new MetadataUpdate.AddSchema(
        SchemaParser.fromJson("{\"type\":\"struct\",\"schema-id\":7,\"fields\":[" + fields + "]}"));
  }

  private static MetadataUpdate addSpec(String fields) {
    return MetadataUpdateParser.fromJson(
        "{\"action\":\"add-spec\",\"spec\":{\"spec-id\":0,\"fields\":[" + fields + "]}}");
  }

  private static void append(Table table, String path, String partition) {
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(path)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withPartitionPath(partition)
                .build())
        .commit();
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

  private UpdateTableRequest stagedCreateRequest(TableIdentifier identifier) {
    TableMetadata metadata =
        CatalogHandlers.stageTableCreate(
                catalog,
                identifier.namespace(),
                CreateTableRequest.builder()
                    .withName(identifier.name())
                    .withSchema(SCHEMA)
                    .setProperties(ImmutableMap.of("format-version", "2"))
                    .stageCreate()
                    .build())
            .tableMetadata();
    return new UpdateTableRequest(
        ImmutableList.of(new UpdateRequirement.AssertTableDoesNotExist()), metadata.changes());
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
