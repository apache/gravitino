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

package org.apache.gravitino.spark.connector.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.spark.connector.catalog.GravitinoCatalogManager;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestGravitinoIcebergCatalogSpark41 {

  private static final Identifier TABLE = Identifier.of(new String[] {"db"}, "t");
  // The name Iceberg's ResolveBranch gives a relation that reads branch "audit" of db.t.
  private static final Identifier AUDIT_BRANCH =
      Identifier.of(new String[] {"db", "t"}, "branch_audit");

  private TableCatalog gravitinoTableCatalog;
  private SparkCatalog sparkCatalog;
  private TestCatalog catalog;

  @BeforeAll
  void initCatalogManager() {
    GravitinoCatalogManager.create(
        new SparkConf(false), "user", identity -> mock(GravitinoClient.class));
  }

  @AfterAll
  void closeCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() {
    Catalog gravitinoCatalog = mock(Catalog.class);
    gravitinoTableCatalog = mock(TableCatalog.class);
    when(gravitinoCatalog.asTableCatalog()).thenReturn(gravitinoTableCatalog);
    sparkCatalog = mock(SparkCatalog.class);
    catalog = new TestCatalog(gravitinoCatalog, sparkCatalog);
  }

  @Test
  void testBranchSelectorLoadsTheBranchOfTheTable() throws Exception {
    SparkIcebergTable loaded = loadAuditBranch();

    assertEquals("db.t", loaded.name());
    // ResolveBranch switches the relation to the branch again unless branch() reports it.
    assertEquals("audit", loaded.branch());
    assertEquals(2L, loaded.snapshotId());
    verify(sparkCatalog).loadTable(TABLE);
  }

  @Test
  void testTableLoadedByBranchNameRefusesWrites() throws Exception {
    SparkIcebergTable loaded = loadAuditBranch();

    assertThrows(
        UnsupportedOperationException.class,
        () -> loaded.newWriteBuilder(mock(LogicalWriteInfo.class)));
    assertThrows(
        UnsupportedOperationException.class,
        () -> loaded.newRowLevelOperationBuilder(mock(RowLevelOperationInfo.class)));
    assertThrows(UnsupportedOperationException.class, loaded::truncateTable);
    assertThrows(UnsupportedOperationException.class, () -> loaded.deleteWhere(new Predicate[0]));
  }

  @Test
  void testDeniedBranchSelectorDoesNotLoadTheIcebergTable() {
    when(gravitinoTableCatalog.loadTable(
            NameIdentifier.of("db", "t"), ImmutableSet.of(Privilege.Name.SELECT_TABLE)))
        .thenThrow(new ForbiddenException("denied"));

    assertThrows(ForbiddenException.class, () -> catalog.loadTable(AUDIT_BRANCH));
    verifyNoInteractions(sparkCatalog);
  }

  @Test
  void testOtherMultiLevelNamesAreStillRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> catalog.loadTable(Identifier.of(new String[] {"db", "t"}, "tag_v1")));
    verifyNoInteractions(gravitinoTableCatalog, sparkCatalog);
  }

  private SparkIcebergTable loadAuditBranch() throws Exception {
    Table gravitinoTable = mock(Table.class);
    when(gravitinoTableCatalog.loadTable(
            NameIdentifier.of("db", "t"), ImmutableSet.of(Privilege.Name.SELECT_TABLE)))
        .thenReturn(gravitinoTable);
    Snapshot main = snapshot(1L);
    Snapshot audit = snapshot(2L);
    org.apache.iceberg.Table icebergTable = mock(org.apache.iceberg.Table.class);
    when(icebergTable.schema())
        .thenReturn(new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get())));
    when(icebergTable.currentSnapshot()).thenReturn(main);
    when(icebergTable.snapshot("audit")).thenReturn(audit);
    when(icebergTable.refs())
        .thenReturn(ImmutableMap.of("audit", SnapshotRef.branchBuilder(2L).build()));
    when(icebergTable.properties()).thenReturn(ImmutableMap.of());
    when(gravitinoTable.name()).thenReturn("t");
    SparkTable sparkTable = mock(SparkTable.class);
    when(sparkTable.table()).thenReturn(icebergTable);
    when(sparkCatalog.loadTable(TABLE)).thenReturn(sparkTable);
    return assertInstanceOf(SparkIcebergTable.class, catalog.loadTable(AUDIT_BRANCH));
  }

  private static Snapshot snapshot(long id) {
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(id);
    return snapshot;
  }

  private static class TestCatalog extends GravitinoIcebergCatalogSpark41 {

    private final Catalog gravitinoCatalog;

    private TestCatalog(Catalog gravitinoCatalog, SparkCatalog sparkCatalog) {
      this.gravitinoCatalog = gravitinoCatalog;
      this.sparkCatalog = sparkCatalog;
    }

    @Override
    protected Catalog gravitinoCatalog() {
      return gravitinoCatalog;
    }
  }
}
