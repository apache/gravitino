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
package org.apache.gravitino.flink.connector.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedCatalogView;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.DefaultCatalogCompat;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestBaseCatalog {

  @Test
  public void testHiveSchemaChanges() {
    Map<String, String> currentProperties = ImmutableMap.of("key", "value", "key2", "value2");
    CatalogDatabase current = new CatalogDatabaseImpl(currentProperties, null);

    Map<String, String> newProperties = ImmutableMap.of("key2", "new-value2", "key3", "value3");
    CatalogDatabase updated = new CatalogDatabaseImpl(newProperties, null);

    SchemaChange[] schemaChange = BaseCatalog.getSchemaChange(current, updated);
    Assertions.assertEquals(3, schemaChange.length);
    Assertions.assertInstanceOf(SchemaChange.RemoveProperty.class, schemaChange[0]);
    Assertions.assertEquals("key", ((SchemaChange.RemoveProperty) schemaChange[0]).getProperty());

    Assertions.assertInstanceOf(SchemaChange.SetProperty.class, schemaChange[1]);
    Assertions.assertEquals("key3", ((SchemaChange.SetProperty) schemaChange[1]).getProperty());
    Assertions.assertEquals("value3", ((SchemaChange.SetProperty) schemaChange[1]).getValue());

    Assertions.assertInstanceOf(SchemaChange.SetProperty.class, schemaChange[2]);
    Assertions.assertEquals("key2", ((SchemaChange.SetProperty) schemaChange[2]).getProperty());
    Assertions.assertEquals("new-value2", ((SchemaChange.SetProperty) schemaChange[2]).getValue());
  }

  @Test
  public void testTableChanges() {
    List<TableChange> tableChanges =
        ImmutableList.of(
            TableChange.add(Column.physical("test", DataTypes.INT())),
            TableChange.modifyPhysicalColumnType(
                Column.physical("test", DataTypes.INT()), DataTypes.DOUBLE()),
            TableChange.modifyColumnName(Column.physical("test", DataTypes.INT()), "test2"),
            TableChange.dropColumn("aaa"),
            TableChange.modifyColumnComment(
                Column.physical("test", DataTypes.INT()), "new comment"),
            TableChange.modifyColumnPosition(
                Column.physical("test", DataTypes.INT()),
                TableChange.ColumnPosition.after("test2")),
            TableChange.modifyColumnPosition(
                Column.physical("test", DataTypes.INT()), TableChange.ColumnPosition.first()),
            TableChange.set("key", "value"),
            TableChange.reset("key"));

    List<org.apache.gravitino.rel.TableChange> expected =
        ImmutableList.of(
            org.apache.gravitino.rel.TableChange.addColumn(
                new String[] {"test"}, Types.IntegerType.get()),
            org.apache.gravitino.rel.TableChange.updateColumnType(
                new String[] {"test"}, Types.DoubleType.get()),
            org.apache.gravitino.rel.TableChange.renameColumn(new String[] {"test"}, "test2"),
            org.apache.gravitino.rel.TableChange.deleteColumn(new String[] {"aaa"}, true),
            org.apache.gravitino.rel.TableChange.updateColumnComment(
                new String[] {"test"}, "new comment"),
            org.apache.gravitino.rel.TableChange.updateColumnPosition(
                new String[] {"test"},
                org.apache.gravitino.rel.TableChange.ColumnPosition.after("test2")),
            org.apache.gravitino.rel.TableChange.updateColumnPosition(
                new String[] {"test"}, org.apache.gravitino.rel.TableChange.ColumnPosition.first()),
            org.apache.gravitino.rel.TableChange.setProperty("key", "value"),
            org.apache.gravitino.rel.TableChange.removeProperty("key"));

    org.apache.gravitino.rel.TableChange[] gravitinoTableChanges =
        BaseCatalog.getGravitinoTableChanges(tableChanges);
    Assertions.assertArrayEquals(expected.toArray(), gravitinoTableChanges);
  }

  @Test
  public void testTableChangesWithoutColumnChange() {
    Schema schema = Schema.newBuilder().column("test", "INT").build();
    CatalogBaseTable table =
        DefaultCatalogCompat.INSTANCE.createCatalogTable(
            schema, "test", ImmutableList.of(), ImmutableMap.of("key", "value", "key2", "value2"));
    CatalogBaseTable newTable =
        DefaultCatalogCompat.INSTANCE.createCatalogTable(
            schema, "new comment", ImmutableList.of(), ImmutableMap.of("key", "new value"));
    org.apache.gravitino.rel.TableChange[] tableChanges =
        BaseCatalog.getGravitinoTableChanges(table, newTable);
    List<org.apache.gravitino.rel.TableChange> expected =
        ImmutableList.of(org.apache.gravitino.rel.TableChange.updateComment("new comment"));
    Assertions.assertArrayEquals(expected.toArray(), tableChanges);
  }

  @Test
  public void testBuildSchemaWithNanosecondTimestamps() {
    Schema schema =
        BaseCatalog.buildSchemaFromColumns(
                new org.apache.gravitino.rel.Column[] {
                  org.apache.gravitino.rel.Column.of(
                      "timestamp_ns", Types.TimestampType.withoutTimeZone(9)),
                  org.apache.gravitino.rel.Column.of(
                      "timestamp_tz_ns", Types.TimestampType.withTimeZone(9))
                })
            .build();

    Assertions.assertEquals(
        DataTypes.TIMESTAMP(9),
        ((Schema.UnresolvedPhysicalColumn) schema.getColumns().get(0)).getDataType());
    Assertions.assertEquals(
        DataTypes.TIMESTAMP_LTZ(9),
        ((Schema.UnresolvedPhysicalColumn) schema.getColumns().get(1)).getDataType());
  }

  @Test
  public void testRejectOffsetTimestampBeforeTableMutation() {
    TableCatalog tableCatalog = Mockito.mock(TableCatalog.class);
    Catalog gravitinoCatalog = Mockito.mock(Catalog.class);
    Mockito.when(gravitinoCatalog.asTableCatalog()).thenReturn(tableCatalog);
    BaseCatalog catalog =
        new TestableBaseCatalog(Mockito.mock(AbstractCatalog.class), gravitinoCatalog);

    Schema schema =
        Schema.newBuilder()
            .column("timestamp_tz_ns", DataTypes.TIMESTAMP_WITH_TIME_ZONE(9))
            .build();
    CatalogTable unresolvedTable =
        DefaultCatalogCompat.INSTANCE.createCatalogTable(
            schema, "test", ImmutableList.of(), ImmutableMap.of());
    ResolvedCatalogTable resolvedTable =
        new ResolvedCatalogTable(
            unresolvedTable,
            ResolvedSchema.of(
                Column.physical("timestamp_tz_ns", DataTypes.TIMESTAMP_WITH_TIME_ZONE(9))));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                catalog.createTable(
                    new ObjectPath("database", "timestamp_tz_table"), resolvedTable, false));
    Assertions.assertTrue(exception.getMessage().contains("cannot be losslessly represented"));
    Mockito.verifyNoInteractions(tableCatalog);
  }

  @Test
  public void testToGravitinoDistributionDefaultsToNone() {
    TestableBaseCatalog catalog =
        new TestableBaseCatalog(Mockito.mock(AbstractCatalog.class), mockUnsupportedViewCatalog());

    Assertions.assertEquals(
        Distributions.NONE,
        catalog.toGravitinoDistribution(ImmutableMap.of("bucket-key", "id", "bucket", "4")));
    Assertions.assertEquals(
        Distributions.NONE, catalog.toGravitinoDistribution(Collections.emptyMap()));
    Assertions.assertEquals(Distributions.NONE, catalog.toGravitinoDistribution(null));
  }

  @Test
  public void testListViewsReturnsEmptyWhenViewCatalogUnsupported() throws Exception {
    Catalog gravitinoCatalog = mockUnsupportedViewCatalog();
    BaseCatalog catalog =
        new TestableBaseCatalog(Mockito.mock(AbstractCatalog.class), gravitinoCatalog);

    List<String> views = catalog.listViews("db");

    Assertions.assertTrue(views.isEmpty());
  }

  @Test
  public void testListViewsDelegatesToViewCatalog() throws Exception {
    ViewCatalog viewCatalog = Mockito.mock(ViewCatalog.class);
    Mockito.when(viewCatalog.listViews(Namespace.of("db")))
        .thenReturn(
            new NameIdentifier[] {NameIdentifier.of("db", "v1"), NameIdentifier.of("db", "v2")});
    Catalog gravitinoCatalog = Mockito.mock(Catalog.class);
    Mockito.when(gravitinoCatalog.asViewCatalog()).thenReturn(viewCatalog);

    BaseCatalog catalog =
        new TestableBaseCatalog(Mockito.mock(AbstractCatalog.class), gravitinoCatalog);

    List<String> views = catalog.listViews("db");

    Assertions.assertEquals(ImmutableList.of("v1", "v2"), views);
  }

  @Test
  public void testGetGravitinoViewChangesSetAndRemoveProperty() {
    List<TableChange> tableChanges =
        ImmutableList.of(TableChange.set("k1", "v1"), TableChange.reset("k2"));

    Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();

    ViewChange[] changes =
        BaseCatalog.toReplaceViewChange(
            tableChanges, resolveView(schema, "SELECT 1", "comment"), Dialects.FLINK);

    Assertions.assertEquals(2, changes.length);
    Assertions.assertInstanceOf(ViewChange.SetProperty.class, changes[0]);
    Assertions.assertEquals("k1", ((ViewChange.SetProperty) changes[0]).getProperty());
    Assertions.assertEquals("v1", ((ViewChange.SetProperty) changes[0]).getValue());

    Assertions.assertInstanceOf(ViewChange.RemoveProperty.class, changes[1]);
    Assertions.assertEquals("k2", ((ViewChange.RemoveProperty) changes[1]).getProperty());
  }

  @Test
  public void testGetGravitinoViewChangesBodyReplaceOnStructuralChange() {
    List<TableChange> tableChanges =
        ImmutableList.of(
            TableChange.add(Column.physical("id", DataTypes.INT())), TableChange.set("k1", "v1"));

    Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();

    ViewChange[] changes =
        BaseCatalog.toReplaceViewChange(
            tableChanges, resolveView(schema, "SELECT id FROM t", "new comment"), Dialects.FLINK);

    // Should have exactly one SetProperty and one ReplaceView (order may vary)
    Assertions.assertEquals(2, changes.length);
    ViewChange.SetProperty setProp =
        (ViewChange.SetProperty)
            Arrays.stream(changes)
                .filter(c -> c instanceof ViewChange.SetProperty)
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected SetProperty"));
    Assertions.assertEquals("k1", setProp.getProperty());
    Assertions.assertEquals("v1", setProp.getValue());

    ViewChange.ReplaceView replaceView =
        (ViewChange.ReplaceView)
            Arrays.stream(changes)
                .filter(c -> c instanceof ViewChange.ReplaceView)
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected ReplaceView"));
    Assertions.assertEquals("new comment", replaceView.getComment());
    Assertions.assertEquals(1, replaceView.getRepresentations().length);
    Assertions.assertInstanceOf(SQLRepresentation.class, replaceView.getRepresentations()[0]);
    SQLRepresentation sqlRep = (SQLRepresentation) replaceView.getRepresentations()[0];
    Assertions.assertEquals(Dialects.FLINK, sqlRep.dialect());
    Assertions.assertEquals("SELECT id FROM t", sqlRep.sql());
  }

  @Test
  public void testGetGravitinoViewChangesFullReplace() {
    Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
    CatalogView existing =
        CatalogView.of(schema, "old comment", "SELECT 1", "SELECT 1", Collections.emptyMap());

    ViewChange[] changes =
        BaseCatalog.toReplaceViewChange(
            existing, resolveView(schema, "SELECT 2", "new comment"), Dialects.FLINK);

    Assertions.assertEquals(1, changes.length);
    Assertions.assertInstanceOf(ViewChange.ReplaceView.class, changes[0]);
    ViewChange.ReplaceView replaceView = (ViewChange.ReplaceView) changes[0];
    Assertions.assertEquals("new comment", replaceView.getComment());
    Representation[] reps = replaceView.getRepresentations();
    Assertions.assertEquals(1, reps.length);
    SQLRepresentation sqlRep = (SQLRepresentation) reps[0];
    Assertions.assertEquals("SELECT 2", sqlRep.sql());
    Assertions.assertEquals(Dialects.FLINK, sqlRep.dialect());
  }

  /**
   * Helper to build a minimal {@link org.apache.flink.table.catalog.ResolvedCatalogView} for
   * testing view-change conversion.
   */
  private static ResolvedCatalogView resolveView(Schema schema, String query, String comment) {
    CatalogView catalogView = CatalogView.of(schema, comment, query, query, Collections.emptyMap());
    ResolvedSchema resolvedSchema =
        ResolvedSchema.of(
            schema.getColumns().stream()
                .map(
                    c -> {
                      if (c instanceof Schema.UnresolvedPhysicalColumn) {
                        Schema.UnresolvedPhysicalColumn pc = (Schema.UnresolvedPhysicalColumn) c;
                        return Column.physical(pc.getName(), DataTypes.INT());
                      }
                      return Column.physical(c.getName(), DataTypes.INT());
                    })
                .collect(Collectors.toList()));
    return new ResolvedCatalogView(catalogView, resolvedSchema);
  }

  @Test
  public void testBaseCatalogViewDialectFallbackOrderIsFLINKThenHIVE() {
    BaseCatalog catalog = new TestableBaseCatalog(null, null);
    List<String> order = catalog.viewDialectFallbackOrder();
    Assertions.assertEquals(2, order.size());
    Assertions.assertEquals(Dialects.FLINK, order.get(0));
    Assertions.assertEquals(Dialects.HIVE, order.get(1));
  }

  @Test
  public void testBaseCatalogBuildViewRepresentationsProducesSingleFlinkEntry() {
    Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
    ResolvedCatalogView view = resolveView(schema, "SELECT id FROM t", "comment");
    BaseCatalog catalog = new TestableBaseCatalog(null, null);
    Representation[] reps = catalog.buildViewRepresentations(view);
    Assertions.assertEquals(1, reps.length);
    Assertions.assertInstanceOf(SQLRepresentation.class, reps[0]);
    SQLRepresentation sqlRep = (SQLRepresentation) reps[0];
    Assertions.assertEquals(Dialects.FLINK, sqlRep.dialect());
    Assertions.assertEquals("SELECT id FROM t", sqlRep.sql());
  }

  @Test
  public void testPaimonLikeViewDialectFallbackOrderIsFLINKThenHIVEThenQUERY() {
    BaseCatalog catalog = new PaimonLikeBaseCatalog();
    List<String> order = catalog.viewDialectFallbackOrder();
    Assertions.assertEquals(3, order.size());
    Assertions.assertEquals(Dialects.FLINK, order.get(0));
    Assertions.assertEquals(Dialects.HIVE, order.get(1));
    Assertions.assertEquals(PaimonConstants.VIEW_QUERY_DIALECT, order.get(2));
  }

  @Test
  public void testPaimonLikeBuildViewRepresentationsProducesBothFlinkAndQueryDialects() {
    Schema schema = Schema.newBuilder().column("id", DataTypes.INT()).build();
    ResolvedCatalogView view = resolveView(schema, "SELECT id FROM t", "comment");
    BaseCatalog catalog = new PaimonLikeBaseCatalog();
    Representation[] reps = catalog.buildViewRepresentations(view);
    Assertions.assertEquals(2, reps.length);
    SQLRepresentation first = (SQLRepresentation) reps[0];
    Assertions.assertEquals(Dialects.FLINK, first.dialect());
    Assertions.assertEquals("SELECT id FROM t", first.sql());
    SQLRepresentation second = (SQLRepresentation) reps[1];
    Assertions.assertEquals(PaimonConstants.VIEW_QUERY_DIALECT, second.dialect());
    Assertions.assertEquals("SELECT id FROM t", second.sql());
  }

  /**
   * Returns a mock {@link Catalog} whose {@code asViewCatalog()} throws
   * UnsupportedOperationException.
   */
  private static Catalog mockUnsupportedViewCatalog() {
    Catalog gravitinoCatalog = Mockito.mock(Catalog.class);
    Mockito.when(gravitinoCatalog.asViewCatalog())
        .thenThrow(new UnsupportedOperationException("views not supported"));
    return gravitinoCatalog;
  }

  /** Simulates a Paimon-like catalog that overrides dialect hooks. */
  private static class PaimonLikeBaseCatalog extends BaseCatalog {

    PaimonLikeBaseCatalog() {
      super(
          "paimon-test",
          Collections.emptyMap(),
          "default",
          Mockito.mock(SchemaAndTablePropertiesConverter.class),
          Mockito.mock(PartitionConverter.class));
    }

    @Override
    protected AbstractCatalog realCatalog() {
      return null;
    }

    @Override
    protected Catalog catalog() {
      return null;
    }

    @Override
    protected List<String> viewDialectFallbackOrder() {
      return Arrays.asList(Dialects.FLINK, Dialects.HIVE, PaimonConstants.VIEW_QUERY_DIALECT);
    }

    @Override
    protected Representation[] buildViewRepresentations(ResolvedCatalogView view) {
      String sql = view.getExpandedQuery();
      return new Representation[] {
        buildSqlRepresentation(Dialects.FLINK, sql)[0],
        buildSqlRepresentation(PaimonConstants.VIEW_QUERY_DIALECT, sql)[0]
      };
    }
  }

  private static class TestableBaseCatalog extends BaseCatalog {

    private final AbstractCatalog delegate;
    private final Catalog gravitinoCatalog;

    TestableBaseCatalog(AbstractCatalog delegate, Catalog gravitinoCatalog) {
      super(
          "test",
          Collections.emptyMap(),
          "default",
          Mockito.mock(SchemaAndTablePropertiesConverter.class),
          Mockito.mock(PartitionConverter.class));
      this.delegate = delegate;
      this.gravitinoCatalog = gravitinoCatalog;
    }

    @Override
    protected AbstractCatalog realCatalog() {
      return delegate;
    }

    @Override
    protected Catalog catalog() {
      return gravitinoCatalog;
    }
  }
}
