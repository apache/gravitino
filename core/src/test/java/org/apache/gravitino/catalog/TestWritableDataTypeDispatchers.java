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
package org.apache.gravitino.catalog;

import static org.mockito.Mockito.verifyNoInteractions;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.TestCatalog;
import org.apache.gravitino.connector.TestCatalogOperations;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class TestWritableDataTypeDispatchers extends TestOperationDispatcher {
  private static final NameIdentifier SCHEMA =
      NameIdentifier.of(metalake, catalog, "writable_types");
  private static final Map<String, String> PROPERTIES = ImmutableMap.of("k1", "v1", "k2", "v2");
  private static final Type UNPARSED = Types.UnparsedType.of("future_type");
  private static final Type EXTERNAL = Types.ExternalType.of("jsonb");
  private static final FunctionImpl[] IMPLS = {
    FunctionImpls.ofJava(FunctionImpl.RuntimeType.SPARK, "com.example.UDF")
  };
  private static final Representation[] REPRESENTATIONS = {
    SQLRepresentation.builder().withDialect("trino").withSql("SELECT 1").build()
  };

  private static TableOperationDispatcher tables;
  private static ViewOperationDispatcher views;
  private static PartitionOperationDispatcher partitions;
  private static FunctionOperationDispatcher functions;

  @BeforeAll
  public static void initialize() throws IllegalAccessException {
    SchemaOperationDispatcher schemas =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "internalSchemaDispatcher", schemas, true);
    schemas.createSchema(SCHEMA, "comment", PROPERTIES);
    tables =
        new TableOperationDispatcher(
            catalogManager, entityStore, idGenerator, () -> schemas, secretManager);
    views =
        new ViewOperationDispatcher(
            catalogManager, entityStore, idGenerator, () -> schemas, secretManager);
    partitions =
        new PartitionOperationDispatcher(catalogManager, entityStore, idGenerator, secretManager);
    functions =
        new FunctionOperationDispatcher(
            catalogManager, schemas, entityStore, idGenerator, secretManager);
  }

  @Test
  public void testCreateTableRejectsTypesBeforeCatalogAccess() {
    for (Type type :
        new Type[] {UNPARSED, Types.ListType.of(UNPARSED, true), Types.ExternalType.of(" ")}) {
      assertRejected(
          () ->
              tables.createTable(
                  ident("invalid"), new Column[] {Column.of("c", type)}, null, PROPERTIES),
          "columns[0].type");
    }
    Literal<?> literal = Literals.of(null, UNPARSED);
    assertRejected(
        () ->
            tables.createTable(
                ident("invalid"),
                new Column[] {Column.of("c", Types.StringType.get(), null, literal)},
                null,
                PROPERTIES),
        "columns[0].defaultValue.dataType");
    Column[] columns = {Column.of("c", Types.StringType.get())};
    RangePartition range = Partitions.range("p", literal, Literals.stringLiteral("a"), null);
    assertRejected(
        () ->
            tables.createTable(
                ident("invalid"),
                columns,
                null,
                PROPERTIES,
                new Transform[] {
                  Transforms.range(new String[] {"c"}, new RangePartition[] {range})
                },
                Distributions.NONE,
                new SortOrder[0],
                Indexes.EMPTY_INDEXES),
        "partitioning[0].assignments[0].upper.dataType");
    assertRejected(
        () ->
            tables.createTable(
                ident("invalid"),
                columns,
                null,
                PROPERTIES,
                new Transform[] {Transforms.apply("f", new Expression[] {literal})},
                Distributions.NONE,
                new SortOrder[0],
                Indexes.EMPTY_INDEXES),
        "partitioning[0].funcArgs[0].dataType");
    assertRejected(
        () ->
            tables.createTable(
                ident("invalid"),
                columns,
                null,
                PROPERTIES,
                new Transform[0],
                Distributions.of(Strategy.HASH, 2, literal),
                new SortOrder[0],
                Indexes.EMPTY_INDEXES),
        "distribution.funcArgs[0].dataType");
    assertRejected(
        () ->
            tables.createTable(
                ident("invalid"),
                columns,
                null,
                PROPERTIES,
                new Transform[0],
                Distributions.NONE,
                new SortOrder[] {SortOrders.of(literal, SortDirection.ASCENDING)},
                Indexes.EMPTY_INDEXES),
        "sortOrders[0].sortTerm.dataType");
    verifyNoInteractions(entityStore);
  }

  @Test
  public void testTableChangesValidateEntireBatchBeforeMutation() {
    NameIdentifier ident = ident("alter_types");
    tables.createTable(ident, new Column[] {Column.of("c", EXTERNAL)}, "original", PROPERTIES);
    for (TableChange invalid :
        new TableChange[] {
          TableChange.addColumn(new String[] {"new"}, Types.ListType.of(UNPARSED, true)),
          TableChange.updateColumnType(new String[] {"c"}, Types.ExternalType.of(" ")),
          TableChange.addColumn(
              new String[] {"new"},
              EXTERNAL,
              null,
              TableChange.ColumnPosition.defaultPos(),
              true,
              false,
              Literals.of("x", UNPARSED)),
          TableChange.updateColumnDefaultValue(
              new String[] {"c"}, FunctionExpression.of("f", Literals.of(null, UNPARSED)))
        }) {
      assertRejected(
          () -> tables.alterTable(ident, TableChange.setProperty("k1", "changed"), invalid),
          "updates[1]");
      Table unchanged = tables.loadTable(ident);
      Assertions.assertEquals("v1", unchanged.properties().get("k1"));
      Assertions.assertEquals(1, unchanged.columns().length);
      Assertions.assertEquals(EXTERNAL, unchanged.columns()[0].dataType());
    }
    Table updated =
        tables.alterTable(
            ident,
            TableChange.addColumn(new String[] {"new"}, Types.ListType.of(EXTERNAL, true)),
            TableChange.updateColumnType(new String[] {"c"}, Types.StringType.get()));
    Assertions.assertEquals(Types.StringType.get(), updated.columns()[0].dataType());
    Assertions.assertEquals(Types.ListType.of(EXTERNAL, true), updated.columns()[1].dataType());
  }

  @Test
  public void testLegacyTableCanBeImportedAndManaged() {
    NameIdentifier ident = ident("legacy_table");
    TestCatalog testCatalog =
        (TestCatalog)
            catalogManager.loadCatalogAndWrap(NameIdentifier.of(metalake, catalog)).catalog();
    ((TestCatalogOperations) testCatalog.ops())
        .createTable(
            ident,
            new Column[] {Column.of("legacy", UNPARSED)},
            "original",
            PROPERTIES,
            new Transform[0],
            Distributions.NONE,
            new SortOrder[0],
            Indexes.EMPTY_INDEXES);
    Assertions.assertEquals(UNPARSED, tables.loadTable(ident).columns()[0].dataType());
    Table updated =
        tables.alterTable(
            ident,
            TableChange.updateComment("updated"),
            TableChange.updateColumnComment(new String[] {"legacy"}, "updated"),
            TableChange.renameColumn(new String[] {"legacy"}, "renamed"),
            TableChange.addColumn(new String[] {"native"}, Types.IntegerType.get()));
    Assertions.assertEquals("updated", updated.columns()[0].comment());
    Assertions.assertEquals("renamed", updated.columns()[0].name());
    Assertions.assertEquals(UNPARSED, updated.columns()[0].dataType());
    Assertions.assertTrue(tables.dropTable(ident));
  }

  @Test
  public void testViewTypesRejectedBeforeCatalogAccess() {
    for (Column column :
        new Column[] {
          Column.of("c", UNPARSED),
          Column.of("c", Types.ListType.of(UNPARSED, true)),
          Column.of("c", Types.ExternalType.of(" ")),
          Column.of("c", Types.StringType.get(), null, Literals.of(null, UNPARSED))
        }) {
      assertRejected(
          () ->
              views.createView(
                  ident("invalid_view"),
                  null,
                  new Column[] {column},
                  REPRESENTATIONS,
                  null,
                  null,
                  PROPERTIES),
          "columns[0]");
      assertRejected(
          () ->
              views.alterView(
                  ident("invalid_view"),
                  ViewChange.setProperty("k1", "changed"),
                  ViewChange.replaceView(new Column[] {column}, REPRESENTATIONS, null, null, null)),
          "updates[1].columns[0]");
    }
    verifyNoInteractions(entityStore);
  }

  @Test
  public void testViewReplacementValidatesBeforeMutation() {
    NameIdentifier ident = ident("view_types");
    View created = views.createView(ident, null, null, REPRESENTATIONS, null, null, PROPERTIES);
    Assertions.assertEquals(0, created.columns().length);
    Type externalList = Types.ListType.of(EXTERNAL, true);
    View replaced =
        views.alterView(
            ident,
            ViewChange.replaceView(
                new Column[] {Column.of("c", externalList)}, REPRESENTATIONS, null, null, null));
    Assertions.assertEquals(externalList, replaced.columns()[0].dataType());

    assertRejected(
        () ->
            views.alterView(
                ident,
                ViewChange.setProperty("k1", "changed"),
                ViewChange.replaceView(
                    new Column[] {Column.of("c", UNPARSED)}, REPRESENTATIONS, null, null, null)),
        "updates[1].columns[0].type");
    View unchanged = views.loadView(ident);
    Assertions.assertEquals("v1", unchanged.properties().get("k1"));
    Assertions.assertEquals(externalList, unchanged.columns()[0].dataType());
    Assertions.assertTrue(views.dropView(ident));
  }

  @Test
  public void testLegacyViewCanBeImportedAndManaged() {
    NameIdentifier ident = ident("legacy_view");
    TestCatalog testCatalog =
        (TestCatalog)
            catalogManager.loadCatalogAndWrap(NameIdentifier.of(metalake, catalog)).catalog();
    ((TestCatalogOperations) testCatalog.ops())
        .createView(
            ident,
            null,
            new Column[] {Column.of("legacy", UNPARSED)},
            REPRESENTATIONS,
            null,
            null,
            PROPERTIES);
    Assertions.assertEquals(UNPARSED, views.loadView(ident).columns()[0].dataType());
    View updated =
        views.alterView(
            ident, ViewChange.setProperty("k1", "changed"), ViewChange.removeProperty("k2"));
    Assertions.assertEquals("changed", updated.properties().get("k1"));
    Assertions.assertFalse(updated.properties().containsKey("k2"));
    Assertions.assertEquals(UNPARSED, updated.columns()[0].dataType());
    View renamed = views.alterView(ident, ViewChange.rename("renamed_legacy_view"));
    Assertions.assertEquals("renamed_legacy_view", renamed.name());
    Assertions.assertEquals(UNPARSED, renamed.columns()[0].dataType());
    Assertions.assertTrue(views.dropView(ident("renamed_legacy_view")));
  }

  @Test
  public void testPartitionTypesRejectedBeforeCatalogAccess() {
    Literal<?> literal = Literals.of(null, UNPARSED);
    for (Partition partition :
        new Partition[] {
          Partitions.identity("p", new String[][] {{"c"}}, new Literal<?>[] {literal}, null),
          Partitions.list("p", new Literal<?>[][] {{literal}}, null),
          Partitions.range("p", literal, Literals.stringLiteral("a"), null),
          Partitions.range("p", Literals.stringLiteral("z"), literal, null)
        }) {
      assertRejected(() -> partitions.addPartition(ident("missing"), partition), "partitions[0]");
    }
    verifyNoInteractions(entityStore);
  }

  @Test
  public void testFunctionDefinitionsRejectedBeforeStorageAccess() {
    FunctionParam[] invalidParam = {FunctionParams.of("x", UNPARSED)};
    FunctionParam[] invalidDefault = {
      FunctionParams.of(
          "x",
          Types.StringType.get(),
          null,
          FunctionExpression.of("f", Literals.of(null, UNPARSED)))
    };
    for (FunctionDefinition definition :
        new FunctionDefinition[] {
          FunctionDefinitions.of(invalidParam, Types.StringType.get(), IMPLS),
          FunctionDefinitions.of(invalidDefault, Types.StringType.get(), IMPLS),
          FunctionDefinitions.of(new FunctionParam[0], Types.ListType.of(UNPARSED, true), IMPLS),
          FunctionDefinitions.of(
              new FunctionParam[0],
              new FunctionColumn[] {FunctionColumn.of("x", Types.ExternalType.of(" "), null)},
              IMPLS)
        }) {
      assertRejected(
          () ->
              functions.registerFunction(
                  ident("invalid_function"),
                  null,
                  FunctionType.SCALAR,
                  true,
                  new FunctionDefinition[] {definition}),
          "definitions[0]");
      assertRejected(
          () ->
              functions.alterFunction(
                  ident("invalid_function"),
                  FunctionChange.updateComment("new"),
                  FunctionChange.addDefinition(definition)),
          "updates[1].definition");
    }
    verifyNoInteractions(entityStore);
  }

  @Test
  public void testExternalFunctionAndLegacySelectors() {
    FunctionParam[] externalParams = {FunctionParams.of("x", EXTERNAL)};
    FunctionDefinition external = FunctionDefinitions.of(externalParams, EXTERNAL, IMPLS);
    Function created =
        functions.registerFunction(
            ident("external_function"),
            null,
            FunctionType.SCALAR,
            true,
            new FunctionDefinition[] {external});
    Assertions.assertEquals(EXTERNAL, created.definitions()[0].returnType());

    NameIdentifier legacyIdent = ident("legacy_function");
    FunctionParam[] legacyParams = {FunctionParams.of("x", UNPARSED)};
    FunctionDefinition legacy = FunctionDefinitions.of(legacyParams, UNPARSED, IMPLS);
    // Seed stored metadata without going through the new-write dispatcher.
    new ManagedFunctionOperations(entityStore, idGenerator)
        .registerFunction(
            legacyIdent, null, FunctionType.SCALAR, true, new FunctionDefinition[] {legacy});
    Assertions.assertEquals(
        UNPARSED, functions.getFunction(legacyIdent).definitions()[0].returnType());
    functions.alterFunction(legacyIdent, FunctionChange.addDefinition(external));
    FunctionImpl trino = FunctionImpls.ofJava(FunctionImpl.RuntimeType.TRINO, "com.example.UDF");
    Function added =
        functions.alterFunction(legacyIdent, FunctionChange.addImpl(legacyParams, trino));
    Assertions.assertEquals(2, added.definitions()[0].impls().length);
    functions.alterFunction(
        legacyIdent,
        FunctionChange.updateImpl(legacyParams, FunctionImpl.RuntimeType.TRINO, trino));
    functions.alterFunction(
        legacyIdent, FunctionChange.removeImpl(legacyParams, FunctionImpl.RuntimeType.TRINO));
    Function remaining =
        functions.alterFunction(legacyIdent, FunctionChange.removeDefinition(legacyParams));
    Assertions.assertEquals(1, remaining.definitions().length);
    Assertions.assertEquals(EXTERNAL, remaining.definitions()[0].returnType());
  }

  private static NameIdentifier ident(String name) {
    return NameIdentifier.of(metalake, catalog, SCHEMA.name(), name);
  }

  private static void assertRejected(Executable operation, String path) {
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, operation);
    Assertions.assertTrue(exception.getMessage().contains(path), exception.getMessage());
  }
}
