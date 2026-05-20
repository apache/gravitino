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
package org.apache.gravitino.catalog.lakehouse.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Arrays;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps;
import org.apache.gravitino.catalog.lakehouse.paimon.utils.PaimonViewTestCatalogHelper;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Tests for {@link PaimonViewCatalogOps}. */
public class TestPaimonViewCatalogOps {

  private static final String DATABASE = "test_view_ops_database";
  private static final String VIEW = "test_view_ops_view";
  private static final NameIdentifier VIEW_IDENTIFIER =
      NameIdentifier.of(Namespace.of(DATABASE), VIEW);

  @TempDir private File warehouse;

  private TestablePaimonCatalogOps paimonCatalogOps;
  private PaimonViewCatalogOps paimonViewCatalogOps;

  @BeforeEach
  public void setUp() throws Exception {
    paimonCatalogOps =
        new TestablePaimonCatalogOps(
            new PaimonConfig(
                ImmutableMap.of(PaimonCatalogPropertiesMetadata.WAREHOUSE, warehouse.getPath())));
    paimonCatalogOps.setCatalog(
        PaimonViewTestCatalogHelper.createViewSupportedCatalog(paimonCatalogOps.catalog()));
    paimonCatalogOps.createDatabase(DATABASE, Maps.newHashMap());

    paimonViewCatalogOps =
        new PaimonViewCatalogOps(
            paimonCatalogOps, this::buildPaimonNameIdentifier, this::schemaExists);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (paimonCatalogOps != null) {
      paimonCatalogOps.close();
    }
  }

  @Test
  public void testRenameViewCannotBeCombinedWithOtherChanges() throws Exception {
    createView(VIEW_IDENTIFIER);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            paimonViewCatalogOps.alterView(
                VIEW_IDENTIFIER,
                ViewChange.rename(VIEW + "_renamed"),
                ViewChange.setProperty("k1", "v1")));

    View unchangedView = paimonViewCatalogOps.loadView(VIEW_IDENTIFIER);
    assertEquals(VIEW, unchangedView.name());
    assertFalse(unchangedView.properties().containsKey("k1"));
  }

  @Test
  public void testAlterViewPropertyChangesAndRenameOnly() throws Exception {
    createView(VIEW_IDENTIFIER);

    paimonViewCatalogOps.alterView(
        VIEW_IDENTIFIER,
        ViewChange.setProperty("k1", "v1"),
        ViewChange.setProperty("k2", "v2"),
        ViewChange.removeProperty("k1"));

    View alteredView = paimonViewCatalogOps.loadView(VIEW_IDENTIFIER);
    assertFalse(alteredView.properties().containsKey("k1"));
    assertEquals("v2", alteredView.properties().get("k2"));

    String renamedViewName = VIEW + "_renamed";
    NameIdentifier renamedIdentifier =
        NameIdentifier.of(VIEW_IDENTIFIER.namespace(), renamedViewName);
    View renamedView =
        paimonViewCatalogOps.alterView(VIEW_IDENTIFIER, ViewChange.rename(renamedViewName));
    assertEquals(renamedViewName, renamedView.name());
    assertThrows(NoSuchViewException.class, () -> paimonViewCatalogOps.loadView(VIEW_IDENTIFIER));
    assertEquals("v2", paimonViewCatalogOps.loadView(renamedIdentifier).properties().get("k2"));
  }

  @Test
  public void testAlterViewReplaceUpdatesBodyAndPreservesProperties() throws Exception {
    NameIdentifier replaceIdentifier = NameIdentifier.of(Namespace.of(DATABASE), VIEW + "_replace");
    String originalQuery = "SELECT col_1 FROM source_table";

    paimonViewCatalogOps.createView(
        replaceIdentifier,
        "original_view_comment",
        new Column[] {Column.of("col_1", Types.IntegerType.get(), "col_1")},
        new Representation[] {
          SQLRepresentation.builder().withDialect("query").withSql(originalQuery).build(),
          SQLRepresentation.builder().withDialect("spark").withSql(originalQuery).build()
        },
        "paimon",
        DATABASE,
        Maps.newHashMap(ImmutableMap.of("keep_key", "keep_value", "remove_key", "remove_value")));

    String replacedQuery = "SELECT col_2, col_3 FROM source_table";
    String replacedTrinoQuery = "SELECT CAST(col_2 AS BIGINT), col_3 FROM source_table";
    paimonViewCatalogOps.alterView(
        replaceIdentifier,
        ViewChange.setProperty("add_key", "add_value"),
        ViewChange.removeProperty("remove_key"),
        ViewChange.replaceView(
            new Column[] {
              Column.of("col_2", Types.LongType.get(), "col_2"),
              Column.of("col_3", Types.StringType.get(), "col_3")
            },
            new Representation[] {
              SQLRepresentation.builder().withDialect("query").withSql(replacedQuery).build(),
              SQLRepresentation.builder().withDialect("trino").withSql(replacedTrinoQuery).build()
            },
            "replaced_catalog",
            "replaced_schema",
            "replaced_view_comment"));

    View replacedView = paimonViewCatalogOps.loadView(replaceIdentifier);
    assertEquals("replaced_view_comment", replacedView.comment());
    assertEquals(2, replacedView.columns().length);
    assertEquals("col_2", replacedView.columns()[0].name());
    assertEquals("col_3", replacedView.columns()[1].name());
    assertEquals(2, replacedView.representations().length);
    assertTrue(replacedView.sqlFor("query").isPresent());
    assertEquals(replacedQuery, replacedView.sqlFor("query").get().sql());
    assertTrue(replacedView.sqlFor("trino").isPresent());
    assertEquals(replacedTrinoQuery, replacedView.sqlFor("trino").get().sql());
    assertTrue(replacedView.sqlFor("spark").isEmpty());
    assertEquals("replaced_catalog", replacedView.defaultCatalog());
    assertEquals("replaced_schema", replacedView.defaultSchema());
    assertEquals("keep_value", replacedView.properties().get("keep_key"));
    assertFalse(replacedView.properties().containsKey("remove_key"));
    assertEquals("add_value", replacedView.properties().get("add_key"));
  }

  @Test
  public void testListAndDropViewOperations() throws Exception {
    NameIdentifier firstIdentifier = NameIdentifier.of(Namespace.of(DATABASE), VIEW + "_list_1");
    NameIdentifier secondIdentifier = NameIdentifier.of(Namespace.of(DATABASE), VIEW + "_list_2");
    createView(firstIdentifier);
    createView(secondIdentifier);

    NameIdentifier[] listedViews = paimonViewCatalogOps.listViews(Namespace.of(DATABASE));
    assertEquals(2, listedViews.length);
    assertTrue(Arrays.asList(listedViews).contains(firstIdentifier));
    assertTrue(Arrays.asList(listedViews).contains(secondIdentifier));

    assertTrue(paimonViewCatalogOps.dropView(firstIdentifier));
    assertFalse(paimonViewCatalogOps.dropView(firstIdentifier));
    assertThrows(NoSuchViewException.class, () -> paimonViewCatalogOps.loadView(firstIdentifier));

    NameIdentifier[] remainingViews = paimonViewCatalogOps.listViews(Namespace.of(DATABASE));
    assertEquals(1, remainingViews.length);
    assertEquals(secondIdentifier, remainingViews[0]);
  }

  @Test
  public void testRenameAndReplaceCannotBeCombined() throws Exception {
    createView(VIEW_IDENTIFIER);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                paimonViewCatalogOps.alterView(
                    VIEW_IDENTIFIER,
                    ViewChange.rename(VIEW + "_renamed"),
                    ViewChange.replaceView(
                        new Column[] {Column.of("col_2", Types.IntegerType.get(), "col_2")},
                        new Representation[] {
                          SQLRepresentation.builder()
                              .withDialect("query")
                              .withSql("SELECT col_2 FROM source_table")
                              .build()
                        },
                        "paimon",
                        DATABASE,
                        "replaced_view_comment")));

    assertTrue(exception.getMessage().contains("cannot be performed together"));
    assertEquals(VIEW, paimonViewCatalogOps.loadView(VIEW_IDENTIFIER).name());
  }

  @Test
  public void testMultipleRenameChangesAreRejected() throws Exception {
    createView(VIEW_IDENTIFIER);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            paimonViewCatalogOps.alterView(
                VIEW_IDENTIFIER, ViewChange.rename("rename_1"), ViewChange.rename("rename_2")));

    assertEquals(VIEW, paimonViewCatalogOps.loadView(VIEW_IDENTIFIER).name());
  }

  @Test
  public void testRenameViewFailsWithTypedExceptionWhenTargetExists() throws Exception {
    NameIdentifier sourceIdentifier = NameIdentifier.of(Namespace.of(DATABASE), VIEW + "_source");
    NameIdentifier targetIdentifier = NameIdentifier.of(Namespace.of(DATABASE), VIEW + "_target");
    createView(sourceIdentifier);
    createView(targetIdentifier);

    ViewAlreadyExistsException exception =
        assertThrows(
            ViewAlreadyExistsException.class,
            () ->
                paimonViewCatalogOps.alterView(
                    sourceIdentifier, ViewChange.rename(targetIdentifier.name())));
    assertTrue(exception.getMessage().contains(targetIdentifier.name()));
    assertEquals(sourceIdentifier.name(), paimonViewCatalogOps.loadView(sourceIdentifier).name());
    assertEquals(targetIdentifier.name(), paimonViewCatalogOps.loadView(targetIdentifier).name());
  }

  @Test
  public void testRenameViewReportsLoadFailureAfterSuccessfulRename() throws Exception {
    NameIdentifier sourceIdentifier =
        NameIdentifier.of(Namespace.of(DATABASE), VIEW + "_rename_load_source");
    createView(sourceIdentifier);
    NameIdentifier renamedIdentifier =
        NameIdentifier.of(sourceIdentifier.namespace(), VIEW + "_rename_load_target");
    NameIdentifier paimonRenamedIdentifier = buildPaimonNameIdentifier(renamedIdentifier);
    paimonCatalogOps.failLoadView(paimonRenamedIdentifier.toString());

    try {
      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class,
              () ->
                  paimonViewCatalogOps.alterView(
                      sourceIdentifier, ViewChange.rename(renamedIdentifier.name())));
      assertTrue(exception.getMessage().contains(sourceIdentifier.toString()));
      assertTrue(exception.getMessage().contains(renamedIdentifier.toString()));
    } finally {
      paimonCatalogOps.clearFailLoadView();
    }

    assertThrows(NoSuchViewException.class, () -> paimonViewCatalogOps.loadView(sourceIdentifier));
    assertEquals(renamedIdentifier.name(), paimonViewCatalogOps.loadView(renamedIdentifier).name());
  }

  @Test
  public void testCreateViewRequiresQueryRepresentation() {
    String query = "SELECT col_1 FROM source_table";
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("spark").withSql(query).build()
        };

    assertThrows(
        IllegalArgumentException.class,
        () ->
            paimonViewCatalogOps.createView(
                NameIdentifier.of(Namespace.of(DATABASE), "missing_query_dialect"),
                "test_view_comment",
                new Column[] {Column.of("col_1", Types.IntegerType.get(), "col_1")},
                representations,
                "paimon",
                DATABASE,
                Maps.newHashMap()));
  }

  @Test
  public void testCreateViewAcceptsCaseInsensitiveQueryRepresentation() throws Exception {
    String query = "SELECT col_1 FROM source_table";
    NameIdentifier identifier = NameIdentifier.of(Namespace.of(DATABASE), "upper_case_query");
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("QUERY").withSql(query).build(),
          SQLRepresentation.builder().withDialect("Spark").withSql(query).build()
        };

    paimonViewCatalogOps.createView(
        identifier,
        "test_view_comment",
        new Column[] {Column.of("col_1", Types.IntegerType.get(), "col_1")},
        representations,
        "paimon",
        DATABASE,
        Maps.newHashMap());

    View loadedView = paimonViewCatalogOps.loadView(identifier);
    boolean hasQueryRepresentation =
        Arrays.stream(loadedView.representations())
            .anyMatch(
                representation ->
                    representation instanceof SQLRepresentation
                        && "query".equals(((SQLRepresentation) representation).dialect())
                        && query.equals(((SQLRepresentation) representation).sql()));
    assertTrue(hasQueryRepresentation);

    boolean hasNormalizedSparkRepresentation =
        Arrays.stream(loadedView.representations())
            .anyMatch(
                representation ->
                    representation instanceof SQLRepresentation
                        && "spark".equals(((SQLRepresentation) representation).dialect())
                        && query.equals(((SQLRepresentation) representation).sql()));
    assertTrue(hasNormalizedSparkRepresentation);
  }

  @Test
  public void testCreateViewRejectsCaseInsensitiveDuplicateDialects() {
    String query = "SELECT col_1 FROM source_table";
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("query").withSql(query).build(),
          SQLRepresentation.builder().withDialect("Spark").withSql(query).build(),
          SQLRepresentation.builder()
              .withDialect("spark")
              .withSql(query + " WHERE col_1 > 1")
              .build()
        };

    assertThrows(
        IllegalArgumentException.class,
        () ->
            paimonViewCatalogOps.createView(
                NameIdentifier.of(Namespace.of(DATABASE), "duplicate_dialect_case"),
                "test_view_comment",
                new Column[] {Column.of("col_1", Types.IntegerType.get(), "col_1")},
                representations,
                "paimon",
                DATABASE,
                Maps.newHashMap()));
  }

  @Test
  public void testCreateViewDatabaseNotExistUsesSchemaIdentifierInErrorMessage() {
    String query = "SELECT col_1 FROM source_table";
    NameIdentifier identifier = NameIdentifier.of(Namespace.of("missing_schema"), "missing_view");
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("query").withSql(query).build()
        };

    PaimonViewCatalogOps bypassedSchemaCheckOps =
        new PaimonViewCatalogOps(
            paimonCatalogOps, this::buildPaimonNameIdentifier, schemaIdentifier -> true);

    NoSuchSchemaException exception =
        assertThrows(
            NoSuchSchemaException.class,
            () ->
                bypassedSchemaCheckOps.createView(
                    identifier,
                    "test_view_comment",
                    new Column[] {Column.of("col_1", Types.IntegerType.get(), "col_1")},
                    representations,
                    "paimon",
                    "missing_schema",
                    Maps.newHashMap()));

    assertTrue(exception.getMessage().contains("missing_schema"));
    assertFalse(exception.getMessage().contains("missing_view"));
  }

  private void createView(NameIdentifier identifier) throws Exception {
    String query = "SELECT col_1 FROM source_table";
    Representation[] representations =
        new Representation[] {
          SQLRepresentation.builder().withDialect("query").withSql(query).build(),
          SQLRepresentation.builder().withDialect("spark").withSql(query).build()
        };

    paimonViewCatalogOps.createView(
        identifier,
        "test_view_comment",
        new Column[] {Column.of("col_1", Types.IntegerType.get(), "col_1")},
        representations,
        "paimon",
        DATABASE,
        Maps.newHashMap());
  }

  private boolean schemaExists(NameIdentifier identifier) {
    try {
      paimonCatalogOps.loadDatabase(identifier.name());
      return true;
    } catch (Catalog.DatabaseNotExistException e) {
      return false;
    }
  }

  private NameIdentifier buildPaimonNameIdentifier(NameIdentifier identifier) {
    String[] levels = identifier.namespace().levels();
    return NameIdentifier.of(levels[levels.length - 1], identifier.name());
  }

  private static class TestablePaimonCatalogOps extends PaimonCatalogOps {

    private String failLoadViewName;

    TestablePaimonCatalogOps(PaimonConfig paimonConfig) {
      super(paimonConfig);
    }

    Catalog catalog() {
      return catalog;
    }

    void setCatalog(Catalog catalog) {
      this.catalog = catalog;
    }

    void failLoadView(String viewName) {
      this.failLoadViewName = viewName;
    }

    void clearFailLoadView() {
      this.failLoadViewName = null;
    }

    @Override
    public org.apache.paimon.view.View loadView(String viewName)
        throws Catalog.ViewNotExistException {
      if (failLoadViewName != null && failLoadViewName.equals(viewName)) {
        throw new Catalog.ViewNotExistException(Identifier.fromString(viewName));
      }
      return super.loadView(viewName);
    }
  }
}
