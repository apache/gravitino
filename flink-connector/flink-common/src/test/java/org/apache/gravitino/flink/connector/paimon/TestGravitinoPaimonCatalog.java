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
package org.apache.gravitino.flink.connector.paimon;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalog;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code enrichCatalogTable} hook introduced in {@link BaseCatalog} and its
 * implementation in {@link GravitinoPaimonCatalog}.
 *
 * <p>These tests verify:
 *
 * <ol>
 *   <li>The default {@code BaseCatalog.enrichCatalogTable} is an identity function.
 *   <li>{@code GravitinoPaimonCatalog.enrichCatalogTable} returns the Paimon-native table (i.e. the
 *       result of {@code paimonCatalog.getTable()}) rather than the plain Gravitino {@link
 *       CatalogTable}.
 *   <li>When {@code paimonCatalog.getTable()} throws {@link TableNotExistException} (metadata
 *       out-of-sync), a descriptive {@link CatalogException} is raised.
 *   <li>{@code paimonCatalog.getTable()} is never called when Gravitino auth fails.
 * </ol>
 */
public class TestGravitinoPaimonCatalog {

  private AbstractCatalog mockPaimonCatalog;

  // ---------------------------------------------------------------------------
  // Minimal test double for BaseCatalog — overrides only what's needed
  // ---------------------------------------------------------------------------

  /**
   * Subclass of {@link BaseCatalog} used to test the {@code enrichCatalogTable} default behaviour.
   * All abstract methods delegate to no-ops or mocks; the only interesting thing is the hook.
   */
  private static class TestableBaseCatalog extends BaseCatalog {

    private final AbstractCatalog realCatalog = mock(AbstractCatalog.class);
    private final Catalog gravitinoCatalog = mock(Catalog.class);
    private CatalogBaseTable toFlinkTableResult;
    private CatalogException toFlinkTableException;

    TestableBaseCatalog() {
      super(
          "test-catalog",
          Collections.emptyMap(),
          "default",
          PaimonPropertiesConverter.INSTANCE,
          DefaultPartitionConverter.INSTANCE);
    }

    @Override
    protected AbstractCatalog realCatalog() {
      return realCatalog;
    }

    @Override
    protected Catalog catalog() {
      return gravitinoCatalog;
    }

    @Override
    protected CatalogBaseTable toFlinkTable(Table table, ObjectPath tablePath) {
      if (toFlinkTableException != null) {
        throw toFlinkTableException;
      }
      CatalogTable baseTable = (CatalogTable) toFlinkTableResult;
      return enrichCatalogTable(baseTable, tablePath);
    }

    public CatalogBaseTable callEnrichCatalogTable(CatalogTable table, ObjectPath path) {
      return enrichCatalogTable(table, path);
    }
  }

  // ---------------------------------------------------------------------------
  // Minimal test double for GravitinoPaimonCatalog that injects a mock paimonCatalog
  // ---------------------------------------------------------------------------

  /**
   * Extends {@link GravitinoPaimonCatalog} so that tests can inject a mock native catalog via
   * {@link #realCatalog()} and verify cache invalidation and enrichment behavior without a real
   * Paimon environment.
   */
  private static class TestablePaimonCatalog extends GravitinoPaimonCatalog {

    private final AbstractCatalog injectedPaimon;
    private final Catalog injectedCatalog;

    TestablePaimonCatalog(AbstractCatalog injectedPaimon) {
      this(injectedPaimon, null);
    }

    TestablePaimonCatalog(AbstractCatalog injectedPaimon, Catalog injectedCatalog) {
      // We cannot call super(context, ...) without a real FlinkCatalogFactory, so we use a
      // package-private constructor shim that skips the factory call.  Because we override
      // realCatalog() the parent constructor's catalog reference is never used.
      super(
          new MockCatalogContext("test-paimon", Collections.emptyMap()),
          "default",
          PaimonPropertiesConverter.INSTANCE,
          DefaultPartitionConverter.INSTANCE);
      this.injectedPaimon = injectedPaimon;
      this.injectedCatalog = injectedCatalog;
    }

    @Override
    protected AbstractCatalog realCatalog() {
      return injectedPaimon;
    }

    @Override
    protected Catalog catalog() {
      return injectedCatalog != null ? injectedCatalog : super.catalog();
    }
  }

  @BeforeEach
  void setUp() {
    mockPaimonCatalog = mock(AbstractCatalog.class);
  }

  // ---------------------------------------------------------------------------
  // BaseCatalog default hook: identity function
  // ---------------------------------------------------------------------------

  /**
   * The base implementation must return the same {@link CatalogTable} it receives, unchanged. This
   * ensures existing catalogs (Hive, Iceberg, JDBC) are unaffected by the new hook.
   */
  @Test
  public void testDefaultEnrichCatalogTableIsIdentity() {
    TestableBaseCatalog base = new TestableBaseCatalog();
    CatalogTable input = mock(CatalogTable.class);
    ObjectPath path = new ObjectPath("db", "tbl");

    CatalogBaseTable result = base.callEnrichCatalogTable(input, path);

    Assertions.assertSame(
        input,
        result,
        "BaseCatalog.enrichCatalogTable must return the input table unchanged by default");
  }

  // ---------------------------------------------------------------------------
  // GravitinoPaimonCatalog: enrichCatalogTable returns Paimon-native table
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@code GravitinoPaimonCatalog.enrichCatalogTable} ignores the plain {@link
   * CatalogTable} and instead returns whatever {@code paimonCatalog.getTable()} produces.
   *
   * <p>The object returned by {@code paimonCatalog.getTable()} is Paimon's {@code
   * DataCatalogTable}, which carries a non-null {@code CatalogEnvironment}. This is what enables
   * {@code AddPartitionCommitCallback} registration on the write path, fixing the "partitions not
   * visible in {@code SHOW PARTITIONS}" bug.
   */
  @Test
  public void testEnrichCatalogTableReturnsPaimonNativeTable()
      throws TableNotExistException, CatalogException {
    CatalogBaseTable paimonNativeTable = mock(CatalogBaseTable.class);
    ObjectPath path = new ObjectPath("db", "tbl");
    when(mockPaimonCatalog.getTable(path)).thenReturn(paimonNativeTable);

    CatalogTable gravitinoBuiltTable = mock(CatalogTable.class);
    TestablePaimonCatalog cat = new TestablePaimonCatalog(mockPaimonCatalog);
    CatalogBaseTable result = cat.enrichCatalogTable(gravitinoBuiltTable, path);

    Assertions.assertSame(
        paimonNativeTable,
        result,
        "enrichCatalogTable must return the Paimon-native DataCatalogTable");
    verify(mockPaimonCatalog).getTable(path);
  }

  // ---------------------------------------------------------------------------
  // Metadata out-of-sync: TableNotExistException → CatalogException
  // ---------------------------------------------------------------------------

  /**
   * When Gravitino confirms the table exists but the underlying Paimon store does not have it,
   * {@code enrichCatalogTable} must throw a descriptive {@link CatalogException} rather than
   * leaking the raw {@link TableNotExistException} from Paimon.
   */
  @Test
  public void testEnrichCatalogTableOutOfSyncThrowsCatalogException()
      throws TableNotExistException {
    ObjectPath path = new ObjectPath("db", "missing_in_paimon");
    when(mockPaimonCatalog.getTable(path))
        .thenThrow(new TableNotExistException("test-paimon", path));

    CatalogTable gravitinoBuiltTable = mock(CatalogTable.class);

    TestablePaimonCatalog cat = new TestablePaimonCatalog(mockPaimonCatalog);
    CatalogException ex =
        Assertions.assertThrows(
            CatalogException.class, () -> cat.enrichCatalogTable(gravitinoBuiltTable, path));

    Assertions.assertTrue(
        ex.getMessage().contains("missing_in_paimon"),
        "Exception message should contain the table name for diagnostics");
    Assertions.assertInstanceOf(
        TableNotExistException.class,
        ex.getCause(),
        "Original TableNotExistException should be preserved as the cause");
  }

  // ---------------------------------------------------------------------------
  // paimonCatalog.getTable() must NOT be called when Gravitino auth fails
  // (this contract is enforced by BaseCatalog.getTable(), tested here as a
  //  sanity check via the enrichCatalogTable path)
  // ---------------------------------------------------------------------------

  /**
   * {@code paimonCatalog.getTable()} must never be called if Gravitino auth throws before {@code
   * enrichCatalogTable} is reached. This is enforced by {@link BaseCatalog#getTable} which invokes
   * the hook only after a successful Gravitino {@code loadTable()} call.
   *
   * <p>The test simulates this contract at the unit level by verifying that a direct call to {@code
   * enrichCatalogTable} (the hook itself) does call the inner catalog — confirming the security
   * boundary is in {@link BaseCatalog#getTable}, not in the hook.
   */
  @Test
  public void testGetTableAuthFailureDoesNotCallPaimonCatalog() throws Exception {
    Catalog mockCatalog = mock(Catalog.class);
    TableCatalog mockTableCatalog = mock(TableCatalog.class);
    ObjectPath path = new ObjectPath("db", "tbl");
    when(mockCatalog.asTableCatalog()).thenReturn(mockTableCatalog);
    when(mockTableCatalog.loadTable(any())).thenThrow(new RuntimeException("denied"));

    TestablePaimonCatalog cat = new TestablePaimonCatalog(mockPaimonCatalog, mockCatalog);

    Assertions.assertThrows(RuntimeException.class, () -> cat.getTable(path));
    verify(mockPaimonCatalog, never()).getTable(any());
  }

  // ---------------------------------------------------------------------------
  // invalidateNativeTableCache: Paimon CachingCatalog is evicted after DDL
  // ---------------------------------------------------------------------------

  /**
   * Verifies that {@code invalidateNativeTableCache} calls {@code Catalog.invalidateTable} on the
   * underlying Paimon inner catalog when {@code paimonCatalog} is a {@link FlinkCatalog}.
   *
   * <p>This ensures that after DDL operations (drop / rename / alter) routed through Gravitino,
   * stale entries in Paimon's {@code CachingCatalog} are evicted so subsequent reads reflect the
   * updated metadata.
   */
  @Test
  public void testInvalidateNativeTableCacheCallsPaimonInvalidate() {
    org.apache.paimon.catalog.Catalog mockInnerCatalog =
        mock(org.apache.paimon.catalog.Catalog.class);
    FlinkCatalog mockFlinkCatalog = mock(FlinkCatalog.class);
    when(mockFlinkCatalog.catalog()).thenReturn(mockInnerCatalog);

    TestablePaimonCatalog cat = new TestablePaimonCatalog(mockFlinkCatalog);
    ObjectPath path = new ObjectPath("mydb", "mytable");
    cat.invalidateNativeTableCache(path);

    Identifier expected = Identifier.create("mydb", "mytable");
    verify(mockInnerCatalog).invalidateTable(expected);
  }

  /**
   * Verifies that {@code invalidateNativeTableCache} is a no-op when the underlying catalog is not
   * a {@link FlinkCatalog} (e.g. in tests using a plain {@link AbstractCatalog} mock). No exception
   * should be thrown.
   */
  @Test
  public void testInvalidateNativeTableCacheIsNoOpForNonFlinkCatalog() {
    // mockPaimonCatalog is AbstractCatalog, not FlinkCatalog — must not throw
    TestablePaimonCatalog cat = new TestablePaimonCatalog(mockPaimonCatalog);
    Assertions.assertDoesNotThrow(
        () -> cat.invalidateNativeTableCache(new ObjectPath("db", "tbl")));
  }

  /**
   * Verifies that {@link BaseCatalog#getTable(ObjectPath)} preserves hook-thrown CatalogException.
   */
  @Test
  public void testGetTablePreservesCatalogException() {
    TestableBaseCatalog baseCatalog = new TestableBaseCatalog();
    Catalog mockCatalog = baseCatalog.catalog();
    TableCatalog mockTableCatalog = mock(TableCatalog.class);
    when(mockCatalog.asTableCatalog()).thenReturn(mockTableCatalog);
    when(mockTableCatalog.loadTable(any())).thenReturn(mock(Table.class));
    baseCatalog.toFlinkTableResult = mock(CatalogTable.class);
    CatalogException expected = new CatalogException("boom");
    baseCatalog.toFlinkTableException = expected;

    CatalogException actual =
        Assertions.assertThrows(
            CatalogException.class, () -> baseCatalog.getTable(new ObjectPath("db", "tbl")));

    Assertions.assertSame(expected, actual, "CatalogException should be rethrown as-is");
  }

  /** Verifies that successful Paimon alterTable invalidates the native cache. */
  @Test
  public void testAlterTableInvalidatesNativeCacheAfterSuccessfulAlter() throws Exception {
    org.apache.paimon.catalog.Catalog mockInnerCatalog =
        mock(org.apache.paimon.catalog.Catalog.class);
    FlinkCatalog mockFlinkCatalog = mock(FlinkCatalog.class);
    when(mockFlinkCatalog.catalog()).thenReturn(mockInnerCatalog);

    Catalog mockCatalog = mock(Catalog.class);
    TableCatalog mockTableCatalog = mock(TableCatalog.class);
    when(mockCatalog.asTableCatalog()).thenReturn(mockTableCatalog);
    when(mockTableCatalog.loadTable(any())).thenReturn(mock(Table.class));

    TestablePaimonCatalog cat = new TestablePaimonCatalog(mockFlinkCatalog, mockCatalog);
    ObjectPath path = new ObjectPath("mydb", "mytable");
    CatalogTable newTable = mock(CatalogTable.class);
    when(newTable.getTableKind()).thenReturn(CatalogBaseTable.TableKind.TABLE);
    when(mockFlinkCatalog.getTable(path)).thenReturn(newTable);

    cat.alterTable(path, newTable, false);

    verify(mockTableCatalog).alterTable(any(), any());
    verify(mockInnerCatalog).invalidateTable(Identifier.create("mydb", "mytable"));
  }

  /** Verifies that alterTable is a no-op when ignoreIfNotExists is true. */
  @Test
  public void testAlterTableIgnoreIfNotExistsIsNoOp() throws Exception {
    Catalog mockCatalog = mock(Catalog.class);
    TableCatalog mockTableCatalog = mock(TableCatalog.class);
    when(mockCatalog.asTableCatalog()).thenReturn(mockTableCatalog);
    when(mockTableCatalog.tableExists(any())).thenReturn(false);

    TestablePaimonCatalog cat = new TestablePaimonCatalog(mockPaimonCatalog, mockCatalog);
    CatalogTable newTable = mock(CatalogTable.class);

    cat.alterTable(new ObjectPath("missing_db", "missing_table"), newTable, true);

    verify(mockTableCatalog, never()).loadTable(any());
    verify(mockTableCatalog, never()).alterTable(any(), any());
    verify(mockPaimonCatalog, never()).getTable(any());
  }

  /** Verifies that alterTable with tableChanges is a no-op when ignoreIfNotExists is true. */
  @Test
  public void testAlterTableWithTableChangesIgnoreIfNotExistsIsNoOp() throws Exception {
    Catalog mockCatalog = mock(Catalog.class);
    TableCatalog mockTableCatalog = mock(TableCatalog.class);
    when(mockCatalog.asTableCatalog()).thenReturn(mockTableCatalog);
    when(mockTableCatalog.tableExists(any())).thenReturn(false);

    TestablePaimonCatalog cat = new TestablePaimonCatalog(mockPaimonCatalog, mockCatalog);
    CatalogTable newTable = mock(CatalogTable.class);

    cat.alterTable(new ObjectPath("missing_db", "missing_table"), newTable, List.of(), true);

    verify(mockTableCatalog, never()).loadTable(any());
    verify(mockTableCatalog, never()).alterTable(any(), any());
    verify(mockPaimonCatalog, never()).getTable(any());
  }

  /** Verifies that successful Paimon dropTable invalidates the native cache. */
  @Test
  public void testDropTableInvalidatesNativeCacheAfterSuccessfulPurge() throws Exception {
    org.apache.paimon.catalog.Catalog mockInnerCatalog =
        mock(org.apache.paimon.catalog.Catalog.class);
    FlinkCatalog mockFlinkCatalog = mock(FlinkCatalog.class);
    when(mockFlinkCatalog.catalog()).thenReturn(mockInnerCatalog);

    Catalog mockCatalog = mock(Catalog.class);
    TableCatalog mockTableCatalog = mock(TableCatalog.class);
    when(mockCatalog.asTableCatalog()).thenReturn(mockTableCatalog);
    when(mockTableCatalog.purgeTable(any())).thenReturn(true);

    TestablePaimonCatalog cat = new TestablePaimonCatalog(mockFlinkCatalog, mockCatalog);
    ObjectPath path = new ObjectPath("mydb", "mytable");
    cat.dropTable(path, false);

    Identifier expected = Identifier.create("mydb", "mytable");
    verify(mockInnerCatalog).invalidateTable(expected);
  }

  // ---------------------------------------------------------------------------
  // Helper: minimal CatalogFactory.Context implementation for constructor
  // ---------------------------------------------------------------------------

  private static class MockCatalogContext implements CatalogFactory.Context {
    private final String name;
    private final Map<String, String> options;

    MockCatalogContext(String name, Map<String, String> options) {
      this.name = name;
      this.options = options;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public Map<String, String> getOptions() {
      return options;
    }

    @Override
    public ReadableConfig getConfiguration() {
      return Configuration.fromMap(options);
    }

    @Override
    public ClassLoader getClassLoader() {
      return Thread.currentThread().getContextClassLoader();
    }
  }
}
