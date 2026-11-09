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

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestCapabilityHelpers {

  private static final Capability UPPERCASE_CAPABILITY =
      new Capability() {
        @Override
        public CapabilityResult caseSensitiveOnName(Scope scope) {
          return CapabilityResult.unsupported("not case sensitive");
        }

        @Override
        public String normalizeName(Scope scope, String name) {
          return name.toUpperCase(Locale.ROOT);
        }
      };

  private static final Capability NULL_NORMALIZING_CAPABILITY =
      new Capability() {
        @Override
        public String normalizeName(Scope scope, String name) {
          return null;
        }
      };

  /**
   * Mimics Oracle's quoting convention: a quoted name (e.g. {@code "My Table"}) is unquoted with
   * its case and embedded spaces preserved, while an unquoted name must match a simple word
   * pattern. {@code specificationOnName} must see the name as it was originally supplied (still
   * quoted) rather than the already-unquoted result of {@code normalizeName}, or a quoted name with
   * a space would be wrongly rejected after normalization even though it was valid as supplied.
   */
  static final Capability QUOTE_AWARE_CAPABILITY =
      new Capability() {
        @Override
        public CapabilityResult specificationOnName(Scope scope, String name) {
          if (name.startsWith("\"") && name.endsWith("\"") && name.length() >= 2) {
            return CapabilityResult.SUPPORTED;
          }
          return name.matches("^\\w+$")
              ? CapabilityResult.SUPPORTED
              : CapabilityResult.unsupported("Illegal name: " + name);
        }

        @Override
        public CapabilityResult caseSensitiveOnName(Scope scope) {
          return CapabilityResult.unsupported("folding depends on quoting");
        }

        @Override
        public String normalizeName(Scope scope, String name) {
          if (name.startsWith("\"") && name.endsWith("\"") && name.length() >= 2) {
            return name.substring(1, name.length() - 1);
          }
          return name.toUpperCase(Locale.ROOT);
        }
      };

  @Test
  void testNewColumnDoesNotUseExistingNameRules() {
    Capability capability =
        new Capability() {
          @Override
          public CapabilityResult specificationOnExistingName(Scope scope, String name) {
            throw new AssertionError("New columns must use creation rules");
          }
        };
    TableChange[] changes =
        CapabilityHelpers.applyCapabilities(
            capability,
            TableChange.addColumn(new String[] {"new_column"}, Types.IntegerType.get()));
    Assertions.assertEquals(1, changes.length);
  }

  /** Checks existing references and new names use their respective provider rules. */
  @Test
  public void testExistingColumnReferencesDoNotUseCreationPattern() {
    Capability strict =
        new Capability() {
          @Override
          public CapabilityResult specificationOnName(Scope scope, String name) {
            return name.matches("[a-zA-Z_][a-zA-Z0-9_]*")
                ? CapabilityResult.SUPPORTED
                : CapabilityResult.unsupported("invalid new name");
          }

          @Override
          public CapabilityResult specificationOnExistingName(Scope scope, String name) {
            return CapabilityResult.SUPPORTED;
          }
        };
    TableChange[] changes =
        CapabilityHelpers.applyCapabilities(
            strict,
            TableChange.updateColumnComment(new String[] {"native col"}, "comment"),
            TableChange.renameColumn(new String[] {"native col"}, "valid_name"),
            TableChange.deleteColumn(new String[] {"native col"}, false));
    Assertions.assertEquals(3, changes.length);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            CapabilityHelpers.applyCapabilities(
                strict, TableChange.addColumn(new String[] {"new col"}, Types.IntegerType.get())));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            CapabilityHelpers.applyCapabilities(
                strict, TableChange.renameColumn(new String[] {"native col"}, "new col")));
  }

  @Test
  void testApplyCaseSensitiveOnNameHonorsCustomNormalizeName() {
    String normalized =
        CapabilityHelpers.applyCaseSensitiveOnName(
            Capability.Scope.TABLE, "myTable", UPPERCASE_CAPABILITY);
    Assertions.assertEquals("MYTABLE", normalized);
  }

  @Test
  void testApplyCaseSensitivePartitionHonorsCustomNormalizeName() {
    IdentityPartition partition =
        Partitions.identity(
            "myPartition",
            new String[][] {{"col1"}},
            new Literal<?>[] {Literals.stringLiteral("val1")},
            null);

    Partition result = CapabilityHelpers.applyCaseSensitive(partition, UPPERCASE_CAPABILITY);
    Assertions.assertEquals("MYPARTITION", result.name());
  }

  @Test
  void testApplyCaseSensitiveOnNameRejectsNullNormalizeNameResult() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            CapabilityHelpers.applyCaseSensitiveOnName(
                Capability.Scope.TABLE, "myTable", NULL_NORMALIZING_CAPABILITY));
  }

  @Test
  void testApplyCaseSensitiveOnNameAllowsNullNameToPassThrough() {
    // A null name (e.g. an auto-generated identity partition name yet to be assigned) must be
    // allowed to pass through as null rather than being rejected as an invalid normalizeName
    // result.
    String normalized =
        CapabilityHelpers.applyCaseSensitiveOnName(
            Capability.Scope.PARTITION, null, Capability.DEFAULT);
    Assertions.assertNull(normalized);
  }

  @Test
  void testApplyCapabilitiesValidatesNameBeforeNormalizing() {
    // A quoted name with an embedded space is valid as supplied, but its normalized (unquoted)
    // form no longer matches the plain-word pattern. specificationOnName must be checked against
    // the original name, not the already-normalized one, or this would be wrongly rejected.
    NameIdentifier quotedIdent =
        NameIdentifier.of(Namespace.of("metalake", "catalog", "schema"), "\"My Table\"");

    NameIdentifier result =
        CapabilityHelpers.applyCapabilities(
            quotedIdent, Capability.Scope.TABLE, QUOTE_AWARE_CAPABILITY);

    Assertions.assertEquals("My Table", result.name());
  }

  @Test
  void testGetCapabilityPropagatesNoSuchCatalogException() {
    NameIdentifier tableIdent =
        NameIdentifier.of(Namespace.of("metalake", "catalog", "schema"), "table");
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    Mockito.when(catalogManager.doWithCatalog(Mockito.any(), Mockito.any()))
        .thenThrow(new NoSuchCatalogException("Catalog %s does not exist", tableIdent));

    // A missing catalog must stay a NoSuchCatalogException (mapped to 404 by the REST layer)
    // instead of being wrapped into a plain RuntimeException (a 500).
    Assertions.assertThrows(
        NoSuchCatalogException.class,
        () -> CapabilityHelpers.getCapability(tableIdent, catalogManager));
  }

  @Test
  void testGetCapabilityWrapsCapabilityFailure() throws Exception {
    NameIdentifier tableIdent =
        NameIdentifier.of(Namespace.of("metalake", "catalog", "schema"), "table");
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    Mockito.when(catalog.capability()).thenThrow(new IllegalStateException("boom"));

    RuntimeException e =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> CapabilityHelpers.getCapability(tableIdent, catalogManager));

    Assertions.assertInstanceOf(IllegalStateException.class, e.getCause());
  }

  @Test
  void testSemanticModelUsesSchemaNamespaceCapabilities() {
    Namespace namespace = Namespace.of("metalake", "catalog", "mixedSchema");

    Assertions.assertEquals(
        Namespace.of("metalake", "catalog", "MIXEDSCHEMA"),
        CapabilityHelpers.applyCapabilities(
            namespace, Capability.Scope.SEMANTIC_MODEL, UPPERCASE_CAPABILITY));
    Assertions.assertEquals(
        Namespace.of("metalake", "catalog", "MIXEDSCHEMA"),
        CapabilityHelpers.applyCaseSensitive(
            namespace, Capability.Scope.SEMANTIC_MODEL, UPPERCASE_CAPABILITY));
  }

  @Test
  void testModelUsesSchemaNamespaceCapabilities() {
    Namespace namespace = Namespace.of("metalake", "catalog", "mixedSchema");

    Assertions.assertEquals(
        Namespace.of("metalake", "catalog", "MIXEDSCHEMA"),
        CapabilityHelpers.applyCapabilities(
            namespace, Capability.Scope.MODEL, UPPERCASE_CAPABILITY));
    Assertions.assertEquals(
        Namespace.of("metalake", "catalog", "MIXEDSCHEMA"),
        CapabilityHelpers.applyCaseSensitive(
            namespace, Capability.Scope.MODEL, UPPERCASE_CAPABILITY));
  }

  @Test
  void testSemanticModelUsesManagedStorageByDefault() {
    Assertions.assertTrue(
        Capability.DEFAULT.managedStorage(Capability.Scope.SEMANTIC_MODEL).supported());
  }

  @Test
  void testApplyCapabilitiesRejectsInitialDefaultEvenWhenWriteDefaultSupported() {
    // Capability.DEFAULT supports write-default (like MySQL), but catalogs do not apply
    // Iceberg-style initial defaults. Create must reject a set initialDefaultValue rather than
    // accept a value the backend would silently drop.
    Assertions.assertTrue(Capability.DEFAULT.columnDefaultValue().supported());

    Column withInitialOnly =
        Column.of(
            "country",
            Types.StringType.get(),
            null,
            true,
            false,
            Column.DEFAULT_VALUE_NOT_SET,
            Literals.stringLiteral("US"));

    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                CapabilityHelpers.applyCapabilities(
                    new Column[] {withInitialOnly}, Capability.DEFAULT));
    Assertions.assertTrue(
        thrown.getMessage().contains("initial default value is read-only"), thrown.getMessage());
    Assertions.assertTrue(thrown.getMessage().contains("catalog capability"), thrown.getMessage());
    Assertions.assertTrue(thrown.getMessage().contains("country"), thrown.getMessage());

    Column unsetInitial =
        Column.of(
            "country",
            Types.StringType.get(),
            null,
            true,
            false,
            Literals.stringLiteral("US"),
            Column.INITIAL_DEFAULT_VALUE_NOT_SET);
    Column[] applied =
        CapabilityHelpers.applyCapabilities(new Column[] {unsetInitial}, Capability.DEFAULT);
    Assertions.assertEquals(1, applied.length);
    Assertions.assertEquals(Column.INITIAL_DEFAULT_VALUE_NOT_SET, applied[0].initialDefaultValue());
    Assertions.assertEquals(Literals.stringLiteral("US"), applied[0].defaultValue());
  }

  @Test
  void testApplyCapabilitiesTreatsNullInitialDefaultAsUnset() {
    // Third-party Column implementations may return null instead of the unset sentinel.
    Column nullInitial = Mockito.mock(Column.class);
    Mockito.when(nullInitial.name()).thenReturn("country");
    Mockito.when(nullInitial.dataType()).thenReturn(Types.StringType.get());
    Mockito.when(nullInitial.comment()).thenReturn(null);
    Mockito.when(nullInitial.nullable()).thenReturn(true);
    Mockito.when(nullInitial.autoIncrement()).thenReturn(false);
    Mockito.when(nullInitial.defaultValue()).thenReturn(Column.DEFAULT_VALUE_NOT_SET);
    Mockito.when(nullInitial.initialDefaultValue()).thenReturn(null);

    Column[] applied =
        CapabilityHelpers.applyCapabilities(new Column[] {nullInitial}, Capability.DEFAULT);
    Assertions.assertEquals(1, applied.length);
    Assertions.assertEquals(Column.INITIAL_DEFAULT_VALUE_NOT_SET, applied[0].initialDefaultValue());
  }

  @Test
  void testLoadThenCreateRejectsColumnsCarryingInitialDefault() {
    // Iceberg v3 load exposes write-default and initial-default on columns. Feeding those columns
    // straight into create/add-column (copy-table / load-then-create) must fail: Gravitino create
    // still rejects initial defaults, and Iceberg create still rejects write defaults via
    // capability for catalogs that do not support them. Document the failure rather than silently
    // dropping.
    Column loadedLikeIcebergV3 =
        Column.of(
            "region",
            Types.StringType.get(),
            "loaded from Iceberg",
            true,
            false,
            Literals.stringLiteral("EU"),
            Literals.stringLiteral("US"));

    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                CapabilityHelpers.applyCapabilities(
                    new Column[] {loadedLikeIcebergV3}, Capability.DEFAULT));
    Assertions.assertTrue(
        thrown.getMessage().contains("initial default value is read-only"), thrown.getMessage());
    Assertions.assertTrue(thrown.getMessage().contains("catalog capability"), thrown.getMessage());
    Assertions.assertTrue(thrown.getMessage().contains("region"), thrown.getMessage());
  }

  @Test
  void testNormalizeMetadataIdentifierKeepsExactStoredColumnName() throws IOException {
    CatalogManager catalogManager = foldingCatalogManager();
    EntityStore store = Mockito.mock(EntityStore.class);
    NameIdentifier table = NameIdentifier.of("metalake", "catalog", "schema", "orders");
    TableEntity tableEntity = Mockito.mock(TableEntity.class);
    ColumnEntity column = Mockito.mock(ColumnEntity.class);
    Mockito.when(column.name()).thenReturn("MyCol");
    Mockito.when(tableEntity.columns()).thenReturn(Collections.singletonList(column));
    Mockito.when(store.get(table, Entity.EntityType.TABLE, TableEntity.class))
        .thenReturn(tableEntity);

    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "orders", "MyCol"),
        CapabilityHelpers.normalizeMetadataIdentifier(
            NameIdentifier.of("metalake", "catalog", "SCHEMA", "ORDERS", "MyCol"),
            Capability.Scope.COLUMN,
            catalogManager,
            () -> store));
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "orders", "othercol"),
        CapabilityHelpers.normalizeMetadataIdentifier(
            NameIdentifier.of("metalake", "catalog", "SCHEMA", "ORDERS", "OtherCol"),
            Capability.Scope.COLUMN,
            catalogManager,
            () -> store));
    Mockito.verify(store, Mockito.times(2)).get(table, Entity.EntityType.TABLE, TableEntity.class);

    // A name that folding does not change needs no stored-column read.
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "orders", "mycol"),
        CapabilityHelpers.normalizeMetadataIdentifier(
            NameIdentifier.of("metalake", "catalog", "SCHEMA", "ORDERS", "mycol"),
            Capability.Scope.COLUMN,
            catalogManager,
            () -> store));
    Mockito.verify(store, Mockito.times(2)).get(table, Entity.EntityType.TABLE, TableEntity.class);
  }

  @Test
  void testNormalizeMetadataIdentifierHandlesMissingOrFailedStoredTable() throws IOException {
    CatalogManager catalogManager = foldingCatalogManager();
    EntityStore store = Mockito.mock(EntityStore.class);
    NameIdentifier table = NameIdentifier.of("metalake", "catalog", "schema", "orders");
    Mockito.when(store.get(table, Entity.EntityType.TABLE, TableEntity.class))
        .thenThrow(new NoSuchEntityException("missing"));
    NameIdentifier column = NameIdentifier.of("metalake", "catalog", "schema", "ORDERS", "MyCol");
    Assertions.assertEquals(
        NameIdentifier.of("metalake", "catalog", "schema", "orders", "mycol"),
        CapabilityHelpers.normalizeMetadataIdentifier(
            column, Capability.Scope.COLUMN, catalogManager, () -> store));

    IOException failure = new IOException("store down");
    Mockito.reset(store);
    Mockito.when(store.get(table, Entity.EntityType.TABLE, TableEntity.class)).thenThrow(failure);
    RuntimeException error =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                CapabilityHelpers.normalizeMetadataIdentifier(
                    column, Capability.Scope.COLUMN, catalogManager, () -> store));
    Assertions.assertSame(failure, error.getCause());
    Assertions.assertTrue(error.getMessage().contains(table.toString()));
  }

  private static CatalogManager foldingCatalogManager() {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    Mockito.when(catalog.capability())
        .thenReturn(
            new Capability() {
              @Override
              public CapabilityResult caseSensitiveOnName(Scope scope) {
                return CapabilityResult.unsupported("case insensitive");
              }
            });
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    return catalogManager;
  }
}
