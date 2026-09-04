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

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.connector.capability.CapabilityResult;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.memory.TestMemoryEntityStore.InMemoryEntityStore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSemanticModelOperationDispatcher {

  private static final String METALAKE = "metalake";
  private static final NameIdentifier METADATA_CATALOG_IDENT =
      NameIdentifier.of(METALAKE, "metadata_catalog");
  private static final NameIdentifier SOURCE_CATALOG_IDENT = NameIdentifier.of(METALAKE, "sales");
  private static final Namespace NAMESPACE =
      Namespace.of(METALAKE, "metadata_catalog", "semantic_schema");
  private static final NameIdentifier SCHEMA_IDENT = NameIdentifier.of(NAMESPACE.levels());
  private static final NameIdentifier MODEL_IDENT = NameIdentifier.of(NAMESPACE, "sales_model");
  private static final NameIdentifier ORDERS_SOURCE =
      NameIdentifier.of(Namespace.of(METALAKE, "sales", "mart"), "orders");
  private static final NameIdentifier CUSTOMERS_SOURCE =
      NameIdentifier.of(Namespace.of(METALAKE, "sales", "mart"), "customers");

  private CatalogManager catalogManager;
  private SchemaDispatcher schemaDispatcher;
  private TableDispatcher tableDispatcher;
  private ViewDispatcher viewDispatcher;
  private InMemoryEntityStore store;
  private SemanticModelOperationDispatcher dispatcher;

  @BeforeAll
  public static void initializeLockManager() throws IllegalAccessException {
    Config config = mock(Config.class);
    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
  }

  @BeforeEach
  public void setUp() throws Exception {
    catalogManager = mock(CatalogManager.class);
    schemaDispatcher = mock(SchemaDispatcher.class);
    tableDispatcher = mock(TableDispatcher.class);
    viewDispatcher = mock(ViewDispatcher.class);
    store = new InMemoryEntityStore();

    useSourceCapability(Capability.DEFAULT);
    Catalog catalog = mock(Catalog.class);
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(catalogManager.loadCatalog(METADATA_CATALOG_IDENT)).thenReturn(catalog);
    when(schemaDispatcher.loadSchema(SCHEMA_IDENT)).thenReturn(mock(Schema.class));
    when(schemaDispatcher.schemaExists(SCHEMA_IDENT)).thenReturn(true);

    dispatcher =
        new SemanticModelOperationDispatcher(
            catalogManager,
            schemaDispatcher,
            tableDispatcher,
            viewDispatcher,
            store,
            new RandomIdGenerator(),
            mock(SecretManager.class));
  }

  @Test
  public void testCreateWithTableAndLogicalViewSourcesThenLoad() {
    doReturn(tableWithColumns("order_id", "customer_id"))
        .when(tableDispatcher)
        .loadTable(ORDERS_SOURCE);
    when(tableDispatcher.loadTable(CUSTOMERS_SOURCE))
        .thenThrow(new NoSuchTableException("Table does not exist"));
    doReturn(viewWithColumns("customer_id")).when(viewDispatcher).loadView(CUSTOMERS_SOURCE);

    SemanticModel created =
        dispatcher.createSemanticModel(MODEL_IDENT, "Sales", validDefinition(), Map.of());

    assertEquals("sales_model", created.name());
    assertEquals(2, created.definition().datasets().length);
    assertSame(created, dispatcher.loadSemanticModel(MODEL_IDENT));
    verify(tableDispatcher).loadTable(ORDERS_SOURCE);
    verify(tableDispatcher).loadTable(CUSTOMERS_SOURCE);
    verify(viewDispatcher).loadView(CUSTOMERS_SOURCE);
  }

  @Test
  public void testLoadDoesNotRevalidatePersistedSources() {
    doReturn(tableWithColumns("order_id", "customer_id"))
        .when(tableDispatcher)
        .loadTable(ORDERS_SOURCE);
    when(tableDispatcher.loadTable(CUSTOMERS_SOURCE))
        .thenThrow(new NoSuchTableException("Table does not exist"));
    doReturn(viewWithColumns("customer_id")).when(viewDispatcher).loadView(CUSTOMERS_SOURCE);
    SemanticModel created =
        dispatcher.createSemanticModel(MODEL_IDENT, null, validDefinition(), Map.of());

    when(tableDispatcher.loadTable(ORDERS_SOURCE))
        .thenThrow(new ConnectionFailedException("Source is now unavailable"));
    when(viewDispatcher.loadView(CUSTOMERS_SOURCE))
        .thenThrow(new ConnectionFailedException("Source is now unavailable"));
    clearInvocations(tableDispatcher, viewDispatcher);

    assertSame(created, dispatcher.loadSemanticModel(MODEL_IDENT));
    verifyNoInteractions(tableDispatcher, viewDispatcher);
  }

  @Test
  public void testPrimaryUniqueAndRelationshipColumnsAreValidated() {
    doReturn(tableWithColumns("order_id", "customer_id"))
        .when(tableDispatcher)
        .loadTable(ORDERS_SOURCE);
    doReturn(tableWithColumns("customer_id")).when(tableDispatcher).loadTable(CUSTOMERS_SOURCE);

    Dataset badPrimary =
        dataset("orders", "orders", new String[] {"missing"}, new String[][] {{"customer_id"}});
    assertSourceColumnFailure(definition(badPrimary), "primaryKey[0]");

    Dataset badUnique =
        dataset("orders", "orders", new String[] {"order_id"}, new String[][] {{"missing"}});
    assertSourceColumnFailure(definition(badUnique), "uniqueKeys[0][0]");

    Relationship badRelationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(new String[] {"missing"})
            .withToColumns(new String[] {"customer_id"})
            .build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(
                new Dataset[] {
                  dataset("orders", "orders", null, null),
                  dataset("customers", "customers", null, null)
                })
            .withRelationships(new Relationship[] {badRelationship})
            .build();
    assertSourceColumnFailure(definition, "relationships[0].fromColumns[0]");
  }

  @Test
  public void testCatalogColumnCaseSensitivityIsHonored() throws Exception {
    doReturn(tableWithColumns("ORDER_ID")).when(tableDispatcher).loadTable(ORDERS_SOURCE);
    SemanticModelDefinition lowerCaseReference =
        definition(dataset("orders", "orders", new String[] {"order_id"}, null));

    IllegalSemanticModelException caseSensitiveFailure =
        assertThrows(
            IllegalSemanticModelException.class,
            () ->
                dispatcher.createSemanticModel(
                    MODEL_IDENT, null, lowerCaseReference, Map.of("mode", "sensitive")));
    assertTrue(caseSensitiveFailure.getMessage().contains("primaryKey[0]"));

    useSourceCapability(new CaseInsensitiveColumnsCapability());
    SemanticModel created =
        dispatcher.createSemanticModel(
            MODEL_IDENT, null, lowerCaseReference, Map.of("mode", "insensitive"));
    assertEquals("orders", created.definition().datasets()[0].name());
  }

  @Test
  public void testMissingSourceIsRejectedBeforePersistence() {
    NameIdentifier missingSource =
        NameIdentifier.of(Namespace.of(METALAKE, "sales", "mart"), "missing");
    when(tableDispatcher.loadTable(missingSource))
        .thenThrow(new NoSuchTableException("Table does not exist"));
    when(viewDispatcher.loadView(missingSource))
        .thenThrow(new NoSuchViewException("View does not exist"));
    SemanticModelDefinition definition =
        definition(dataset("missing", "missing", null, new String[0][]));

    IllegalSemanticModelException failure =
        assertThrows(
            IllegalSemanticModelException.class,
            () -> dispatcher.createSemanticModel(MODEL_IDENT, null, definition, Map.of()));

    assertTrue(failure.getMessage().contains("does not exist as a Table or logical View"));
    assertFalse(dispatcher.semanticModelExists(MODEL_IDENT));
  }

  @Test
  public void testSchemaAndConnectionFailuresRemainTyped() {
    when(schemaDispatcher.loadSchema(SCHEMA_IDENT))
        .thenThrow(new NoSuchSchemaException("Schema does not exist"));
    assertThrows(
        NoSuchSchemaException.class,
        () -> dispatcher.createSemanticModel(MODEL_IDENT, null, validDefinition(), Map.of()));

    doReturn(mock(Schema.class)).when(schemaDispatcher).loadSchema(SCHEMA_IDENT);
    ConnectionFailedException failure = new ConnectionFailedException("Catalog unavailable");
    when(tableDispatcher.loadTable(ORDERS_SOURCE)).thenThrow(failure);
    assertSame(
        failure,
        assertThrows(
            ConnectionFailedException.class,
            () -> dispatcher.createSemanticModel(MODEL_IDENT, null, validDefinition(), Map.of())));
    verify(viewDispatcher, never()).loadView(ORDERS_SOURCE);
    assertFalse(dispatcher.semanticModelExists(MODEL_IDENT));
  }

  @Test
  public void testNonRelationalCatalogIsRejectedBeforeSchemaLookup() {
    Catalog catalog = mock(Catalog.class);
    when(catalog.type()).thenReturn(Catalog.Type.FILESET);
    when(catalogManager.loadCatalog(METADATA_CATALOG_IDENT)).thenReturn(catalog);

    assertThrows(
        UnsupportedOperationException.class, () -> dispatcher.listSemanticModels(NAMESPACE));
    verify(schemaDispatcher, never()).loadSchema(SCHEMA_IDENT);
  }

  @Test
  public void testListAlterAndDropLifecycleWithSelectiveSourceValidation() {
    doReturn(tableWithColumns("order_id", "customer_id"))
        .when(tableDispatcher)
        .loadTable(ORDERS_SOURCE);
    when(tableDispatcher.loadTable(CUSTOMERS_SOURCE))
        .thenThrow(new NoSuchTableException("Table does not exist"));
    doReturn(viewWithColumns("customer_id")).when(viewDispatcher).loadView(CUSTOMERS_SOURCE);
    dispatcher.createSemanticModel(MODEL_IDENT, "Original", validDefinition(), Map.of());
    clearInvocations(tableDispatcher, viewDispatcher);

    SemanticModel propertyUpdated =
        dispatcher.alterSemanticModel(
            MODEL_IDENT, SemanticModelChange.setProperty("owner", "analytics"));
    SemanticModel renamed =
        dispatcher.alterSemanticModel(
            MODEL_IDENT,
            SemanticModelChange.rename("renamed_sales_model"),
            SemanticModelChange.updateComment("Updated"));
    NameIdentifier renamedIdent = NameIdentifier.of(NAMESPACE, renamed.name());
    assertEquals(Map.of("owner", "analytics"), propertyUpdated.properties());
    assertEquals("Updated", renamed.comment());
    verifyNoInteractions(tableDispatcher, viewDispatcher);

    SemanticModel replaced =
        dispatcher.alterSemanticModel(
            renamedIdent, SemanticModelChange.replaceDefinition(validDefinition()));
    assertEquals(2, replaced.definition().datasets().length);
    verify(tableDispatcher).loadTable(ORDERS_SOURCE);
    verify(tableDispatcher).loadTable(CUSTOMERS_SOURCE);
    verify(viewDispatcher).loadView(CUSTOMERS_SOURCE);
    assertArrayEquals(
        new NameIdentifier[] {renamedIdent}, dispatcher.listSemanticModels(NAMESPACE));
    assertTrue(dispatcher.dropSemanticModel(renamedIdent));
    assertFalse(dispatcher.dropSemanticModel(renamedIdent));
  }

  @Test
  public void testRejectedSourceReplacementDoesNotPersistOtherChanges() {
    doReturn(tableWithColumns("order_id", "customer_id"))
        .when(tableDispatcher)
        .loadTable(ORDERS_SOURCE);
    when(tableDispatcher.loadTable(CUSTOMERS_SOURCE))
        .thenThrow(new NoSuchTableException("Table does not exist"));
    doReturn(viewWithColumns("customer_id")).when(viewDispatcher).loadView(CUSTOMERS_SOURCE);
    SemanticModel original =
        dispatcher.createSemanticModel(
            MODEL_IDENT, "Original", validDefinition(), Map.of("owner", "sales"));

    NameIdentifier missingSource =
        NameIdentifier.of(Namespace.of(METALAKE, "sales", "mart"), "replacement_missing");
    when(tableDispatcher.loadTable(missingSource))
        .thenThrow(new NoSuchTableException("Table does not exist"));
    when(viewDispatcher.loadView(missingSource))
        .thenThrow(new NoSuchViewException("View does not exist"));
    SemanticModelDefinition invalidReplacement =
        definition(dataset("missing", "replacement_missing", null, null));

    assertThrows(
        IllegalSemanticModelException.class,
        () ->
            dispatcher.alterSemanticModel(
                MODEL_IDENT,
                SemanticModelChange.rename("must_not_persist"),
                SemanticModelChange.updateComment("Must not persist"),
                SemanticModelChange.setProperty("owner", "changed"),
                SemanticModelChange.replaceDefinition(invalidReplacement)));

    SemanticModel loaded = dispatcher.loadSemanticModel(MODEL_IDENT);
    assertEquals(original.name(), loaded.name());
    assertEquals(original.comment(), loaded.comment());
    assertEquals(original.properties(), loaded.properties());
    assertEquals(original.definition(), loaded.definition());
    assertFalse(dispatcher.semanticModelExists(NameIdentifier.of(NAMESPACE, "must_not_persist")));
  }

  @Test
  public void testLifecycleMissingParentAndInvalidChangeSemantics() {
    assertThrows(
        IllegalSemanticModelException.class,
        () -> dispatcher.alterSemanticModel(MODEL_IDENT, (SemanticModelChange[]) null));
    assertThrows(
        IllegalSemanticModelException.class, () -> dispatcher.alterSemanticModel(MODEL_IDENT));

    when(schemaDispatcher.loadSchema(SCHEMA_IDENT))
        .thenThrow(new NoSuchSchemaException("Schema does not exist"));
    assertThrows(NoSuchSchemaException.class, () -> dispatcher.listSemanticModels(NAMESPACE));
    when(schemaDispatcher.schemaExists(SCHEMA_IDENT)).thenReturn(false);
    assertThrows(
        NoSuchSemanticModelException.class,
        () ->
            dispatcher.alterSemanticModel(
                MODEL_IDENT, SemanticModelChange.updateComment("Missing")));
    assertFalse(dispatcher.dropSemanticModel(MODEL_IDENT));
  }

  @Test
  public void testNullDefinitionIsRejectedByValidator() {
    assertThrows(
        IllegalSemanticModelException.class,
        () -> dispatcher.createSemanticModel(MODEL_IDENT, null, null, Map.of()));
    assertFalse(dispatcher.semanticModelExists(MODEL_IDENT));
  }

  @Test
  public void testNullPropertiesAreRejectedBeforeCatalogLookup() {
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.createSemanticModel(MODEL_IDENT, null, validDefinition(), null));
    verify(catalogManager, never()).loadCatalog(METADATA_CATALOG_IDENT);
  }

  private void useSourceCapability(Capability capability) throws Exception {
    CatalogManager.CatalogWrapper wrapper = mock(CatalogManager.CatalogWrapper.class);
    when(wrapper.capabilities()).thenReturn(capability);
    when(catalogManager.loadCatalogAndWrap(SOURCE_CATALOG_IDENT)).thenReturn(wrapper);
  }

  private void assertSourceColumnFailure(
      SemanticModelDefinition definition, String expectedMessageFragment) {
    IllegalSemanticModelException failure =
        assertThrows(
            IllegalSemanticModelException.class,
            () -> dispatcher.createSemanticModel(MODEL_IDENT, null, definition, Map.of()));
    assertTrue(failure.getMessage().contains(expectedMessageFragment));
    assertFalse(dispatcher.semanticModelExists(MODEL_IDENT));
  }

  private static SemanticModelDefinition validDefinition() {
    Dataset orders =
        dataset("orders", "orders", new String[] {"order_id"}, new String[][] {{"customer_id"}});
    Dataset customers =
        dataset("customers", "customers", new String[] {"customer_id"}, new String[0][]);
    Relationship relationship =
        Relationship.builder()
            .withName("orders_to_customers")
            .withFrom("orders")
            .withTo("customers")
            .withFromColumns(new String[] {"customer_id"})
            .withToColumns(new String[] {"customer_id"})
            .build();
    return SemanticModelDefinition.builder()
        .withDatasets(new Dataset[] {orders, customers})
        .withRelationships(new Relationship[] {relationship})
        .build();
  }

  private static SemanticModelDefinition definition(Dataset dataset) {
    return SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build();
  }

  private static Dataset dataset(
      String name, String source, String[] primaryKey, String[][] uniqueKeys) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", source))
        .withPrimaryKey(primaryKey)
        .withUniqueKeys(uniqueKeys)
        .build();
  }

  private static Table tableWithColumns(String... names) {
    Table table = mock(Table.class);
    Column[] tableColumns = columns(names);
    when(table.columns()).thenReturn(tableColumns);
    return table;
  }

  private static View viewWithColumns(String... names) {
    View view = mock(View.class);
    Column[] viewColumns = columns(names);
    when(view.columns()).thenReturn(viewColumns);
    return view;
  }

  private static Column[] columns(String... names) {
    Column[] columns = new Column[names.length];
    for (int index = 0; index < names.length; index++) {
      columns[index] = mock(Column.class);
      when(columns[index].name()).thenReturn(names[index]);
    }
    return columns;
  }

  private static final class CaseInsensitiveColumnsCapability implements Capability {

    @Override
    public CapabilityResult caseSensitiveOnName(Scope scope) {
      if (scope == Scope.COLUMN) {
        return CapabilityResult.unsupported("Column names are case-insensitive");
      }
      return CapabilityResult.SUPPORTED;
    }
  }
}
