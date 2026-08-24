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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.lock.LockManager;
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
  private static final Namespace NAMESPACE =
      Namespace.of(METALAKE, "metadata_catalog", "semantic_schema");
  private static final NameIdentifier SCHEMA_IDENT = NameIdentifier.of(NAMESPACE.levels());
  private static final NameIdentifier MODEL_IDENT = NameIdentifier.of(NAMESPACE, "sales_model");

  private CatalogManager catalogManager;
  private SchemaDispatcher schemaDispatcher;
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
    store = new InMemoryEntityStore();

    Catalog catalog = mock(Catalog.class);
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(catalogManager.loadCatalog(METADATA_CATALOG_IDENT)).thenReturn(catalog);
    when(schemaDispatcher.loadSchema(SCHEMA_IDENT)).thenReturn(mock(Schema.class));
    when(schemaDispatcher.schemaExists(SCHEMA_IDENT)).thenReturn(true);

    dispatcher =
        new SemanticModelOperationDispatcher(
            catalogManager,
            schemaDispatcher,
            store,
            new RandomIdGenerator(),
            mock(SecretManager.class));
  }

  @Test
  public void testCreateThenLoadWithoutCatalogBackedSourceValidation() {
    SemanticModel created =
        dispatcher.createSemanticModel(MODEL_IDENT, "Sales", validDefinition(), Map.of());

    assertEquals("sales_model", created.name());
    assertEquals(2, created.definition().datasets().length);
    assertSame(created, dispatcher.loadSemanticModel(MODEL_IDENT));
  }

  @Test
  public void testSchemaFailureRemainsTyped() {
    when(schemaDispatcher.loadSchema(SCHEMA_IDENT))
        .thenThrow(new NoSuchSchemaException("Schema does not exist"));
    assertThrows(
        NoSuchSchemaException.class,
        () -> dispatcher.createSemanticModel(MODEL_IDENT, null, validDefinition(), Map.of()));
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
  public void testListAlterAndDropLifecycleWithDefinitionValidation() {
    dispatcher.createSemanticModel(MODEL_IDENT, "Original", validDefinition(), Map.of());

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

    SemanticModel replaced =
        dispatcher.alterSemanticModel(
            renamedIdent, SemanticModelChange.replaceDefinition(validDefinition()));
    assertEquals(2, replaced.definition().datasets().length);
    assertArrayEquals(
        new NameIdentifier[] {renamedIdent}, dispatcher.listSemanticModels(NAMESPACE));
    assertTrue(dispatcher.dropSemanticModel(renamedIdent));
    assertFalse(dispatcher.dropSemanticModel(renamedIdent));
  }

  @Test
  public void testRejectedDefinitionReplacementDoesNotPersistOtherChanges() {
    SemanticModel original =
        dispatcher.createSemanticModel(
            MODEL_IDENT, "Original", validDefinition(), Map.of("owner", "sales"));

    SemanticModelDefinition invalidReplacement =
        SemanticModelDefinition.builder()
            .withDatasets(
                new Dataset[] {
                  dataset("duplicate", "orders", null, null),
                  dataset("duplicate", "customers", null, null)
                })
            .withRelationships(new Relationship[0])
            .build();

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

  private static Dataset dataset(
      String name, String source, String[] primaryKey, String[][] uniqueKeys) {
    return Dataset.builder()
        .withName(name)
        .withSource(NameIdentifier.of("sales", "mart", source))
        .withPrimaryKey(primaryKey)
        .withUniqueKeys(uniqueKeys)
        .build();
  }
}
