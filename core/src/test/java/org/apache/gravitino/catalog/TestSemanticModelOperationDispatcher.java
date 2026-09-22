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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.secret.SecretManager;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.IdGenerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestSemanticModelOperationDispatcher {

  private static final String METALAKE = "metalake";
  private static final NameIdentifier CATALOG_IDENT = NameIdentifier.of(METALAKE, "catalog");
  private static final Namespace NAMESPACE = Namespace.of(METALAKE, "catalog", "schema");
  private static final NameIdentifier SCHEMA_IDENT = NameIdentifier.of(NAMESPACE.levels());
  private static final NameIdentifier MODEL_IDENT = NameIdentifier.of(NAMESPACE, "sales_model");

  private CatalogManager catalogManager;
  private SchemaDispatcher schemaDispatcher;
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

    Catalog catalog = mock(Catalog.class);
    when(catalog.type()).thenReturn(Catalog.Type.RELATIONAL);
    when(catalogManager.loadCatalog(CATALOG_IDENT)).thenReturn(catalog);
    when(schemaDispatcher.loadSchema(SCHEMA_IDENT)).thenReturn(mock(Schema.class));
    when(schemaDispatcher.schemaExists(SCHEMA_IDENT)).thenReturn(true);

    dispatcher =
        new SemanticModelOperationDispatcher(
            catalogManager,
            schemaDispatcher,
            mock(EntityStore.class),
            mock(IdGenerator.class),
            mock(SecretManager.class));
  }

  @Test
  public void testFrameworkValidatesParentAndDelegatesToManagedOperations() {
    UnsupportedOperationException listFailure =
        assertThrows(
            UnsupportedOperationException.class, () -> dispatcher.listSemanticModels(NAMESPACE));
    assertTrue(listFailure.getMessage().startsWith("listSemanticModels:"));

    UnsupportedOperationException createFailure =
        assertThrows(
            UnsupportedOperationException.class,
            () -> dispatcher.createSemanticModel(MODEL_IDENT, null, definition(), Map.of()));
    assertTrue(createFailure.getMessage().startsWith("createSemanticModel:"));

    UnsupportedOperationException loadFailure =
        assertThrows(
            UnsupportedOperationException.class, () -> dispatcher.loadSemanticModel(MODEL_IDENT));
    assertTrue(loadFailure.getMessage().startsWith("loadSemanticModel:"));

    UnsupportedOperationException alterFailure =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                dispatcher.alterSemanticModel(
                    MODEL_IDENT, SemanticModelChange.updateComment("updated")));
    assertTrue(alterFailure.getMessage().startsWith("alterSemanticModel:"));

    UnsupportedOperationException dropFailure =
        assertThrows(
            UnsupportedOperationException.class, () -> dispatcher.dropSemanticModel(MODEL_IDENT));
    assertTrue(dropFailure.getMessage().startsWith("dropSemanticModel:"));

    verify(schemaDispatcher, times(2)).loadSchema(SCHEMA_IDENT);
    verify(schemaDispatcher, times(3)).schemaExists(SCHEMA_IDENT);
  }

  @Test
  public void testMissingSchemaPreservesTypedResults() {
    when(schemaDispatcher.loadSchema(SCHEMA_IDENT))
        .thenThrow(new NoSuchSchemaException("Schema does not exist"));
    when(schemaDispatcher.schemaExists(SCHEMA_IDENT)).thenReturn(false);

    assertThrows(NoSuchSchemaException.class, () -> dispatcher.listSemanticModels(NAMESPACE));
    assertThrows(
        NoSuchSchemaException.class,
        () -> dispatcher.createSemanticModel(MODEL_IDENT, null, definition(), Map.of()));
    assertThrows(
        NoSuchSemanticModelException.class, () -> dispatcher.loadSemanticModel(MODEL_IDENT));
    assertThrows(
        NoSuchSemanticModelException.class,
        () ->
            dispatcher.alterSemanticModel(
                MODEL_IDENT, SemanticModelChange.updateComment("updated")));
    assertFalse(dispatcher.dropSemanticModel(MODEL_IDENT));
  }

  @Test
  public void testNonRelationalCatalogIsRejectedBeforeSchemaLookup() {
    Catalog catalog = mock(Catalog.class);
    when(catalog.type()).thenReturn(Catalog.Type.FILESET);
    when(catalogManager.loadCatalog(CATALOG_IDENT)).thenReturn(catalog);

    UnsupportedOperationException failure =
        assertThrows(
            UnsupportedOperationException.class, () -> dispatcher.listSemanticModels(NAMESPACE));

    assertTrue(failure.getMessage().contains("does not support Semantic Model operations"));
    verify(schemaDispatcher, never()).loadSchema(SCHEMA_IDENT);
  }

  @Test
  public void testInvalidInputsAreRejectedBeforeCatalogLookup() {
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.createSemanticModel(MODEL_IDENT, null, null, Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.createSemanticModel(MODEL_IDENT, null, definition(), null));
    verify(catalogManager, never()).loadCatalog(CATALOG_IDENT);
  }

  private static SemanticModelDefinition definition() {
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .build();
    return SemanticModelDefinition.builder().withDatasets(new Dataset[] {dataset}).build();
  }
}
