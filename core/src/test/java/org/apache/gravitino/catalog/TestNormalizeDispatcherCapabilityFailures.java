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

import java.util.stream.Stream;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.capability.Capability;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/** Verifies capability failures at each migrated normalization dispatcher boundary. */
class TestNormalizeDispatcherCapabilityFailures {

  private static final NameIdentifier IDENT =
      NameIdentifier.of("metalake", "catalog", "schema", "object");

  @ParameterizedTest(name = "{0}")
  @MethodSource("dispatchers")
  void testInvalidNameExceptionReachesCaller(
      String name, Object backend, ThrowingConsumer<CatalogManager> operation) {
    CatalogManager catalogManager = Mockito.mock(CatalogManager.class);
    BaseCatalog<?> catalog = Mockito.mock(BaseCatalog.class);
    CatalogTestUtils.mockDoWithCatalog(catalogManager, catalog);
    IllegalArgumentException invalidName = new IllegalArgumentException("Invalid name");
    Capability capability =
        new Capability() {
          @Override
          public String normalizeName(Scope scope, String value) {
            throw invalidName;
          }
        };
    Mockito.when(catalog.capability()).thenReturn(capability);

    IllegalArgumentException thrown =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> operation.accept(catalogManager), name);

    Assertions.assertSame(invalidName, thrown, name);
    Mockito.verify(catalog).capability();
    Mockito.verifyNoInteractions(backend);
  }

  private static Stream<Arguments> dispatchers() {
    TableDispatcher tables = Mockito.mock(TableDispatcher.class);
    PartitionDispatcher partitions = Mockito.mock(PartitionDispatcher.class);
    FilesetDispatcher filesets = Mockito.mock(FilesetDispatcher.class);
    ViewDispatcher views = Mockito.mock(ViewDispatcher.class);
    TopicDispatcher topics = Mockito.mock(TopicDispatcher.class);
    ModelDispatcher models = Mockito.mock(ModelDispatcher.class);
    FunctionDispatcher functions = Mockito.mock(FunctionDispatcher.class);
    SchemaDispatcher schemas = Mockito.mock(SchemaDispatcher.class);
    SemanticModelDispatcher semanticModels = Mockito.mock(SemanticModelDispatcher.class);
    return Stream.of(
        dispatcher(
            "table",
            tables,
            manager -> new TableNormalizeDispatcher(tables, manager).loadTable(IDENT)),
        dispatcher(
            "partition",
            partitions,
            manager ->
                new PartitionNormalizeDispatcher(partitions, manager).getPartition(IDENT, "p")),
        dispatcher(
            "fileset",
            filesets,
            manager -> new FilesetNormalizeDispatcher(filesets, manager).loadFileset(IDENT)),
        dispatcher(
            "view", views, manager -> new ViewNormalizeDispatcher(views, manager).loadView(IDENT)),
        dispatcher(
            "topic",
            topics,
            manager -> new TopicNormalizeDispatcher(topics, manager).loadTopic(IDENT)),
        dispatcher(
            "model",
            models,
            manager -> new ModelNormalizeDispatcher(models, manager).getModel(IDENT)),
        dispatcher(
            "function",
            functions,
            manager -> new FunctionNormalizeDispatcher(functions, manager).getFunction(IDENT)),
        dispatcher(
            "schema",
            schemas,
            manager ->
                new SchemaNormalizeDispatcher(schemas, manager)
                    .loadSchema(NameIdentifier.of("metalake", "catalog", "schema"))),
        dispatcher(
            "semantic model",
            semanticModels,
            manager ->
                new SemanticModelNormalizeDispatcher(semanticModels, manager)
                    .loadSemanticModel(IDENT)));
  }

  private static Arguments dispatcher(
      String name, Object backend, ThrowingConsumer<CatalogManager> operation) {
    return Arguments.of(name, backend, operation);
  }
}
