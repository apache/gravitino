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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cache.NoOpsCache;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Verifies managed create and load through a real relational persistence backend. */
public class TestManagedSemanticModelOperationsJDBC extends TestJDBCBackend {

  private final AtomicInteger writeValidationCount = new AtomicInteger();

  private Config previousConfig;
  private NameIdentifier modelIdent;
  private ManagedSemanticModelOperations operations;

  @BeforeAll
  public void captureEnvironmentConfig() {
    previousConfig = GravitinoEnv.getInstance().config();
  }

  @BeforeEach
  public void prepareManagedOperations() throws IOException, IllegalAccessException {
    writeValidationCount.set(0);
    String metalake = "managed_semantic_model_metalake";
    String catalog = "managed_semantic_model_catalog";
    String schema = "managed_semantic_model_schema";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, catalog);
    createAndInsertSchema(metalake, catalog, schema);

    Namespace namespace = NamespaceUtil.ofSemanticModel(metalake, catalog, schema);
    modelIdent = NameIdentifier.of(namespace, "sales_model");

    Config config = new Config(false) {};
    config.set(Configs.CACHE_ENABLED, false);
    RelationalEntityStore store = new RelationalEntityStore();
    FieldUtils.writeField(store, "backend", backend, true);
    FieldUtils.writeField(store, "cache", new NoOpsCache(config), true);
    operations =
        new ManagedSemanticModelOperations(
            store,
            RandomIdGenerator.INSTANCE,
            (ident, definition) -> writeValidationCount.incrementAndGet());
  }

  @AfterEach
  public void restoreEnvironmentConfig() throws IllegalAccessException {
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previousConfig, true);
  }

  @TestTemplate
  public void testCreateThenLoadRoundTrip() throws IOException {
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("source_catalog", "source_schema", "orders"))
            .withPrimaryKey(new String[0])
            .withUniqueKeys(new String[0][])
            .withCustomExtensions(new CustomExtension[0])
            .build();
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(new Dataset[] {dataset})
            .withRelationships(new Relationship[0])
            .withMetrics((Metric[]) null)
            .withCustomExtensions(new CustomExtension[0])
            .build();

    SemanticModel created =
        operations.createSemanticModel(
            modelIdent, "Persisted model", definition, Map.of("domain", "sales"));
    SemanticModel loaded = operations.loadSemanticModel(modelIdent);

    assertNotSame(created, loaded);
    assertEquals(created, loaded);
    assertEquals(definition, loaded.definition());
    assertEquals(1, writeValidationCount.get());
    assertTrue(backend.exists(modelIdent, Entity.EntityType.SEMANTIC_MODEL));
  }
}
