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

package org.apache.gravitino.cache.it;

import static org.apache.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PATH;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.apache.gravitino.Configs.VERSION_RETENTION_COUNT;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.storage.relational.converters.H2ExceptionConverter;
import org.apache.gravitino.storage.relational.converters.SQLExceptionConverterFactory;
import org.apache.gravitino.utils.RandomNameUtils;
import org.apache.gravitino.utils.TestUtil;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/**
 * Benchmark base class for evaluating {@code EntityStorage} performance using JMH.
 *
 * <p>This abstract class provides a reusable setup for benchmarking relational entity store
 * implementations. It uses an embedded H2 backend and generates randomized metadata entities (e.g.,
 * models, users, topics) to simulate realistic usage patterns. Subclasses should implement specific
 * {@code @Benchmark} methods to test operations such as {@code put}, {@code get}, {@code contains},
 * and {@code insertRelation}.
 *
 * <p>Benchmark operations include:
 *
 * <ul>
 *   <li>{@code benchmarkContains} — test for entity existence
 *   <li>{@code benchmarkGet} — test for entity retrieval
 *   <li>{@code benchmarkGetWithRelation} — test for listing entities with relations
 * </ul>
 *
 * <p>Environment:
 *
 * <ul>
 *   <li>CPU: Apple M2 Pro
 *   <li>Memory: 16 GB
 *   <li>OS: macOS Ventura 15.5
 *   <li>JVM: OpenJDK 17.0.15
 *   <li>JMH: 1.37
 *   <li>Backend: H2
 * </ul>
 *
 * <p>Benchmark configuration:
 *
 * <ul>
 *   <li>{@code @BenchmarkMode}: Throughput and AverageTime
 *   <li>{@code @State}: Scope.Thread
 *   <li>{@code @Fork}: 1
 *   <li>{@code @Warmup}: 5 iterations
 *   <li>{@code @Measurement}: 10 iterations
 *   <li>Gradle config:
 *       <pre>{@code
 * jmh {
 *   warmupIterations = 5
 *   iterations = 10
 *   fork = 1
 *   threads = 10
 *   resultFormat = "csv"
 *   resultsFile = file("$buildDir/reports/jmh/results.csv")
 * }
 *
 * }</pre>
 * </ul>
 *
 * <p>Two cache modes are benchmarked:
 *
 * <ol>
 *   <li><strong>With cache enabled</strong>: throughput up to ~10⁶ ops/sec
 *   <li><strong>With cache disabled</strong>: throughput around ~10⁴ ops/sec
 * </ol>
 *
 * <p>A mixed benchmark is also included to simulate real workloads, consisting of:
 *
 * <ul>
 *   <li>60% {@code get}
 *   <li>30% {@code listRelation}
 *   <li>10% {@code contains}
 * </ul>
 *
 * <p><strong>Note:</strong> If your machine has fewer than 10 CPU cores, benchmark results may
 * vary.
 *
 * <p>Related PR: https://github.com/apache/gravitino/issues/7546
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class AbstractEntityStorageBenchmark<E extends Entity & HasIdentifier> {
  protected static final Random random = ThreadLocalRandom.current();
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractEntityStorageBenchmark.class.getName());
  private static final String JDBC_STORE_PATH =
      "/tmp/gravitino_jdbc_entityStore_benchmark_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = JDBC_STORE_PATH + "/testdb";
  private static final String H2_FILE = DB_DIR + ".mv.db";
  private static final String BENCHMARK_METALAKE_NAME = "benchmark_metalake";
  private static final String BENCHMARK_CATALOG_NAME = "benchmark_catalog";
  private static final String BENCHMARK_SCHEMA_NAME = "benchmark_schema";
  private static final boolean CACHE_ENABLED = true;
  private static RandomIdGenerator generator = new RandomIdGenerator();
  protected List<ModelEntity> entities;
  protected Map<UserEntity, Entity> entitiesWithRelations;
  protected EntityStore store;
  protected BaseMetalake metalake;
  protected CatalogEntity catalog;
  protected SchemaEntity schema;

  @Param({"10", "100", "1000"})
  public int totalCnt;

  @Setup(Level.Trial)
  public final void init() throws IOException {
    initStore();
    initParentEntities();
    initBenchmarkEntities();
  }

  @TearDown(Level.Trial)
  public void destroy() throws IOException {
    try {
      if (store != null) {
        store.close();
      }
    } catch (Exception e) {
      LOG.warn("WARNING: Failed to close store during benchmark teardown: " + e.getMessage());
    }
    File dir = new File(DB_DIR);
    if (dir.exists()) {
      dir.delete();
    }

    FileUtils.deleteQuietly(new File(H2_FILE));
  }

  /**
   * Get test entities.
   *
   * @param entityCnt The number of entity to create.
   * @return The list of test entities.
   */
  protected List<ModelEntity> getEntities(int entityCnt) {
    List<ModelEntity> entities = new ArrayList<>(entityCnt);
    for (int i = 0; i < entityCnt; i++) {
      entities.add(
          TestUtil.getTestModelEntity(
              generator.nextId(),
              RandomNameUtils.genRandomName("model"),
              Namespace.of(
                  BENCHMARK_METALAKE_NAME, BENCHMARK_CATALOG_NAME, BENCHMARK_SCHEMA_NAME)));
    }

    return entities;
  }

  /**
   * Get relation entities.
   *
   * @param entityCnt The number of entities to create.
   * @return The map of relation entities.
   */
  protected Map<UserEntity, Entity> getRelationEntities(int entityCnt) {
    Map<UserEntity, Entity> relationEntities = Maps.newHashMap();
    for (int i = 0; i < entityCnt; i++) {
      RoleEntity testRoleEntity =
          TestUtil.getTestRoleEntity(
              generator.nextId(), RandomNameUtils.genRandomName("role"), BENCHMARK_METALAKE_NAME);
      List<Long> roleIds = ImmutableList.of(testRoleEntity.id());
      UserEntity testUserEntity =
          TestUtil.getTestUserEntity(
              generator.nextId(),
              RandomNameUtils.genRandomName("user"),
              BENCHMARK_METALAKE_NAME,
              roleIds);
      TopicEntity testTopicEntity =
          TestUtil.getTestTopicEntity(
              generator.nextId(),
              RandomNameUtils.genRandomName("topic"),
              Namespace.of(BENCHMARK_METALAKE_NAME, BENCHMARK_CATALOG_NAME, BENCHMARK_SCHEMA_NAME),
              "");

      relationEntities.put(testUserEntity, testTopicEntity);
    }

    return relationEntities;
  }

  /**
   * Validate entity from store.
   *
   * @param entity The target entity to validate.
   * @param entityFromStore The entity from store.
   * @return The validated entity. if the entity is not equal, it will throw an exception.
   */
  protected Entity validateEntity(E entity, E entityFromStore) {
    if (entity.equals(entityFromStore)) {
      return entityFromStore;
    }

    throw new RuntimeException("Entity not equal");
  }

  /**
   * Init entity store.
   *
   * @throws IOException if an I/O error occurs.
   */
  protected void initStore() throws IOException {
    File dir = new File(DB_DIR);
    if (dir.exists()) {
      FileUtils.deleteQuietly(dir);
    }
    dir.mkdirs();

    Config config = mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PATH)).thenReturn(DB_DIR);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);
    Mockito.when(config.get(STORE_DELETE_AFTER_TIME)).thenReturn(20 * 60 * 1000L);
    Mockito.when(config.get(VERSION_RETENTION_COUNT)).thenReturn(1L);
    // Fix cache config for test
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(CACHE_ENABLED);
    Mockito.when(config.get(Configs.CACHE_MAX_ENTRIES)).thenReturn(10_000);
    Mockito.when(config.get(Configs.CACHE_EXPIRATION_TIME)).thenReturn(3_600_000L);
    Mockito.when(config.get(Configs.CACHE_WEIGHER_ENABLED)).thenReturn(true);
    Mockito.when(config.get(Configs.CACHE_STATS_ENABLED)).thenReturn(false);
    Mockito.when(config.get(Configs.CACHE_IMPLEMENTATION)).thenReturn("caffeine");

    try {
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
          .thenReturn(
              String.format(
                  "jdbc:h2:%s;DB_CLOSE_DELAY=-1;AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE", DB_DIR));
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("gravitino");
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("gravitino");
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS)).thenReturn(100);
      Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS)).thenReturn(1000L);

      FieldUtils.writeStaticField(
          SQLExceptionConverterFactory.class, "converter", new H2ExceptionConverter(), true);

    } catch (Exception e) {
      LOG.error("Failed to init entity store", e);
      throw new RuntimeException(e);
    }

    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
  }

  /**
   * Init benchmark entities with relations.
   *
   * @throws IOException if an I/O error occurs.
   */
  protected void initBenchmarkEntities() throws IOException {
    this.entities = getEntities(totalCnt);
    this.entitiesWithRelations = getRelationEntities(totalCnt);

    for (ModelEntity entity : entities) {
      store.put(entity);
    }

    for (Map.Entry<UserEntity, Entity> entry : entitiesWithRelations.entrySet()) {
      UserEntity user = entry.getKey();
      TopicEntity topic = (TopicEntity) entry.getValue();

      store.put(user, true);
      store.put(topic, true);

      ((RelationalEntityStore) store)
          .insertRelation(
              SupportsRelationOperations.Type.OWNER_REL,
              topic.nameIdentifier(),
              Entity.EntityType.TOPIC,
              user.nameIdentifier(),
              Entity.EntityType.USER,
              true);
    }
  }

  private void initParentEntities() throws IOException {
    metalake =
        BaseMetalake.builder()
            .withId(generator.nextId())
            .withName(BENCHMARK_METALAKE_NAME)
            .withComment("benchmark metalake")
            .withProperties(ImmutableMap.of())
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(TestUtil.getTestAuditInfo())
            .build();
    store.put(metalake, true);

    catalog =
        CatalogEntity.builder()
            .withId(generator.nextId())
            .withName(BENCHMARK_CATALOG_NAME)
            .withComment("benchmark catalog")
            .withProperties(ImmutableMap.of())
            .withNamespace(Namespace.of(BENCHMARK_METALAKE_NAME))
            .withAuditInfo(TestUtil.getTestAuditInfo())
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("hive")
            .build();
    store.put(catalog, true);

    schema =
        SchemaEntity.builder()
            .withId(generator.nextId())
            .withName(BENCHMARK_SCHEMA_NAME)
            .withComment("benchmark schema")
            .withProperties(ImmutableMap.of())
            .withNamespace(Namespace.of(BENCHMARK_METALAKE_NAME, BENCHMARK_CATALOG_NAME))
            .withAuditInfo(TestUtil.getTestAuditInfo())
            .build();
    store.put(schema, true);
  }
}
