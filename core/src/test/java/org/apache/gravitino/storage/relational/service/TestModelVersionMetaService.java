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
package org.apache.gravitino.storage.relational.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.ModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.ModelVersionMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestModelVersionMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_model_version_meta_test";

  private static final String CATALOG_NAME = "catalog_for_model_version_meta_test";

  private static final String SCHEMA_NAME = "schema_for_model_version_meta_test";

  private static final Namespace MODEL_NS = Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

  private final Map<String, String> properties = ImmutableMap.of("k1", "v1");

  private final List<String> aliases = Lists.newArrayList("alias1", "alias2");

  @TestTemplate
  public void testInsertModelVersionWaitsForConcurrentSchemaDelete() throws Exception {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    SchemaEntity schema =
        SchemaMetaService.getInstance()
            .getSchemaByIdentifier(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME));
    SchemaPO observedSchemaPO =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schema.id()));

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model_racing_schema_drop",
            "model comment",
            0,
            properties,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(modelEntity, false);
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            aliases,
            "version comment",
            properties,
            AUDIT_INFO);

    CountDownLatch schemaDeleteLocked = new CountDownLatch(1);
    CountDownLatch allowDeleteCommit = new CountDownLatch(1);
    CountDownLatch versionInsertStarted = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Throwable> deleteResult =
        executor.submit(
            () -> {
              try {
                SessionUtils.doMultipleWithCommit(
                    () -> {
                      int deleted =
                          SessionUtils.getWithoutCommit(
                              SchemaMetaMapper.class,
                              mapper ->
                                  mapper.softDeleteSchemaMetaBySchemaIdAndVersion(
                                      observedSchemaPO.getSchemaId(),
                                      observedSchemaPO.getCurrentVersion()));
                      Assertions.assertEquals(1, deleted);
                      schemaDeleteLocked.countDown();
                      try {
                        Assertions.assertTrue(allowDeleteCommit.await(30, TimeUnit.SECONDS));
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                    });
                return null;
              } catch (Throwable throwable) {
                return throwable;
              }
            });
    try {
      Assertions.assertTrue(schemaDeleteLocked.await(30, TimeUnit.SECONDS));
      Future<Throwable> insertResult =
          executor.submit(
              () -> {
                versionInsertStarted.countDown();
                try {
                  ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity);
                  return null;
                } catch (Throwable throwable) {
                  return throwable;
                }
              });
      Assertions.assertTrue(versionInsertStarted.await(30, TimeUnit.SECONDS));
      Assertions.assertThrows(
          TimeoutException.class, () -> insertResult.get(500, TimeUnit.MILLISECONDS));

      allowDeleteCommit.countDown();
      Assertions.assertNull(deleteResult.get(30, TimeUnit.SECONDS));
      Assertions.assertInstanceOf(
          NoSuchEntityException.class, insertResult.get(30, TimeUnit.SECONDS));
    } finally {
      allowDeleteCommit.countDown();
      executor.shutdownNow();
    }

    Assertions.assertTrue(
        SessionUtils.getWithoutCommit(
                ModelVersionMetaMapper.class,
                mapper -> mapper.listModelVersionMetasByModelId(modelEntity.id()))
            .isEmpty());
    Assertions.assertTrue(
        SessionUtils.getWithoutCommit(
                ModelVersionAliasRelMapper.class,
                mapper -> mapper.selectModelVersionAliasRelsByModelId(modelEntity.id()))
            .isEmpty());
  }

  @TestTemplate
  public void testDeleteModelVersionLosingRaceToModelDropReturnsFalse() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model_dropped_during_version_delete",
            "model comment",
            0,
            properties,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    ModelVersionEntity modelVersion =
        createModelVersionEntity(
            model.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "path"),
            aliases,
            "version comment",
            properties,
            AUDIT_INFO);
    ModelVersionMetaService.getInstance().insertModelVersion(modelVersion);

    ModelPO observedModelPO =
        ModelMetaService.getInstance().getModelPOByIdentifier(model.nameIdentifier());
    // Another node drops the model row after this one read it, while the version rows this delete
    // is about are still visible. The version bump below is what discovers the model is gone.
    SessionUtils.doWithCommit(
        ModelMetaMapper.class,
        mapper ->
            mapper.softDeleteModelMetaByIdAndVersion(
                observedModelPO.getModelId(), observedModelPO.getCurrentVersion()));

    ModelMetaService modelMetaService = Mockito.spy(ModelMetaService.getInstance());
    Mockito.doReturn(observedModelPO)
        .when(modelMetaService)
        .getModelPOByIdentifier(model.nameIdentifier());

    try (MockedStatic<ModelMetaService> mocked = Mockito.mockStatic(ModelMetaService.class)) {
      mocked.when(ModelMetaService::getInstance).thenReturn(modelMetaService);

      // A version delete whose model disappeared underneath it reports the same false as a delete
      // that found no model in the first place.
      Assertions.assertFalse(
          ModelVersionMetaService.getInstance().deleteModelVersion(modelVersion.nameIdentifier()));
    }
  }

  @TestTemplate
  public void testUpdateModelVersionFailsWhenSchemaIsDeletedConcurrently() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model_updated_during_schema_drop",
            "model comment",
            0,
            properties,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    ModelVersionEntity modelVersion =
        createModelVersionEntity(
            model.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "old_path"),
            aliases,
            "version comment",
            properties,
            AUDIT_INFO);
    ModelVersionMetaService.getInstance().insertModelVersion(modelVersion);

    ModelVersionEntity updatedVersion =
        createModelVersionEntity(
            model.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "new_path"),
            aliases,
            "version comment",
            properties,
            AUDIT_INFO);
    NameIdentifier schemaIdent = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    modelVersion.nameIdentifier(),
                    ignored -> {
                      // The update has already resolved both the model and its version here. A
                      // schema cascade that commits now must make the write fail before it can
                      // reinsert the version with its new URI.
                      Assertions.assertTrue(
                          SchemaMetaService.getInstance().deleteSchema(schemaIdent, true));
                      return updatedVersion;
                    }));

    Assertions.assertTrue(
        SessionUtils.getWithoutCommit(
                ModelVersionMetaMapper.class,
                mapper -> mapper.listModelVersionMetasByModelId(model.id()))
            .isEmpty());
  }

  @TestTemplate
  public void testInsertAndSelectModelVersion() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    // Create a model entity
    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    // Create a model version entity
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            aliases,
            "test comment",
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    // Test if the model version can be retrieved by the identifier
    Assertions.assertEquals(
        modelVersionEntity,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(getModelVersionIdent(modelEntity.nameIdentifier(), 0)));

    Assertions.assertEquals(
        modelVersionEntity,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(
                getModelVersionIdent(modelEntity.nameIdentifier(), "alias1")));

    Assertions.assertEquals(
        modelVersionEntity,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(
                getModelVersionIdent(modelEntity.nameIdentifier(), "alias2")));

    // Test insert again to get a new version number
    ModelVersionEntity modelVersionEntity2 =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            1,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            null,
            null,
            null,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity2));

    // Test if the new model version can be retrieved by the identifier
    Assertions.assertEquals(
        modelVersionEntity2,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(getModelVersionIdent(modelEntity.nameIdentifier(), 1)));

    // Test if the old model version can still be retrieved by the identifier
    Assertions.assertEquals(
        modelVersionEntity,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(getModelVersionIdent(modelEntity.nameIdentifier(), 0)));

    // Test if the old model version can still be retrieved by the alias
    Assertions.assertEquals(
        modelVersionEntity,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(
                getModelVersionIdent(modelEntity.nameIdentifier(), "alias1")));

    // Test fetch a non-exist model version
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(
                    getModelVersionIdent(modelEntity.nameIdentifier(), 2)));

    // Test fetch a non-exist model alias
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(
                    getModelVersionIdent(modelEntity.nameIdentifier(), "alias3")));

    // Test fetch from a non-exist model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(
                    getModelVersionIdent(NameIdentifier.of(MODEL_NS, "model2"), 0)));

    // Model latest version should be updated
    ModelEntity registeredModelEntity =
        ModelMetaService.getInstance().getModelByIdentifier(modelEntity.nameIdentifier());
    Assertions.assertEquals(2, registeredModelEntity.latestVersion());

    // Test fetch from an invalid model version
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(NameIdentifier.of(MODEL_NS, "model1")));

    // Throw NoSuchEntityException if the model does not exist
    ModelVersionEntity modelVersionEntity3 =
        createModelVersionEntity(
            NameIdentifier.of(MODEL_NS, "model2"),
            1,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            aliases,
            "test comment",
            properties,
            AUDIT_INFO);

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity3));
  }

  @TestTemplate
  public void testInsertAndListModelVersions() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    // Create a model entity
    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    // Create a model version entity
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            aliases,
            "test comment",
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    List<ModelVersionEntity> modelVersions =
        ModelVersionMetaService.getInstance()
            .listModelVersionsByNamespace(getModelVersionNs(modelEntity.nameIdentifier()));
    Assertions.assertEquals(1, modelVersions.size());
    Assertions.assertEquals(modelVersionEntity, modelVersions.get(0));

    // Test insert again to get a new version number
    ModelVersionEntity modelVersionEntity2 =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            1,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            null,
            null,
            null,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity2));

    List<ModelVersionEntity> modelVersions2 =
        ModelVersionMetaService.getInstance()
            .listModelVersionsByNamespace(getModelVersionNs(modelEntity.nameIdentifier()));
    Map<Integer, ModelVersionEntity> modelVersionMap =
        modelVersions2.stream().collect(Collectors.toMap(ModelVersionEntity::version, v -> v));
    Assertions.assertEquals(2, modelVersions2.size());
    Assertions.assertEquals(modelVersionEntity, modelVersionMap.get(0));
    Assertions.assertEquals(modelVersionEntity2, modelVersionMap.get(1));

    // List model versions from a non-exist model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .listModelVersionsByNamespace(
                    getModelVersionNs(NameIdentifier.of(MODEL_NS, "model2"))));
  }

  @TestTemplate
  public void testInsertAndDeleteModelVersion() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    // Create a model entity
    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    // Create a model version entity
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            aliases,
            "test comment",
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    // Test using a non-exist model version to delete
    Assertions.assertFalse(
        ModelVersionMetaService.getInstance()
            .deleteModelVersion(getModelVersionIdent(modelEntity.nameIdentifier(), 100)));

    // Test delete the model version
    Assertions.assertTrue(
        ModelVersionMetaService.getInstance()
            .deleteModelVersion(getModelVersionIdent(modelEntity.nameIdentifier(), 0)));

    // Test fetch a non-exist model version
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(
                    getModelVersionIdent(modelEntity.nameIdentifier(), 0)));

    // Test delete a non-exist model version
    Assertions.assertFalse(
        ModelVersionMetaService.getInstance()
            .deleteModelVersion(getModelVersionIdent(modelEntity.nameIdentifier(), 0)));

    // Test delete a non-exist model version
    Assertions.assertFalse(
        ModelVersionMetaService.getInstance()
            .deleteModelVersion(getModelVersionIdent(modelEntity.nameIdentifier(), 1)));

    // Test delete from a non-exist model
    Assertions.assertFalse(
        ModelVersionMetaService.getInstance()
            .deleteModelVersion(getModelVersionIdent(NameIdentifier.of(MODEL_NS, "model2"), 0)));

    // Test delete by alias
    ModelVersionEntity modelVersionEntity2 =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            1,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            aliases,
            "test comment",
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity2));
    ModelVersionEntity registeredModelVersionEntity =
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(
                getModelVersionIdent(modelEntity.nameIdentifier(), "alias1"));
    Assertions.assertEquals(1, registeredModelVersionEntity.version());

    ModelEntity registeredModelEntity =
        ModelMetaService.getInstance().getModelByIdentifier(modelEntity.nameIdentifier());
    Assertions.assertEquals(2, registeredModelEntity.latestVersion());

    // Test delete by a non-exist alias
    Assertions.assertFalse(
        ModelVersionMetaService.getInstance()
            .deleteModelVersion(getModelVersionIdent(modelEntity.nameIdentifier(), "alias3")));

    // Test delete by an exist alias
    Assertions.assertTrue(
        ModelVersionMetaService.getInstance()
            .deleteModelVersion(getModelVersionIdent(modelEntity.nameIdentifier(), "alias1")));

    // Test delete again by the same alias
    Assertions.assertFalse(
        ModelVersionMetaService.getInstance()
            .deleteModelVersion(getModelVersionIdent(modelEntity.nameIdentifier(), "alias1")));

    // Test fetch a non-exist model version
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(
                    getModelVersionIdent(modelEntity.nameIdentifier(), "alias1")));

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(
                    getModelVersionIdent(modelEntity.nameIdentifier(), "alias2")));
  }

  @TestTemplate
  public void testModelVersionWithMultipleUris() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    // Create a model entity
    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            AUDIT_INFO);
    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    // Create a model version entity with multiple URIs
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "uri1", "uri-name-2", "uri2"),
            aliases,
            "test comment",
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    // Test if the model version can be retrieved by the identifier
    Assertions.assertEquals(
        modelVersionEntity,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(getModelVersionIdent(modelEntity.nameIdentifier(), 0)));

    Assertions.assertEquals(
        modelVersionEntity,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(
                getModelVersionIdent(modelEntity.nameIdentifier(), "alias1")));

    Assertions.assertEquals(
        modelVersionEntity,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(
                getModelVersionIdent(modelEntity.nameIdentifier(), "alias2")));

    // Test list model versions
    List<ModelVersionEntity> modelVersions =
        ModelVersionMetaService.getInstance()
            .listModelVersionsByNamespace(getModelVersionNs(modelEntity.nameIdentifier()));
    Assertions.assertEquals(1, modelVersions.size());
    Assertions.assertEquals(modelVersionEntity, modelVersions.get(0));

    // Test update model version
    ModelVersionEntity updatedModelVersionEntity =
        createModelVersionEntity(
            modelVersionEntity.modelIdentifier(),
            modelVersionEntity.version(),
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "uri1-updated", "uri-name-2", "uri2"),
            ImmutableList.of("alias2", "alias3"),
            "updated comment",
            ImmutableMap.of("k1", "v1", "k2", "v2"),
            modelVersionEntity.auditInfo());

    Function<ModelVersionEntity, ModelVersionEntity> updatePropertiesUpdater =
        oldModelVersionEntity -> updatedModelVersionEntity;

    ModelVersionEntity alteredModelVersionEntity =
        ModelVersionMetaService.getInstance()
            .updateModelVersion(modelVersionEntity.nameIdentifier(), updatePropertiesUpdater);
    Assertions.assertEquals(updatedModelVersionEntity, alteredModelVersionEntity);

    // Test if the model version is updated
    Assertions.assertEquals(
        updatedModelVersionEntity,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(getModelVersionIdent(modelEntity.nameIdentifier(), 0)));

    // Test delete the model version
    Assertions.assertTrue(
        ModelVersionMetaService.getInstance()
            .deleteModelVersion(getModelVersionIdent(modelEntity.nameIdentifier(), 0)));

    // Test fetch a non-exist model version
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(
                    getModelVersionIdent(modelEntity.nameIdentifier(), 0)));
  }

  @TestTemplate
  public void testDeleteModelVersionsInDeletion() throws IOException, SQLException {
    for (String param : new String[] {"model", "schema", "catalog", "metalake"}) {
      init();
      performDeletionTestLogic(param);
    }
  }

  private void performDeletionTestLogic(String input) throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    // Create a model entity
    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    // Create a model version entity
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            aliases,
            "test comment",
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    ModelVersionEntity modelVersionEntity1 =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            1,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            null,
            null,
            null,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity1));

    if (input.equals("model")) {
      // Test delete the model
      Assertions.assertTrue(
          ModelMetaService.getInstance().deleteModel(modelEntity.nameIdentifier()));

    } else if (input.equals("schema")) {
      NameIdentifier schemaIdent = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);
      Assertions.assertThrows(
          NonEmptyEntityException.class,
          () -> SchemaMetaService.getInstance().deleteSchema(schemaIdent, false));

      // Test delete the schema with cascade
      Assertions.assertTrue(SchemaMetaService.getInstance().deleteSchema(schemaIdent, true));

    } else if (input.equals("catalog")) {
      NameIdentifier catalogIdent = NameIdentifier.of(METALAKE_NAME, CATALOG_NAME);
      Assertions.assertThrows(
          NonEmptyEntityException.class,
          () -> CatalogMetaService.getInstance().deleteCatalog(catalogIdent, false));

      // Test delete the catalog with cascade
      Assertions.assertTrue(CatalogMetaService.getInstance().deleteCatalog(catalogIdent, true));

    } else if (input.equals("metalake")) {
      NameIdentifier metalakeIdent = NameIdentifier.of(METALAKE_NAME);
      Assertions.assertThrows(
          NonEmptyEntityException.class,
          () -> MetalakeMetaService.getInstance().deleteMetalake(metalakeIdent, false));

      // Test delete the metalake with cascade
      Assertions.assertTrue(MetalakeMetaService.getInstance().deleteMetalake(metalakeIdent, true));
    }

    // Test fetch a non-exist model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> ModelMetaService.getInstance().getModelByIdentifier(modelEntity.nameIdentifier()));

    // Test list the model versions
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .listModelVersionsByNamespace(getModelVersionNs(modelEntity.nameIdentifier())));

    // Test fetch a non-exist model version
    verifyModelVersionExists(getModelVersionIdent(modelEntity.nameIdentifier(), 0));
    verifyModelVersionExists(getModelVersionIdent(modelEntity.nameIdentifier(), 1));
    verifyModelVersionExists(getModelVersionIdent(modelEntity.nameIdentifier(), "alias1"));
    verifyModelVersionExists(getModelVersionIdent(modelEntity.nameIdentifier(), "alias2"));
  }

  @TestTemplate
  void testUpdateVersionComment() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    Map<String, String> properties = ImmutableMap.of("k1", "v1");
    String modelName = randomModelName();
    String modelComment = null;
    String modelVersionUri = "S3://test/path/to/model/version";
    List<String> modelVersionAliases = ImmutableList.of("alias1", "alias2");
    String modelVersionComment = "test comment";
    String updatedComment = "new comment";
    int version = 0;

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            modelName,
            modelComment,
            0,
            properties,
            AUDIT_INFO);

    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            version,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, modelVersionUri),
            modelVersionAliases,
            modelVersionComment,
            properties,
            AUDIT_INFO);

    ModelVersionEntity updatedModelVersionEntity =
        createModelVersionEntity(
            modelVersionEntity.modelIdentifier(),
            modelVersionEntity.version(),
            modelVersionEntity.uris(),
            modelVersionEntity.aliases(),
            updatedComment,
            modelVersionEntity.properties(),
            modelVersionEntity.auditInfo());

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    Function<ModelVersionEntity, ModelVersionEntity> updateCommentUpdater =
        oldModelVersionEntity -> updatedModelVersionEntity;

    ModelVersionEntity alteredModelVersionEntity =
        ModelVersionMetaService.getInstance()
            .updateModelVersion(modelVersionEntity.nameIdentifier(), updateCommentUpdater);

    Assertions.assertEquals(updatedModelVersionEntity, alteredModelVersionEntity);

    // Test update a non-exist model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    NameIdentifierUtil.ofModelVersion(
                        METALAKE_NAME,
                        CATALOG_NAME,
                        SCHEMA_NAME,
                        "non_exist_model",
                        "non_exist_version"),
                    updateCommentUpdater));

    // Test update a non-exist model version
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    NameIdentifierUtil.ofModelVersion(
                        METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, modelName, "non_exist_version"),
                    updateCommentUpdater));
  }

  @TestTemplate
  void testAlterModelVersionProperties() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");
    String modelName = randomModelName();
    String modelComment = "model1 comment";
    String modelVersionUri = "S3://test/path/to/model/version";
    List<String> modelVersionAliases = ImmutableList.of("alias1", "alias2");
    String modelVersionComment = "test comment";
    Map<String, String> updatedProperties =
        ImmutableMap.of("k1", "new value", "k2", "v2", "k3", "v3");
    int version = 0;

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            modelName,
            modelComment,
            0,
            properties,
            AUDIT_INFO);

    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            version,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, modelVersionUri),
            modelVersionAliases,
            modelVersionComment,
            properties,
            AUDIT_INFO);

    ModelVersionEntity updatedModelVersionEntity =
        createModelVersionEntity(
            modelVersionEntity.modelIdentifier(),
            modelVersionEntity.version(),
            modelVersionEntity.uris(),
            modelVersionEntity.aliases(),
            modelVersionEntity.comment(),
            updatedProperties,
            modelVersionEntity.auditInfo());

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    Function<ModelVersionEntity, ModelVersionEntity> updatePropertiesUpdater =
        oldModelVersionEntity -> updatedModelVersionEntity;

    ModelVersionEntity alteredModelVersionEntity =
        ModelVersionMetaService.getInstance()
            .updateModelVersion(modelVersionEntity.nameIdentifier(), updatePropertiesUpdater);

    Assertions.assertEquals(updatedModelVersionEntity, alteredModelVersionEntity);

    // Test update a non-exist model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    NameIdentifierUtil.ofModelVersion(
                        METALAKE_NAME,
                        CATALOG_NAME,
                        SCHEMA_NAME,
                        "non_exist_model",
                        "non_exist_version"),
                    updatePropertiesUpdater));

    // Test update a non-exist model version
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    NameIdentifierUtil.ofModelVersion(
                        METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, modelName, "non_exist_version"),
                    updatePropertiesUpdater));
  }

  @TestTemplate
  void testUpdateModelVersionUri() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");
    String modelName = randomModelName();
    String modelComment = "model1 comment";
    Map<String, String> modelVersionUris = ImmutableMap.of("n1", "u1");
    List<String> modelVersionAliases = ImmutableList.of("alias1", "alias2");
    String modelVersionComment = "test comment";
    int version = 0;

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            modelName,
            modelComment,
            0,
            properties,
            AUDIT_INFO);

    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            version,
            modelVersionUris,
            modelVersionAliases,
            modelVersionComment,
            properties,
            AUDIT_INFO);

    Map<String, String> newModelVersionUris = ImmutableMap.of("n1", "u1-1", "n2", "u2");
    ModelVersionEntity updatedModelVersionEntity =
        createModelVersionEntity(
            modelVersionEntity.modelIdentifier(),
            modelVersionEntity.version(),
            newModelVersionUris,
            modelVersionEntity.aliases(),
            modelVersionEntity.comment(),
            modelVersionEntity.properties(),
            modelVersionEntity.auditInfo());

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    Function<ModelVersionEntity, ModelVersionEntity> updatePropertiesUpdater =
        oldModelVersionEntity -> updatedModelVersionEntity;

    ModelVersionEntity alteredModelVersionEntity =
        ModelVersionMetaService.getInstance()
            .updateModelVersion(modelVersionEntity.nameIdentifier(), updatePropertiesUpdater);

    Assertions.assertEquals(updatedModelVersionEntity, alteredModelVersionEntity);

    // Test update a non-exist model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    NameIdentifierUtil.ofModelVersion(
                        METALAKE_NAME,
                        CATALOG_NAME,
                        SCHEMA_NAME,
                        "non_exist_model",
                        "non_exist_version"),
                    updatePropertiesUpdater));

    // Test update a non-exist model version
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    NameIdentifierUtil.ofModelVersion(
                        METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, modelName, "non_exist_version"),
                    updatePropertiesUpdater));
  }

  @TestTemplate
  void testUpdateModelVersionAliases() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");
    String modelName = randomModelName();
    String modelComment = "model1 comment";
    String modelVersionUri = "S3://test/path/to/model/version";
    List<String> modelVersionAliases = ImmutableList.of("alias1", "alias2");
    List<String> updatedVersionAliases = ImmutableList.of("alias2", "alias3");
    String modelVersionComment = "test comment";
    int version = 0;

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            modelName,
            modelComment,
            0,
            properties,
            AUDIT_INFO);

    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            version,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, modelVersionUri),
            modelVersionAliases,
            modelVersionComment,
            properties,
            AUDIT_INFO);

    ModelVersionEntity updatedModelVersionEntity =
        createModelVersionEntity(
            modelVersionEntity.modelIdentifier(),
            modelVersionEntity.version(),
            modelVersionEntity.uris(),
            updatedVersionAliases,
            modelVersionEntity.comment(),
            modelVersionEntity.properties(),
            modelVersionEntity.auditInfo());

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    Function<ModelVersionEntity, ModelVersionEntity> updatePropertiesUpdater =
        oldModelVersionEntity -> updatedModelVersionEntity;

    ModelVersionEntity alteredModelVersionEntity =
        ModelVersionMetaService.getInstance()
            .updateModelVersion(modelVersionEntity.nameIdentifier(), updatePropertiesUpdater);

    Assertions.assertEquals(updatedModelVersionEntity, alteredModelVersionEntity);

    // Test update a non-exist model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    NameIdentifierUtil.ofModelVersion(
                        METALAKE_NAME,
                        CATALOG_NAME,
                        SCHEMA_NAME,
                        "non_exist_model",
                        "non_exist_version"),
                    updatePropertiesUpdater));

    // Test update a non-exist model version
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    NameIdentifierUtil.ofModelVersion(
                        METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, modelName, "non_exist_version"),
                    updatePropertiesUpdater));
  }

  @TestTemplate
  void testUpdateModelVersionAliasesFromEmpty() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    Map<String, String> properties = ImmutableMap.of("k1", "v1");
    String modelName = randomModelName();
    List<String> updatedVersionAliases = ImmutableList.of("alias1", "alias2");

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            modelName,
            "model comment",
            0,
            properties,
            AUDIT_INFO);

    // Create model version with NO aliases
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "S3://test/path"),
            Collections.emptyList(),
            "version comment",
            properties,
            AUDIT_INFO);

    ModelVersionEntity updatedModelVersionEntity =
        createModelVersionEntity(
            modelVersionEntity.modelIdentifier(),
            modelVersionEntity.version(),
            modelVersionEntity.uris(),
            updatedVersionAliases,
            modelVersionEntity.comment(),
            modelVersionEntity.properties(),
            modelVersionEntity.auditInfo());

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));
    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    // Updating aliases on a version that had no aliases must not throw
    Function<ModelVersionEntity, ModelVersionEntity> updater = old -> updatedModelVersionEntity;

    ModelVersionEntity altered =
        Assertions.assertDoesNotThrow(
            () ->
                ModelVersionMetaService.getInstance()
                    .updateModelVersion(modelVersionEntity.nameIdentifier(), updater));

    Assertions.assertEquals(updatedVersionAliases, altered.aliases());
  }

  @TestTemplate
  void testModelVersionWritesAdvanceAggregateVersion() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            randomModelName(),
            "model comment",
            0,
            properties,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    assertModelCurrentVersion(model.id(), 1L);

    ModelVersionEntity version =
        createModelVersionEntity(
            model.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            ImmutableList.of("aggregate_alias"),
            "original",
            properties,
            AUDIT_INFO);
    ModelVersionMetaService.getInstance().insertModelVersion(version);
    assertModelCurrentVersion(model.id(), 2L);

    ModelVersionMetaService.getInstance()
        .updateModelVersion(
            version.nameIdentifier(),
            current -> copyModelVersion((ModelVersionEntity) current, "updated"));
    assertModelCurrentVersion(model.id(), 3L);

    Assertions.assertTrue(
        ModelVersionMetaService.getInstance().deleteModelVersion(version.nameIdentifier()));
    assertModelCurrentVersion(model.id(), 4L);
  }

  @TestTemplate
  void testModelVersionAlterRejectsStaleAggregateVersion() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            randomModelName(),
            "model comment",
            0,
            properties,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    ModelVersionEntity version =
        createModelVersionEntity(
            model.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"),
            ImmutableList.of("stale_alias"),
            "original",
            properties,
            AUDIT_INFO);
    ModelVersionMetaService.getInstance().insertModelVersion(version);

    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    version.nameIdentifier(),
                    current -> {
                      updateModelVersionUnchecked(
                          version.nameIdentifier(), winner -> copyModelVersion(winner, "winner"));
                      return copyModelVersion((ModelVersionEntity) current, "stale update");
                    }));

    ModelVersionEntity current =
        ModelVersionMetaService.getInstance().getModelVersionByIdentifier(version.nameIdentifier());
    Assertions.assertEquals("winner", current.comment());
    assertModelCurrentVersion(model.id(), 3L);
  }

  @TestTemplate
  void testAliasConflictRollsBackAggregateVersionAndVersionUpdate() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            randomModelName(),
            "model comment",
            0,
            properties,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    ModelVersionEntity first =
        createModelVersionEntity(
            model.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "first_path"),
            ImmutableList.of("taken_alias"),
            "first",
            properties,
            AUDIT_INFO);
    ModelVersionEntity second =
        createModelVersionEntity(
            model.nameIdentifier(),
            1,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "second_path"),
            ImmutableList.of("second_alias"),
            "second",
            properties,
            AUDIT_INFO);
    ModelVersionMetaService.getInstance().insertModelVersion(first);
    ModelVersionMetaService.getInstance().insertModelVersion(second);
    ModelPO beforeFailure = ModelMetaService.getInstance().getModelPOById(model.id());

    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .updateModelVersion(
                    second.nameIdentifier(),
                    current ->
                        copyModelVersion(
                            (ModelVersionEntity) current,
                            "must roll back",
                            ImmutableList.of("taken_alias"))));

    ModelPO afterFailure = ModelMetaService.getInstance().getModelPOById(model.id());
    ModelVersionEntity unchanged =
        ModelVersionMetaService.getInstance().getModelVersionByIdentifier(second.nameIdentifier());
    Assertions.assertEquals(beforeFailure.getCurrentVersion(), afterFailure.getCurrentVersion());
    Assertions.assertEquals(beforeFailure.getLastVersion(), afterFailure.getLastVersion());
    Assertions.assertEquals("second", unchanged.comment());
    Assertions.assertEquals(ImmutableList.of("second_alias"), unchanged.aliases());
  }

  @TestTemplate
  void testDeleteModelVersionsByLegacyTimeline() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    // Create a model entity
    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    // Create model version entities with aliases
    ModelVersionEntity modelVersionEntity0 =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path_0"),
            aliases,
            "version 0 comment",
            properties,
            AUDIT_INFO);

    ModelVersionEntity modelVersionEntity1 =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            1,
            ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path_1"),
            ImmutableList.of("alias3"),
            "version 1 comment",
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity0));
    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity1));

    // Soft delete the model (cascade deletes model versions)
    Assertions.assertTrue(ModelMetaService.getInstance().deleteModel(modelEntity.nameIdentifier()));

    // Verify model versions are soft deleted (cannot be retrieved)
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(
                    getModelVersionIdent(modelEntity.nameIdentifier(), 0)));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelVersionMetaService.getInstance()
                .getModelVersionByIdentifier(
                    getModelVersionIdent(modelEntity.nameIdentifier(), 1)));

    // Hard delete legacy data for MODEL_VERSION entity type
    int deletedCount =
        backend.hardDeleteLegacyData(
            Entity.EntityType.MODEL_VERSION, Instant.now().toEpochMilli() + 1000);

    // Verify correct number of records deleted
    // Expected: 2 model_version_info records + 3 model_version_alias_rel records = 5 total
    Assertions.assertEquals(5, deletedCount, "Should have deleted 5 legacy records");
  }

  private NameIdentifier getModelVersionIdent(NameIdentifier modelIdent, int version) {
    List<String> parts = Lists.newArrayList(modelIdent.namespace().levels());
    parts.add(modelIdent.name());
    parts.add(String.valueOf(version));
    return NameIdentifier.of(parts.toArray(new String[0]));
  }

  private NameIdentifier getModelVersionIdent(NameIdentifier modelIdent, String alias) {
    List<String> parts = Lists.newArrayList(modelIdent.namespace().levels());
    parts.add(modelIdent.name());
    parts.add(alias);
    return NameIdentifier.of(parts.toArray(new String[0]));
  }

  private Namespace getModelVersionNs(NameIdentifier modelIdent) {
    List<String> parts = Lists.newArrayList(modelIdent.namespace().levels());
    parts.add(modelIdent.name());
    return Namespace.of(parts.toArray(new String[0]));
  }

  private void verifyModelVersionExists(NameIdentifier ident) {
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> ModelVersionMetaService.getInstance().getModelVersionByIdentifier(ident));

    Assertions.assertFalse(ModelVersionMetaService.getInstance().deleteModelVersion(ident));
  }

  private String randomModelName() {
    return "model_" + UUID.randomUUID().toString().replace("-", "");
  }

  private ModelVersionEntity createModelVersionEntity(
      NameIdentifier modelId,
      Integer version,
      Map<String, String> modelUris,
      List<String> aliases,
      String comment,
      Map<String, String> properties,
      AuditInfo auditInfo) {
    return ModelVersionEntity.builder()
        .withModelIdentifier(modelId)
        .withVersion(version)
        .withUris(modelUris)
        .withAliases(aliases)
        .withComment(comment)
        .withProperties(properties)
        .withAuditInfo(auditInfo)
        .build();
  }

  private ModelVersionEntity copyModelVersion(ModelVersionEntity version, String comment) {
    return copyModelVersion(version, comment, version.aliases());
  }

  private ModelVersionEntity copyModelVersion(
      ModelVersionEntity version, String comment, List<String> updatedAliases) {
    return createModelVersionEntity(
        version.modelIdentifier(),
        version.version(),
        version.uris(),
        updatedAliases,
        comment,
        version.properties(),
        version.auditInfo());
  }

  private void updateModelVersionUnchecked(
      NameIdentifier identifier, Function<ModelVersionEntity, ModelVersionEntity> updater) {
    try {
      ModelVersionMetaService.getInstance().updateModelVersion(identifier, updater);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertModelCurrentVersion(long modelId, long expectedVersion) {
    ModelPO modelPO = ModelMetaService.getInstance().getModelPOById(modelId);
    Assertions.assertEquals(expectedVersion, modelPO.getCurrentVersion());
    Assertions.assertEquals(modelPO.getCurrentVersion(), modelPO.getLastVersion());
  }
}
