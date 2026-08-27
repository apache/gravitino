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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

public class TestModelMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_model_meta_test";

  private static final String CATALOG_NAME = "catalog_for_model_meta_test";

  private static final String SCHEMA_NAME = "schema_for_model_meta_test";

  private static final Namespace MODEL_NS = Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    BaseMetalake metalake = createAndInsertMakeLake(METALAKE_NAME);
    createAndInsertCatalog(METALAKE_NAME, CATALOG_NAME);
    createAndInsertSchema(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofModel(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
            "model",
            "model comment",
            1,
            ImmutableMap.of("key", "value"),
            AUDIT_INFO);
    backend.insert(model, false);

    List<ModelEntity> models = backend.list(model.namespace(), Entity.EntityType.MODEL, true);
    assertTrue(models.contains(model));

    // meta data soft delete
    backend.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, true);
    assertFalse(backend.exists(model.nameIdentifier(), Entity.EntityType.MODEL));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(model.id(), Entity.EntityType.MODEL));

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(model.id(), Entity.EntityType.MODEL));
  }

  @TestTemplate
  public void testInsertAndSelectModel() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

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

    ModelEntity registeredModelEntity =
        ModelMetaService.getInstance().getModelByIdentifier(modelEntity.nameIdentifier());
    Assertions.assertEquals(modelEntity, registeredModelEntity);

    // Test insert again without overwrite
    Assertions.assertThrows(
        EntityAlreadyExistsException.class,
        () -> ModelMetaService.getInstance().insertModel(modelEntity, false));

    // Test insert again with overwrite
    ModelEntity modelEntity2 =
        createModelEntity(
            modelEntity.id(),
            modelEntity.namespace(),
            "model2",
            null,
            modelEntity.latestVersion(),
            null,
            AUDIT_INFO);
    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity2, true));
    ModelEntity registeredModelEntity2 =
        ModelMetaService.getInstance().getModelByIdentifier(modelEntity2.nameIdentifier());
    Assertions.assertEquals(modelEntity2, registeredModelEntity2);

    // Test get an in-existent model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelMetaService.getInstance()
                .getModelByIdentifier(NameIdentifier.of(MODEL_NS, "model3")));

    // Test get model by id
    ModelPO modelPO = ModelMetaService.getInstance().getModelPOById(modelEntity.id());
    Assertions.assertEquals(
        modelEntity2, POConverters.fromModelPO(modelPO, modelEntity.namespace()));

    // Test get in-existent model by id
    Assertions.assertThrows(
        NoSuchEntityException.class, () -> ModelMetaService.getInstance().getModelPOById(111L));

    // Test get model id by name
    Long schemaId =
        EntityIdService.getEntityId(NameIdentifier.of(MODEL_NS.levels()), Entity.EntityType.SCHEMA);
    Long modelId =
        ModelMetaService.getInstance().getModelIdBySchemaIdAndModelName(schemaId, "model2");
    Assertions.assertEquals(modelEntity2.id(), modelId);

    // Test get in-existent model id by name
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> ModelMetaService.getInstance().getModelIdBySchemaIdAndModelName(schemaId, "model3"));
  }

  @TestTemplate
  public void testInsertAndListModels() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

    ModelEntity modelEntity1 =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            AUDIT_INFO);
    ModelEntity modelEntity2 =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model2",
            "model2 comment",
            0,
            properties,
            AUDIT_INFO);

    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity1, false));
    Assertions.assertDoesNotThrow(
        () -> ModelMetaService.getInstance().insertModel(modelEntity2, false));

    List<ModelEntity> modelEntities =
        ModelMetaService.getInstance().listModelsByNamespace(MODEL_NS);
    Assertions.assertEquals(2, modelEntities.size());
    Assertions.assertTrue(modelEntities.contains(modelEntity1));
    Assertions.assertTrue(modelEntities.contains(modelEntity2));

    // Test list models by in-existent namespace
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelMetaService.getInstance()
                .listModelsByNamespace(Namespace.of(METALAKE_NAME, CATALOG_NAME, "inexistent")));
  }

  @TestTemplate
  public void testDeleteModelRetriesWhenAVersionIsRegisteredConcurrently() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model_dropped_during_version_register",
            "model comment",
            0,
            ImmutableMap.of("k1", "v1"),
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(modelEntity, false);

    ModelPO stalePO =
        ModelMetaService.getInstance().getModelPOByIdentifier(modelEntity.nameIdentifier());
    // Registering a version advances the concurrency version the model shares with its versions,
    // so a drop that read the model before that point loses its compare-and-set.
    ModelVersionMetaService.getInstance()
        .insertModelVersion(
            ModelVersionEntity.builder()
                .withModelIdentifier(modelEntity.nameIdentifier())
                .withVersion(0)
                .withUris(ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "path"))
                .withAliases(Lists.newArrayList("alias1"))
                .withComment("version comment")
                .withProperties(ImmutableMap.of("k1", "v1"))
                .withAuditInfo(AUDIT_INFO)
                .build());

    ModelMetaService service = Mockito.spy(ModelMetaService.getInstance());
    Mockito.doReturn(stalePO)
        .doCallRealMethod()
        .when(service)
        .getModelPOByIdentifier(modelEntity.nameIdentifier());

    // The drop reads the model again instead of reporting a conflict the caller cannot act on.
    Assertions.assertTrue(service.deleteModel(modelEntity.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> ModelMetaService.getInstance().getModelByIdentifier(modelEntity.nameIdentifier()));
  }

  @TestTemplate
  public void testDeleteModelLosingRaceToAnotherDeleteReturnsFalse() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            ImmutableMap.of("k1", "v1"),
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(modelEntity, false);

    ModelPO observedModelPO =
        ModelMetaService.getInstance().getModelPOByIdentifier(modelEntity.nameIdentifier());
    // Another node drops the model after this one read it, so the compare-and-set below matches no
    // row and the model turns out to be gone rather than concurrently modified.
    Assertions.assertTrue(ModelMetaService.getInstance().deleteModel(modelEntity.nameIdentifier()));

    ModelMetaService service = Mockito.spy(ModelMetaService.getInstance());
    Mockito.doReturn(observedModelPO)
        .when(service)
        .getModelPOByIdentifier(modelEntity.nameIdentifier());

    // A drop that finds nothing left to drop stays a plain false, the same answer the caller gets
    // when the model was already gone before it was read.
    Assertions.assertFalse(service.deleteModel(modelEntity.nameIdentifier()));
  }

  @TestTemplate
  public void testInsertAndDeleteModel() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

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

    Assertions.assertTrue(ModelMetaService.getInstance().deleteModel(modelEntity.nameIdentifier()));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> ModelMetaService.getInstance().getModelByIdentifier(modelEntity.nameIdentifier()));

    // Delete again should return false
    Assertions.assertFalse(
        ModelMetaService.getInstance().deleteModel(modelEntity.nameIdentifier()));

    // Test delete in-existent model
    Assertions.assertFalse(
        ModelMetaService.getInstance().deleteModel(NameIdentifier.of(MODEL_NS, "inexistent")));

    // Test delete in-existent schema
    Assertions.assertFalse(
        ModelMetaService.getInstance()
            .deleteModel(NameIdentifier.of(METALAKE_NAME, CATALOG_NAME, "inexistent", "model1")));
  }

  @TestTemplate
  void testInsertAndRenameModel() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    Map<String, String> properties = ImmutableMap.of("k1", "v1");
    String newName = "new_model_name";

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

    ModelEntity updatedModel =
        ModelEntity.builder()
            .withId(modelEntity.id())
            .withName(newName)
            .withNamespace(modelEntity.namespace())
            .withLatestVersion(modelEntity.latestVersion())
            .withAuditInfo(modelEntity.auditInfo())
            .withComment(modelEntity.comment())
            .withProperties(modelEntity.properties())
            .build();

    Function<ModelEntity, ModelEntity> renameUpdater = oldModel -> updatedModel;
    ModelEntity alteredModel =
        ModelMetaService.getInstance().updateModel(modelEntity.nameIdentifier(), renameUpdater);

    Assertions.assertEquals(alteredModel, updatedModel);
    // Test update an in-existent model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelMetaService.getInstance()
                .updateModel(NameIdentifier.of(MODEL_NS, "model3"), renameUpdater));
  }

  @TestTemplate
  void testInsertAndUpdateModelComment() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    Map<String, String> properties = ImmutableMap.of("k1", "v1");
    String newComment = "new_model_comment";

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

    ModelEntity updatedModel =
        ModelEntity.builder()
            .withId(modelEntity.id())
            .withName(modelEntity.name())
            .withNamespace(modelEntity.namespace())
            .withLatestVersion(modelEntity.latestVersion())
            .withAuditInfo(modelEntity.auditInfo())
            .withComment(newComment)
            .withProperties(modelEntity.properties())
            .build();

    Function<ModelEntity, ModelEntity> renameUpdater = oldModel -> updatedModel;
    ModelEntity alteredModel =
        ModelMetaService.getInstance().updateModel(modelEntity.nameIdentifier(), renameUpdater);

    Assertions.assertEquals(alteredModel, updatedModel);
    // Test update an in-existent model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelMetaService.getInstance()
                .updateModel(NameIdentifier.of(MODEL_NS, "model3"), renameUpdater));

    // test update model comment from null
    ModelEntity modelEntity4 =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model4",
            "model4 comment",
            0,
            properties,
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(modelEntity4, false);

    ModelMetaService.getInstance()
        .updateModel(
            modelEntity4.nameIdentifier(),
            entity -> {
              ModelEntity model = (ModelEntity) entity;
              return ModelEntity.builder()
                  .withId(model.id())
                  .withName(model.name())
                  .withNamespace(model.namespace())
                  .withComment("model comment updated")
                  .withLatestVersion(model.latestVersion())
                  .withProperties(model.properties())
                  .withAuditInfo(model.auditInfo())
                  .build();
            });
    ModelEntity updatedModel4 =
        ModelMetaService.getInstance().getModelByIdentifier(modelEntity4.nameIdentifier());
    Assertions.assertEquals("model comment updated", updatedModel4.comment());
  }

  @TestTemplate
  void testInsertAndUpdateModelProperties() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");
    Map<String, String> newProps = ImmutableMap.of("k1", "v1", "k3", "v3");

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

    ModelEntity updatedModel =
        ModelEntity.builder()
            .withId(modelEntity.id())
            .withName(modelEntity.name())
            .withNamespace(modelEntity.namespace())
            .withLatestVersion(modelEntity.latestVersion())
            .withAuditInfo(modelEntity.auditInfo())
            .withComment(modelEntity.comment())
            .withProperties(newProps)
            .build();

    Function<ModelEntity, ModelEntity> renameUpdater = oldModel -> updatedModel;
    ModelEntity alteredModel =
        ModelMetaService.getInstance().updateModel(modelEntity.nameIdentifier(), renameUpdater);

    Assertions.assertEquals(alteredModel, updatedModel);
    // Test update an in-existent model
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () ->
            ModelMetaService.getInstance()
                .updateModel(NameIdentifier.of(MODEL_NS, "model3"), renameUpdater));
  }

  @TestTemplate
  void testAlterRejectsStaleVersionAndKeepsWinner() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model_alter_conflict",
            "original",
            0,
            ImmutableMap.of("key", "value"),
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    ModelPO initialPO = ModelMetaService.getInstance().getModelPOById(model.id());

    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            ModelMetaService.getInstance()
                .updateModel(
                    model.nameIdentifier(),
                    entity -> {
                      updateModelUnchecked(
                          model.nameIdentifier(), current -> copyModel(current, "winner"));
                      return copyModel((ModelEntity) entity, "stale update");
                    }));

    ModelEntity current =
        ModelMetaService.getInstance().getModelByIdentifier(model.nameIdentifier());
    ModelPO currentPO = ModelMetaService.getInstance().getModelPOById(model.id());
    Assertions.assertEquals("winner", current.comment());
    Assertions.assertEquals(initialPO.getCurrentVersion() + 1, currentPO.getCurrentVersion());
    Assertions.assertEquals(currentPO.getCurrentVersion(), currentPO.getLastVersion());
  }

  @TestTemplate
  void testOverwriteAdvancesVersionAndRejectsStaleDelete() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, AUDIT_INFO);
    ModelEntity model =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model_overwrite_conflict",
            "original",
            0,
            ImmutableMap.of("key", "value"),
            AUDIT_INFO);
    ModelMetaService.getInstance().insertModel(model, false);
    ModelVersionEntity modelVersion =
        ModelVersionEntity.builder()
            .withModelIdentifier(model.nameIdentifier())
            .withVersion(0)
            .withUris(ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "model_path"))
            .withAliases(List.of("surviving_alias"))
            .withComment("version comment")
            .withProperties(ImmutableMap.of("version_key", "version_value"))
            .withAuditInfo(AUDIT_INFO)
            .build();
    ModelVersionMetaService.getInstance().insertModelVersion(modelVersion);
    ModelPO stalePO = ModelMetaService.getInstance().getModelPOById(model.id());

    ModelEntity registered =
        ModelMetaService.getInstance().getModelByIdentifier(model.nameIdentifier());
    ModelMetaService.getInstance().insertModel(copyModel(registered, "winner"), true);

    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    ModelMetaService.getInstance()
                        .deleteModelWithVersion(model.nameIdentifier(), stalePO)));
    ModelEntity current =
        ModelMetaService.getInstance().getModelByIdentifier(model.nameIdentifier());
    ModelPO currentPO = ModelMetaService.getInstance().getModelPOById(model.id());
    Assertions.assertEquals("winner", current.comment());
    Assertions.assertEquals(stalePO.getCurrentVersion() + 1, currentPO.getCurrentVersion());
    Assertions.assertEquals(currentPO.getCurrentVersion(), currentPO.getLastVersion());
    Assertions.assertEquals(
        modelVersion,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(modelVersion.nameIdentifier()));
    Assertions.assertEquals(
        modelVersion,
        ModelVersionMetaService.getInstance()
            .getModelVersionByIdentifier(
                NameIdentifier.of(modelVersion.nameIdentifier().namespace(), "surviving_alias")));
  }

  private ModelEntity copyModel(ModelEntity model, String comment) {
    return ModelEntity.builder()
        .withId(model.id())
        .withName(model.name())
        .withNamespace(model.namespace())
        .withComment(comment)
        .withLatestVersion(model.latestVersion())
        .withProperties(model.properties())
        .withAuditInfo(model.auditInfo())
        .build();
  }

  private void updateModelUnchecked(
      NameIdentifier identifier, Function<ModelEntity, ModelEntity> updater) {
    try {
      ModelMetaService.getInstance().updateModel(identifier, updater);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
