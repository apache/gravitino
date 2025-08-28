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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.ModelPO;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestModelMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_model_meta_test";

  private static final String CATALOG_NAME = "catalog_for_model_meta_test";

  private static final String SCHEMA_NAME = "schema_for_model_meta_test";

  private static final Namespace MODEL_NS = Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  @Test
  public void testInsertAndSelectModel() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            auditInfo);

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
            auditInfo);
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
    Long schemaId = CommonMetaService.getInstance().getParentEntityIdByNamespace(MODEL_NS);
    Long modelId =
        ModelMetaService.getInstance().getModelIdBySchemaIdAndModelName(schemaId, "model2");
    Assertions.assertEquals(modelEntity2.id(), modelId);

    // Test get in-existent model id by name
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> ModelMetaService.getInstance().getModelIdBySchemaIdAndModelName(schemaId, "model3"));
  }

  @Test
  public void testInsertAndListModels() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

    ModelEntity modelEntity1 =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            auditInfo);
    ModelEntity modelEntity2 =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model2",
            "model2 comment",
            0,
            properties,
            auditInfo);

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

  @Test
  public void testInsertAndDeleteModel() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

    ModelEntity modelEntity =
        createModelEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            MODEL_NS,
            "model1",
            "model1 comment",
            0,
            properties,
            auditInfo);

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

  @Test
  void testInsertAndRenameModel() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);
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
            auditInfo);

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

  @Test
  void testInsertAndUpdateModelComment() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);
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
            auditInfo);

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
  }

  @Test
  void testInsertAndUpdateModelProperties() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);
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
            auditInfo);

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
}
