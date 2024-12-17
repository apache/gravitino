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
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestModelVersionMetaService extends TestJDBCBackend {

  private static final String METALAKE_NAME = "metalake_for_model_version_meta_test";

  private static final String CATALOG_NAME = "catalog_for_model_version_meta_test";

  private static final String SCHEMA_NAME = "schema_for_model_version_meta_test";

  private static final Namespace MODEL_NS = Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

  private final Map<String, String> properties = ImmutableMap.of("k1", "v1");

  private final List<String> aliases = Lists.newArrayList("alias1", "alias2");

  @Test
  public void testInsertAndSelectModelVersion() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);

    // Create a model entity
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

    // Create a model version entity
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            "model_path",
            aliases,
            "test comment",
            properties,
            auditInfo);

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
            modelEntity.nameIdentifier(), 1, "model_path", null, null, null, auditInfo);

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
            "model_path",
            aliases,
            "test comment",
            properties,
            auditInfo);

    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity3));
  }

  @Test
  public void testInsertAndListModelVersions() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);

    // Create a model entity
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

    // Create a model version entity
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            "model_path",
            aliases,
            "test comment",
            properties,
            auditInfo);

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
            modelEntity.nameIdentifier(), 1, "model_path", null, null, null, auditInfo);

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

  @Test
  public void testInsertAndDeleteModelVersion() throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);

    // Create a model entity
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

    // Create a model version entity
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            "model_path",
            aliases,
            "test comment",
            properties,
            auditInfo);

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
            "model_path",
            aliases,
            "test comment",
            properties,
            auditInfo);

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

  @ParameterizedTest
  @ValueSource(strings = {"model", "schema", "catalog", "metalake"})
  public void testDeleteModelVersionsInDeletion(String input) throws IOException {
    createParentEntities(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, auditInfo);

    // Create a model entity
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

    // Create a model version entity
    ModelVersionEntity modelVersionEntity =
        createModelVersionEntity(
            modelEntity.nameIdentifier(),
            0,
            "model_path",
            aliases,
            "test comment",
            properties,
            auditInfo);

    Assertions.assertDoesNotThrow(
        () -> ModelVersionMetaService.getInstance().insertModelVersion(modelVersionEntity));

    ModelVersionEntity modelVersionEntity1 =
        createModelVersionEntity(
            modelEntity.nameIdentifier(), 1, "model_path", null, null, null, auditInfo);

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
}
