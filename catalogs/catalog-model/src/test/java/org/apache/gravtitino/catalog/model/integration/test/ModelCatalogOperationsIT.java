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
package org.apache.gravtitino.catalog.model.integration.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.utils.RandomNameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ModelCatalogOperationsIT extends BaseIT {

  private final String metalakeName = RandomNameUtils.genRandomName("model_it_metalake");
  private final String catalogName = RandomNameUtils.genRandomName("model_it_catalog");
  private final String schemaName = RandomNameUtils.genRandomName("model_it_schema");

  private GravitinoMetalake gravitinoMetalake;
  private Catalog gravitinoCatalog;

  @BeforeAll
  public void setUp() {
    createMetalake();
    createCatalog();
  }

  @AfterAll
  public void tearDown() {
    gravitinoMetalake.dropCatalog(catalogName, true);
    client.dropMetalake(metalakeName, true);
  }

  @BeforeEach
  public void beforeEach() {
    createSchema();
  }

  @AfterEach
  public void afterEach() {
    dropSchema();
  }

  @Test
  public void testRegisterAndGetModel() {
    String modelName = RandomNameUtils.genRandomName("model1");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    String comment = "comment";
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");

    Model model = gravitinoCatalog.asModelCatalog().registerModel(modelIdent, comment, properties);
    Assertions.assertEquals(modelName, model.name());
    Assertions.assertEquals(comment, model.comment());
    Assertions.assertEquals(properties, model.properties());

    Model loadModel = gravitinoCatalog.asModelCatalog().getModel(modelIdent);
    Assertions.assertEquals(modelName, loadModel.name());
    Assertions.assertEquals(comment, loadModel.comment());
    Assertions.assertEquals(properties, loadModel.properties());

    Assertions.assertTrue(gravitinoCatalog.asModelCatalog().modelExists(modelIdent));

    // Test register existing model
    Assertions.assertThrows(
        ModelAlreadyExistsException.class,
        () -> gravitinoCatalog.asModelCatalog().registerModel(modelIdent, comment, properties));

    // Test register model in a non-existent schema
    NameIdentifier nonExistentSchemaIdent = NameIdentifier.of("non_existent_schema", modelName);
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () ->
            gravitinoCatalog
                .asModelCatalog()
                .registerModel(nonExistentSchemaIdent, comment, properties));

    // Test get non-existent model
    NameIdentifier nonExistentModelIdent = NameIdentifier.of(schemaName, "non_existent_model");
    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> gravitinoCatalog.asModelCatalog().getModel(nonExistentModelIdent));

    // Test get model from non-existent schema
    NameIdentifier nonExistentModelIdent2 = NameIdentifier.of("non_existent_schema", modelName);
    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> gravitinoCatalog.asModelCatalog().getModel(nonExistentModelIdent2));
  }

  @Test
  public void testRegisterAndListModels() {
    String modelName1 = RandomNameUtils.genRandomName("model1");
    String modelName2 = RandomNameUtils.genRandomName("model2");
    NameIdentifier modelIdent1 = NameIdentifier.of(schemaName, modelName1);
    NameIdentifier modelIdent2 = NameIdentifier.of(schemaName, modelName2);

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent1, null, null);
    gravitinoCatalog.asModelCatalog().registerModel(modelIdent2, null, null);

    NameIdentifier[] models =
        gravitinoCatalog.asModelCatalog().listModels(Namespace.of(schemaName));
    Set<NameIdentifier> resultSet = Sets.newHashSet(models);

    Assertions.assertEquals(2, resultSet.size());
    Assertions.assertTrue(resultSet.contains(modelIdent1));
    Assertions.assertTrue(resultSet.contains(modelIdent2));

    // Test delete and list models
    Assertions.assertTrue(gravitinoCatalog.asModelCatalog().deleteModel(modelIdent1));
    NameIdentifier[] modelsAfterDelete =
        gravitinoCatalog.asModelCatalog().listModels(Namespace.of(schemaName));

    Assertions.assertEquals(1, modelsAfterDelete.length);
    Assertions.assertEquals(modelIdent2, modelsAfterDelete[0]);

    Assertions.assertTrue(gravitinoCatalog.asModelCatalog().deleteModel(modelIdent2));
    NameIdentifier[] modelsAfterDeleteAll =
        gravitinoCatalog.asModelCatalog().listModels(Namespace.of(schemaName));

    Assertions.assertEquals(0, modelsAfterDeleteAll.length);

    // Test list models from non-existent schema
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> gravitinoCatalog.asModelCatalog().listModels(Namespace.of("non_existent_schema")));
  }

  @Test
  public void testRegisterAndDeleteModel() {
    String modelName = RandomNameUtils.genRandomName("model1");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    Assertions.assertTrue(gravitinoCatalog.asModelCatalog().deleteModel(modelIdent));
    Assertions.assertFalse(gravitinoCatalog.asModelCatalog().modelExists(modelIdent));
    Assertions.assertFalse(gravitinoCatalog.asModelCatalog().deleteModel(modelIdent));

    // Test delete non-existent model
    NameIdentifier nonExistentModelIdent = NameIdentifier.of(schemaName, "non_existent_model");
    Assertions.assertFalse(gravitinoCatalog.asModelCatalog().deleteModel(nonExistentModelIdent));

    // Test delete model from non-existent schema
    NameIdentifier nonExistentSchemaIdent = NameIdentifier.of("non_existent_schema", modelName);
    Assertions.assertFalse(gravitinoCatalog.asModelCatalog().deleteModel(nonExistentSchemaIdent));
  }

  @Test
  public void testLinkAndGerModelVersion() {
    String modelName = RandomNameUtils.genRandomName("model1");
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri", new String[] {"alias1"}, "comment", properties);

    ModelVersion modelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, "alias1");

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals("uri", modelVersion.uri());
    Assertions.assertArrayEquals(new String[] {"alias1"}, modelVersion.aliases());
    Assertions.assertEquals("comment", modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());
    Assertions.assertTrue(
        gravitinoCatalog.asModelCatalog().modelVersionExists(modelIdent, "alias1"));

    ModelVersion modelVersion1 = gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(0, modelVersion1.version());
    Assertions.assertEquals("uri", modelVersion1.uri());
    Assertions.assertArrayEquals(new String[] {"alias1"}, modelVersion1.aliases());
    Assertions.assertTrue(gravitinoCatalog.asModelCatalog().modelVersionExists(modelIdent, 0));

    // Test link a version to a non-existent model
    NameIdentifier nonExistentModelIdent = NameIdentifier.of(schemaName, "non_existent_model");
    Assertions.assertThrows(
        NoSuchModelException.class,
        () ->
            gravitinoCatalog
                .asModelCatalog()
                .linkModelVersion(
                    nonExistentModelIdent, "uri", new String[] {"alias1"}, "comment", properties));

    // Test link a version using existing alias
    Assertions.assertThrows(
        ModelVersionAliasesAlreadyExistException.class,
        () ->
            gravitinoCatalog
                .asModelCatalog()
                .linkModelVersion(
                    modelIdent, "uri", new String[] {"alias1"}, "comment", properties));

    // Test get non-existent model version
    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () -> gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, "non_existent_alias"));
    Assertions.assertFalse(
        gravitinoCatalog.asModelCatalog().modelVersionExists(modelIdent, "non_existent_alias"));

    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () -> gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 1));
    Assertions.assertFalse(gravitinoCatalog.asModelCatalog().modelVersionExists(modelIdent, 1));
  }

  @Test
  public void testLinkAndDeleteModelVersions() {
    String modelName = RandomNameUtils.genRandomName("model1");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri1", new String[] {"alias1"}, "comment1", null);
    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri2", new String[] {"alias2"}, "comment2", null);

    Assertions.assertTrue(
        gravitinoCatalog.asModelCatalog().deleteModelVersion(modelIdent, "alias1"));
    Assertions.assertFalse(
        gravitinoCatalog.asModelCatalog().modelVersionExists(modelIdent, "alias1"));
    Assertions.assertFalse(gravitinoCatalog.asModelCatalog().deleteModelVersion(modelIdent, 0));

    Assertions.assertTrue(gravitinoCatalog.asModelCatalog().deleteModelVersion(modelIdent, 1));
    Assertions.assertFalse(
        gravitinoCatalog.asModelCatalog().modelVersionExists(modelIdent, "alias2"));
    Assertions.assertFalse(gravitinoCatalog.asModelCatalog().deleteModelVersion(modelIdent, 1));

    // Test delete non-existent model version
    Assertions.assertFalse(
        gravitinoCatalog.asModelCatalog().deleteModelVersion(modelIdent, "non_existent_alias"));

    // Test delete model version of non-existent model
    NameIdentifier nonExistentModelIdent = NameIdentifier.of(schemaName, "non_existent_model");
    Assertions.assertFalse(
        gravitinoCatalog.asModelCatalog().deleteModelVersion(nonExistentModelIdent, "alias1"));

    // Test delete model version of non-existent schema
    NameIdentifier nonExistentSchemaIdent = NameIdentifier.of("non_existent_schema", modelName);
    Assertions.assertFalse(
        gravitinoCatalog.asModelCatalog().deleteModelVersion(nonExistentSchemaIdent, "alias1"));
  }

  @Test
  public void testLinkAndListModelVersions() {
    String modelName = RandomNameUtils.genRandomName("model1");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri1", new String[] {"alias1"}, "comment1", null);
    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri2", new String[] {"alias2"}, "comment2", null);

    int[] modelVersions = gravitinoCatalog.asModelCatalog().listModelVersions(modelIdent);
    Set<Integer> resultSet = Arrays.stream(modelVersions).boxed().collect(Collectors.toSet());

    Assertions.assertEquals(2, resultSet.size());
    Assertions.assertTrue(resultSet.contains(0));
    Assertions.assertTrue(resultSet.contains(1));

    // Test list model versions of non-existent model
    NameIdentifier nonExistentModelIdent = NameIdentifier.of(schemaName, "non_existent_model");
    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> gravitinoCatalog.asModelCatalog().listModelVersions(nonExistentModelIdent));

    // Test list model versions of non-existent schema
    NameIdentifier nonExistentSchemaIdent = NameIdentifier.of("non_existent_schema", modelName);
    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> gravitinoCatalog.asModelCatalog().listModelVersions(nonExistentSchemaIdent));

    // Test delete and list model versions
    Assertions.assertTrue(gravitinoCatalog.asModelCatalog().deleteModelVersion(modelIdent, 1));
    int[] modelVersionsAfterDelete =
        gravitinoCatalog.asModelCatalog().listModelVersions(modelIdent);

    Assertions.assertEquals(1, modelVersionsAfterDelete.length);
    Assertions.assertEquals(0, modelVersionsAfterDelete[0]);

    Assertions.assertTrue(gravitinoCatalog.asModelCatalog().deleteModelVersion(modelIdent, 0));
    int[] modelVersionsAfterDeleteAll =
        gravitinoCatalog.asModelCatalog().listModelVersions(modelIdent);

    Assertions.assertEquals(0, modelVersionsAfterDeleteAll.length);
  }

  @Test
  public void testLinkAndListModelVersionInfos() {
    String modelName = RandomNameUtils.genRandomName("model1");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);
    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri1", new String[] {"alias1"}, "comment1", null);

    ModelVersion[] modelVersions =
        gravitinoCatalog.asModelCatalog().listModelVersionInfos(modelIdent);
    Assertions.assertEquals(1, modelVersions.length);
    Assertions.assertEquals(0, modelVersions[0].version());
    Assertions.assertEquals("uri1", modelVersions[0].uri());
    Assertions.assertArrayEquals(new String[] {"alias1"}, modelVersions[0].aliases());
    Assertions.assertEquals("comment1", modelVersions[0].comment());
    Assertions.assertEquals(Collections.emptyMap(), modelVersions[0].properties());
    Assertions.assertTrue(
        gravitinoCatalog.asModelCatalog().modelVersionExists(modelIdent, "alias1"));

    // Test list model versions info of non-existent model
    NameIdentifier nonExistentModelIdent = NameIdentifier.of(schemaName, "non_existent_model");
    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> gravitinoCatalog.asModelCatalog().listModelVersionInfos(nonExistentModelIdent));

    // Test list model versions info of non-existent schema
    NameIdentifier nonExistentSchemaIdent = NameIdentifier.of("non_existent_schema", modelName);
    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> gravitinoCatalog.asModelCatalog().listModelVersionInfos(nonExistentSchemaIdent));

    // Test delete and list model versions info
    Assertions.assertTrue(gravitinoCatalog.asModelCatalog().deleteModelVersion(modelIdent, 0));
    ModelVersion[] modelVersionsAfterDelete =
        gravitinoCatalog.asModelCatalog().listModelVersionInfos(modelIdent);
    Assertions.assertEquals(0, modelVersionsAfterDelete.length);
  }

  @Test
  void testLinkAndUpdateModelVersionComment() {
    String modelName = RandomNameUtils.genRandomName("model1");
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    String versionNewComment = "new comment";

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri", new String[] {"alias1"}, "comment", properties);

    ModelVersion modelVersion = gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals("uri", modelVersion.uri());
    Assertions.assertArrayEquals(new String[] {"alias1"}, modelVersion.aliases());
    Assertions.assertEquals("comment", modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange updateComment = ModelVersionChange.updateComment(versionNewComment);
    ModelVersion updatedModelVersion =
        gravitinoCatalog.asModelCatalog().alterModelVersion(modelIdent, 0, updateComment);

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), updatedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), updatedModelVersion.aliases());
    Assertions.assertEquals(versionNewComment, updatedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), updatedModelVersion.properties());
  }

  @Test
  void testLinkAndUpdateModelVersionCommentViaAlias() {
    String modelName = RandomNameUtils.genRandomName("model1");
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    String aliasNewComment = "new comment";

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri", new String[] {"alias1"}, "comment", properties);

    ModelVersion modelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, "alias1");
    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals("uri", modelVersion.uri());
    Assertions.assertArrayEquals(new String[] {"alias1"}, modelVersion.aliases());
    Assertions.assertEquals("comment", modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange updateComment = ModelVersionChange.updateComment(aliasNewComment);
    ModelVersion updatedModelVersion =
        gravitinoCatalog.asModelCatalog().alterModelVersion(modelIdent, "alias1", updateComment);

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), updatedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), updatedModelVersion.aliases());
    Assertions.assertEquals(aliasNewComment, updatedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), updatedModelVersion.properties());
  }

  @Test
  public void testRegisterAndRenameModel() {
    String comment = "comment";
    String modelName = RandomNameUtils.genRandomName("alter_name_model");
    String newName = RandomNameUtils.genRandomName("new_name");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    Map<String, String> properties = ImmutableMap.of("owner", "data-team");

    Model createdModel =
        gravitinoCatalog.asModelCatalog().registerModel(modelIdent, comment, properties);

    ModelChange updateName = ModelChange.rename(newName);
    Model alteredModel = gravitinoCatalog.asModelCatalog().alterModel(modelIdent, updateName);

    Assertions.assertEquals(newName, alteredModel.name());
    Assertions.assertEquals(createdModel.properties(), alteredModel.properties());
    Assertions.assertEquals(createdModel.comment(), alteredModel.comment());

    NameIdentifier nonExistIdent = NameIdentifier.of(schemaName, "non_exist_model");
    Assertions.assertThrows(
        NoSuchModelException.class,
        () -> gravitinoCatalog.asModelCatalog().alterModel(nonExistIdent, updateName));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            gravitinoCatalog
                .asModelCatalog()
                .alterModel(NameIdentifier.of(schemaName, null), updateName));

    // reload model and check name
    Model reloadedModel =
        gravitinoCatalog
            .asModelCatalog()
            .getModel(NameIdentifier.of(modelIdent.namespace(), newName));
    Assertions.assertEquals(newName, reloadedModel.name());
    Assertions.assertEquals(createdModel.properties(), reloadedModel.properties());
    Assertions.assertEquals(createdModel.comment(), reloadedModel.comment());
  }

  @Test
  void testRegisterAndAddModelProperty() {
    String comment = "comment";
    String modelName = RandomNameUtils.genRandomName("alter_name_model");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    Map<String, String> properties = ImmutableMap.of("owner", "data-team", "key1", "val1");
    Map<String, String> newProperties =
        ImmutableMap.of("owner", "data-team", "key1", "val1", "key2", "val2");

    Model createdModel =
        gravitinoCatalog.asModelCatalog().registerModel(modelIdent, comment, properties);

    ModelChange addProperty = ModelChange.setProperty("key2", "val2");
    Model alteredModel = gravitinoCatalog.asModelCatalog().alterModel(modelIdent, addProperty);

    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertNotEquals(createdModel.properties(), alteredModel.properties());
    Assertions.assertEquals(newProperties, alteredModel.properties());
    Assertions.assertEquals(createdModel.comment(), alteredModel.comment());

    // reload model and check properties
    Model reloadedModel = gravitinoCatalog.asModelCatalog().getModel(modelIdent);
    Assertions.assertEquals(modelName, reloadedModel.name());
    Assertions.assertNotEquals(createdModel.properties(), reloadedModel.properties());
    Assertions.assertEquals(newProperties, reloadedModel.properties());
    Assertions.assertEquals(createdModel.comment(), reloadedModel.comment());
  }

  @Test
  void testRegisterAndUpdateModelProperty() {
    String comment = "comment";
    String modelName = RandomNameUtils.genRandomName("alter_name_model");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    Map<String, String> properties = ImmutableMap.of("owner", "data-team", "key1", "val1");
    Map<String, String> newProperties = ImmutableMap.of("owner", "data-team", "key1", "val3");

    Model createdModel =
        gravitinoCatalog.asModelCatalog().registerModel(modelIdent, comment, properties);
    ModelChange addProperty = ModelChange.setProperty("key1", "val3");
    Model alteredModel = gravitinoCatalog.asModelCatalog().alterModel(modelIdent, addProperty);

    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(newProperties, alteredModel.properties());
    Assertions.assertEquals(createdModel.comment(), alteredModel.comment());

    // reload model and check properties
    Model reloadedModel = gravitinoCatalog.asModelCatalog().getModel(modelIdent);
    Assertions.assertEquals(modelName, reloadedModel.name());
    Assertions.assertEquals(newProperties, reloadedModel.properties());
    Assertions.assertEquals(createdModel.comment(), reloadedModel.comment());
  }

  @Test
  void testRegisterAndRemoveModelProperty() {
    String comment = "comment";
    String modelName = RandomNameUtils.genRandomName("alter_name_model");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    Map<String, String> properties = ImmutableMap.of("owner", "data-team", "key1", "val1");
    Map<String, String> newProperties = ImmutableMap.of("owner", "data-team");

    Model createdModel =
        gravitinoCatalog.asModelCatalog().registerModel(modelIdent, comment, properties);
    ModelChange addProperty = ModelChange.removeProperty("key1");
    Model alteredModel = gravitinoCatalog.asModelCatalog().alterModel(modelIdent, addProperty);

    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(newProperties, alteredModel.properties());
    Assertions.assertEquals(createdModel.comment(), alteredModel.comment());

    // reload model and check properties
    Model reloadedModel = gravitinoCatalog.asModelCatalog().getModel(modelIdent);
    Assertions.assertEquals(modelName, reloadedModel.name());
    Assertions.assertEquals(newProperties, reloadedModel.properties());
    Assertions.assertEquals(createdModel.comment(), reloadedModel.comment());
  }

  @Test
  void testRegisterAndUpdateModelComment() {
    String comment = "comment";
    String newComment = "new comment";
    String modelName = RandomNameUtils.genRandomName("alter_name_model");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    Map<String, String> properties = ImmutableMap.of("owner", "data-team", "key1", "val1");

    Model createdModel =
        gravitinoCatalog.asModelCatalog().registerModel(modelIdent, comment, properties);
    ModelChange change = ModelChange.updateComment(newComment);
    Model alteredModel = gravitinoCatalog.asModelCatalog().alterModel(modelIdent, change);

    Assertions.assertEquals(createdModel.name(), alteredModel.name());
    Assertions.assertEquals(createdModel.properties(), alteredModel.properties());
    Assertions.assertEquals(newComment, alteredModel.comment());

    // reload model and check comment
    Model reloadedModel = gravitinoCatalog.asModelCatalog().getModel(modelIdent);
    Assertions.assertEquals(createdModel.name(), reloadedModel.name());
    Assertions.assertEquals(createdModel.properties(), reloadedModel.properties());
    Assertions.assertEquals(newComment, reloadedModel.comment());
  }

  @Test
  void testLinkAndSetModelVersionProperties() {
    String modelName = RandomNameUtils.genRandomName("model1");
    String[] aliases = {"alias1"};
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    Map<String, String> newProperties =
        ImmutableMap.of("key1", "new value", "key2", "val2", "key3", "val3");

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri", aliases, "comment", properties);

    ModelVersion modelVersion = gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals("uri", modelVersion.uri());
    Assertions.assertArrayEquals(aliases, modelVersion.aliases());
    Assertions.assertEquals("comment", modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange[] changes = {
      ModelVersionChange.setProperty("key1", "new value"),
      ModelVersionChange.setProperty("key3", "val3")
    };
    ModelVersion updatedModelVersion =
        gravitinoCatalog.asModelCatalog().alterModelVersion(modelIdent, 0, changes);

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), updatedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), updatedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), updatedModelVersion.comment());
    Assertions.assertEquals(newProperties, updatedModelVersion.properties());

    // reload model version and check properties
    ModelVersion reloadedModelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(newProperties, reloadedModelVersion.properties());
  }

  @Test
  void testLinkAndSetModelVersionPropertiesByAlias() {
    String modelName = RandomNameUtils.genRandomName("model1");
    String[] aliases = {"alias1"};
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    Map<String, String> newProperties =
        ImmutableMap.of("key1", "new value", "key2", "val2", "key3", "val3");

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri", aliases, "comment", properties);

    ModelVersion modelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, aliases[0]);

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals("uri", modelVersion.uri());
    Assertions.assertArrayEquals(aliases, modelVersion.aliases());
    Assertions.assertEquals("comment", modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange[] changes = {
      ModelVersionChange.setProperty("key1", "new value"),
      ModelVersionChange.setProperty("key3", "val3")
    };
    ModelVersion updatedModelVersion =
        gravitinoCatalog.asModelCatalog().alterModelVersion(modelIdent, aliases[0], changes);

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), updatedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), updatedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), updatedModelVersion.comment());
    Assertions.assertEquals(newProperties, updatedModelVersion.properties());

    // reload model version and check properties
    ModelVersion reloadedModelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, aliases[0]);

    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(newProperties, reloadedModelVersion.properties());
  }

  @Test
  void testLinkAndUpdateModelVersionUri() {
    String modelName = RandomNameUtils.genRandomName("model1");
    String[] aliases = {"alias1"};
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);

    String uri = "s3://bucket/path/to/model.zip";
    String newUri = "s3://bucket/path/to/new_model.zip";
    String versionComment = "comment";

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, uri, aliases, versionComment, properties);

    ModelVersion modelVersion = gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals(uri, modelVersion.uri());
    Assertions.assertArrayEquals(aliases, modelVersion.aliases());
    Assertions.assertEquals(versionComment, modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange updateUriChange = ModelVersionChange.updateUri(newUri);
    ModelVersion updatedModelVersion =
        gravitinoCatalog.asModelCatalog().alterModelVersion(modelIdent, 0, updateUriChange);

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(newUri, updatedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), updatedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), updatedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), updatedModelVersion.properties());

    // reload model version and check uri
    ModelVersion reloadedModelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(newUri, reloadedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  @Test
  void testLinkAndUpdateModelVersionUriByAlias() {
    String modelName = RandomNameUtils.genRandomName("model1");
    String[] aliases = {"alias1"};
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);

    String uri = "s3://bucket/path/to/model.zip";
    String newUri = "s3://bucket/path/to/new_model.zip";
    String versionComment = "comment";

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, uri, aliases, versionComment, properties);

    ModelVersion modelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, aliases[0]);

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals(uri, modelVersion.uri());
    Assertions.assertArrayEquals(aliases, modelVersion.aliases());
    Assertions.assertEquals(versionComment, modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange updateUriChange = ModelVersionChange.updateUri(newUri);
    ModelVersion updatedModelVersion =
        gravitinoCatalog
            .asModelCatalog()
            .alterModelVersion(modelIdent, aliases[0], updateUriChange);

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(newUri, updatedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), updatedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), updatedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), updatedModelVersion.properties());

    // reload model version and check uri
    ModelVersion reloadedModelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, aliases[0]);

    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(newUri, reloadedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  @Test
  void testLinkAndRemoveModelVersionProperties() {
    String modelName = RandomNameUtils.genRandomName("model1");
    String[] aliases = {"alias1"};
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    Map<String, String> newProperties = ImmutableMap.of("key2", "val2");

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri", aliases, "comment", properties);

    ModelVersion modelVersion = gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals("uri", modelVersion.uri());
    Assertions.assertArrayEquals(aliases, modelVersion.aliases());
    Assertions.assertEquals("comment", modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange change = ModelVersionChange.removeProperty("key1");
    ModelVersion updatedModelVersion =
        gravitinoCatalog.asModelCatalog().alterModelVersion(modelIdent, 0, change);

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), updatedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), updatedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), updatedModelVersion.comment());
    Assertions.assertEquals(newProperties, updatedModelVersion.properties());

    // reload model version and check properties
    ModelVersion reloadedModelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(newProperties, reloadedModelVersion.properties());
  }

  @Test
  void testLinkAndRemoveModelVersionPropertiesByAlias() {
    String modelName = RandomNameUtils.genRandomName("model1");
    String[] aliases = {"alias1"};
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);
    Map<String, String> newProperties = ImmutableMap.of("key2", "val2");

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri", aliases, "comment", properties);

    ModelVersion modelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, aliases[0]);

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals("uri", modelVersion.uri());
    Assertions.assertArrayEquals(aliases, modelVersion.aliases());
    Assertions.assertEquals("comment", modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange change = ModelVersionChange.removeProperty("key1");
    ModelVersion updatedModelVersion =
        gravitinoCatalog.asModelCatalog().alterModelVersion(modelIdent, aliases[0], change);

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), updatedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), updatedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), updatedModelVersion.comment());
    Assertions.assertEquals(newProperties, updatedModelVersion.properties());

    // reload model version and check properties
    ModelVersion reloadedModelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, aliases[0]);

    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertArrayEquals(modelVersion.aliases(), reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(newProperties, reloadedModelVersion.properties());
  }

  @Test
  void testLinkAndUpdateModelVersionAliases() {
    String modelName = RandomNameUtils.genRandomName("model1");
    String[] aliases = {"alias1", "alias2"};
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri", aliases, "comment", properties);

    ModelVersion modelVersion = gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals("uri", modelVersion.uri());
    Assertions.assertArrayEquals(aliases, modelVersion.aliases());
    Assertions.assertEquals("comment", modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange change =
        ModelVersionChange.updateAliases(
            new String[] {"alias1", "alias3"}, new String[] {"alias1", "alias2"});
    ModelVersion updatedModelVersion =
        gravitinoCatalog.asModelCatalog().alterModelVersion(modelIdent, 0, change);
    String[] newAliases = {"alias1", "alias3"};

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), updatedModelVersion.uri());
    Assertions.assertArrayEquals(newAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), updatedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), updatedModelVersion.properties());

    // reload model version and check aliases
    ModelVersion reloadedModelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, 0);

    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertArrayEquals(newAliases, reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  @Test
  void testLinkAndUpdateModelVersionAliasesByAlias() {
    String modelName = RandomNameUtils.genRandomName("model1");
    String[] aliases = {"alias1", "alias2"};
    Map<String, String> properties = ImmutableMap.of("key1", "val1", "key2", "val2");
    NameIdentifier modelIdent = NameIdentifier.of(schemaName, modelName);

    gravitinoCatalog.asModelCatalog().registerModel(modelIdent, null, null);

    gravitinoCatalog
        .asModelCatalog()
        .linkModelVersion(modelIdent, "uri", aliases, "comment", properties);

    ModelVersion modelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, "alias1");

    Assertions.assertEquals(0, modelVersion.version());
    Assertions.assertEquals("uri", modelVersion.uri());
    Assertions.assertArrayEquals(aliases, modelVersion.aliases());
    Assertions.assertEquals("comment", modelVersion.comment());
    Assertions.assertEquals(properties, modelVersion.properties());

    ModelVersionChange change =
        ModelVersionChange.updateAliases(
            new String[] {"alias1", "alias3"}, new String[] {"alias1", "alias2"});
    ModelVersion updatedModelVersion =
        gravitinoCatalog.asModelCatalog().alterModelVersion(modelIdent, "alias1", change);
    String[] newAliases = {"alias1", "alias3"};

    Assertions.assertEquals(modelVersion.version(), updatedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), updatedModelVersion.uri());
    Assertions.assertArrayEquals(newAliases, updatedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), updatedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), updatedModelVersion.properties());

    // reload model version and check aliases
    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () -> gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, "alias2"));
    Assertions.assertDoesNotThrow(
        () -> gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, "alias1"));
    Assertions.assertDoesNotThrow(
        () -> gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, "alias3"));
    ModelVersion reloadedModelVersion =
        gravitinoCatalog.asModelCatalog().getModelVersion(modelIdent, "alias3");

    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertArrayEquals(newAliases, reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  private void createMetalake() {
    GravitinoMetalake[] gravitinoMetalakes = client.listMetalakes();
    Assertions.assertEquals(0, gravitinoMetalakes.length);

    client.createMetalake(metalakeName, "comment", Collections.emptyMap());
    GravitinoMetalake loadMetalake = client.loadMetalake(metalakeName);
    Assertions.assertEquals(metalakeName, loadMetalake.name());

    gravitinoMetalake = loadMetalake;
  }

  private void createCatalog() {
    gravitinoMetalake.createCatalog(catalogName, Catalog.Type.MODEL, "comment", ImmutableMap.of());
    gravitinoCatalog = gravitinoMetalake.loadCatalog(catalogName);
  }

  private void createSchema() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    String comment = "comment";

    gravitinoCatalog.asSchemas().createSchema(schemaName, comment, properties);
    Schema loadSchema = gravitinoCatalog.asSchemas().loadSchema(schemaName);
    Assertions.assertEquals(schemaName, loadSchema.name());
    Assertions.assertEquals(comment, loadSchema.comment());
    Assertions.assertEquals("val1", loadSchema.properties().get("key1"));
    Assertions.assertEquals("val2", loadSchema.properties().get("key2"));
  }

  private void dropSchema() {
    gravitinoCatalog.asSchemas().dropSchema(schemaName, true);
  }
}
