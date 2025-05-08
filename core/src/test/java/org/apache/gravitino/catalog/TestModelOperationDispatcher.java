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
import static org.apache.gravitino.StringIdentifier.ID_KEY;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestModelOperationDispatcher extends TestOperationDispatcher {

  static ModelOperationDispatcher modelOperationDispatcher;

  static SchemaOperationDispatcher schemaOperationDispatcher;

  @BeforeAll
  public static void initialize() throws IOException, IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);

    modelOperationDispatcher =
        new ModelOperationDispatcher(catalogManager, entityStore, idGenerator);
    schemaOperationDispatcher =
        new SchemaOperationDispatcher(catalogManager, entityStore, idGenerator);
  }

  @Test
  public void testRegisterAndGetModel() {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, "comment", null);

    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    String modelName = randomModelName();
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);

    Model model = modelOperationDispatcher.registerModel(modelIdent, "comment", props);
    Assertions.assertEquals(modelName, model.name());
    Assertions.assertEquals("comment", model.comment());
    Assertions.assertEquals(props, model.properties());
    Assertions.assertFalse(model.properties().containsKey(ID_KEY));

    Model registeredModel = modelOperationDispatcher.getModel(modelIdent);
    Assertions.assertEquals(modelName, registeredModel.name());
    Assertions.assertEquals("comment", registeredModel.comment());
    Assertions.assertEquals(props, registeredModel.properties());
    Assertions.assertFalse(registeredModel.properties().containsKey(ID_KEY));

    // Test register model with illegal property
    Map<String, String> illegalProps = ImmutableMap.of("k1", "v1", ID_KEY, "test");
    testPropertyException(
        () -> modelOperationDispatcher.registerModel(modelIdent, "comment", illegalProps),
        "Properties or property prefixes are reserved and cannot be set",
        ID_KEY);
  }

  @Test
  public void testRegisterAndListModels() {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, "comment", null);

    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    String modelName1 = randomModelName();
    NameIdentifier modelIdent1 =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName1);
    modelOperationDispatcher.registerModel(modelIdent1, "comment", props);

    String modelName2 = randomModelName();
    NameIdentifier modelIdent2 =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName2);
    modelOperationDispatcher.registerModel(modelIdent2, "comment", props);

    NameIdentifier[] modelIdents = modelOperationDispatcher.listModels(modelIdent1.namespace());
    Assertions.assertEquals(2, modelIdents.length);
    Set<NameIdentifier> modelIdentSet = Sets.newHashSet(modelIdents);
    Assertions.assertTrue(modelIdentSet.contains(modelIdent1));
    Assertions.assertTrue(modelIdentSet.contains(modelIdent2));
  }

  @Test
  public void testRegisterAndDeleteModel() {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, "comment", null);

    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    String modelName = randomModelName();
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);

    modelOperationDispatcher.registerModel(modelIdent, "comment", props);
    Assertions.assertTrue(modelOperationDispatcher.deleteModel(modelIdent));
    Assertions.assertFalse(modelOperationDispatcher.deleteModel(modelIdent));
    Assertions.assertThrows(
        NoSuchModelException.class, () -> modelOperationDispatcher.getModel(modelIdent));

    // Test delete in-existent model
    Assertions.assertFalse(
        modelOperationDispatcher.deleteModel(NameIdentifier.of(metalake, catalog, "inexistent")));
  }

  @Test
  public void testLinkAndGetModelVersion() {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, "comment", null);

    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    String modelName = randomModelName();
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);

    Model model = modelOperationDispatcher.registerModel(modelIdent, "comment", props);
    Assertions.assertEquals(0, model.latestVersion());

    String[] aliases = new String[] {"alias1", "alias2"};
    modelOperationDispatcher.linkModelVersion(modelIdent, "path", aliases, "comment", props);

    ModelVersion linkedModelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(0, linkedModelVersion.version());
    Assertions.assertEquals("path", linkedModelVersion.uri());
    Assertions.assertArrayEquals(aliases, linkedModelVersion.aliases());
    Assertions.assertEquals("comment", linkedModelVersion.comment());
    Assertions.assertEquals(props, linkedModelVersion.properties());
    Assertions.assertFalse(linkedModelVersion.properties().containsKey(ID_KEY));

    // Test get model version with alias
    ModelVersion linkedModelVersionWithAlias =
        modelOperationDispatcher.getModelVersion(modelIdent, "alias1");
    Assertions.assertEquals(0, linkedModelVersionWithAlias.version());
    Assertions.assertEquals("path", linkedModelVersionWithAlias.uri());
    Assertions.assertArrayEquals(aliases, linkedModelVersionWithAlias.aliases());
    Assertions.assertFalse(linkedModelVersionWithAlias.properties().containsKey(ID_KEY));

    ModelVersion linkedModelVersionWithAlias2 =
        modelOperationDispatcher.getModelVersion(modelIdent, "alias2");
    Assertions.assertEquals(0, linkedModelVersionWithAlias2.version());
    Assertions.assertEquals("path", linkedModelVersionWithAlias2.uri());
    Assertions.assertArrayEquals(aliases, linkedModelVersionWithAlias2.aliases());
    Assertions.assertFalse(linkedModelVersionWithAlias2.properties().containsKey(ID_KEY));

    // Test Link model version with illegal property
    Map<String, String> illegalProps = ImmutableMap.of("k1", "v1", ID_KEY, "test");
    testPropertyException(
        () ->
            modelOperationDispatcher.linkModelVersion(
                modelIdent, "path", aliases, "comment", illegalProps),
        "Properties or property prefixes are reserved and cannot be set",
        ID_KEY);
  }

  @Test
  public void testLinkAndListModelVersion() {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, "comment", null);

    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    String modelName = randomModelName();
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);

    Model model = modelOperationDispatcher.registerModel(modelIdent, "comment", props);
    Assertions.assertEquals(0, model.latestVersion());

    String[] aliases1 = new String[] {"alias1"};
    String[] aliases2 = new String[] {"alias2"};
    modelOperationDispatcher.linkModelVersion(modelIdent, "path1", aliases1, "comment", props);
    modelOperationDispatcher.linkModelVersion(modelIdent, "path2", aliases2, "comment", props);

    int[] versions = modelOperationDispatcher.listModelVersions(modelIdent);
    Assertions.assertEquals(2, versions.length);
    Set<Integer> versionSet = Arrays.stream(versions).boxed().collect(Collectors.toSet());
    Assertions.assertTrue(versionSet.contains(0));
    Assertions.assertTrue(versionSet.contains(1));
  }

  @Test
  public void testLinkAndDeleteModelVersion() {
    String schemaName = randomSchemaName();
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, "comment", null);

    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    String modelName = randomModelName();
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);

    Model model = modelOperationDispatcher.registerModel(modelIdent, "comment", props);
    Assertions.assertEquals(0, model.latestVersion());

    String[] aliases = new String[] {"alias1"};
    modelOperationDispatcher.linkModelVersion(modelIdent, "path", aliases, "comment", props);
    Assertions.assertTrue(modelOperationDispatcher.deleteModelVersion(modelIdent, 0));
    Assertions.assertFalse(modelOperationDispatcher.deleteModelVersion(modelIdent, 0));
    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () -> modelOperationDispatcher.getModelVersion(modelIdent, 0));

    // Test delete in-existent model version
    Assertions.assertFalse(modelOperationDispatcher.deleteModelVersion(modelIdent, 1));

    // Tet delete model version with alias
    String[] aliases2 = new String[] {"alias2"};
    modelOperationDispatcher.linkModelVersion(modelIdent, "path2", aliases2, "comment", props);
    Assertions.assertTrue(modelOperationDispatcher.deleteModelVersion(modelIdent, "alias2"));
    Assertions.assertFalse(modelOperationDispatcher.deleteModelVersion(modelIdent, "alias2"));
    Assertions.assertThrows(
        NoSuchModelVersionException.class,
        () -> modelOperationDispatcher.getModelVersion(modelIdent, "alias2"));
  }

  @Test
  public void testRenameModel() {
    String schemaName = "test_rename_model_schema";
    String newModelName = "new_model_name";
    String modelComment = "model which tests rename";
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(
        schemaIdent, "comment", ImmutableMap.of("k1", "v1", "k2", "v2"));

    String modelName = "test_rename_model";
    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Model model = modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    ModelChange[] changeComment = new ModelChange[] {ModelChange.rename(newModelName)};
    Model alteredModel = modelOperationDispatcher.alterModel(modelIdent, changeComment);

    Assertions.assertEquals(newModelName, alteredModel.name());
    Assertions.assertEquals(modelComment, alteredModel.comment());
    Assertions.assertEquals(model.properties(), alteredModel.properties());
  }

  @Test
  void testAddModelProperty() {
    String schemaName = "schema";
    String modelName = "test_update_model_property";
    String modelComment = "model which tests update property";
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(
        schemaIdent, "schema comment", ImmutableMap.of("k1", "v1", "k2", "v2"));

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Model model = modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    // validate registered model
    Assertions.assertEquals(modelName, model.name());
    Assertions.assertEquals(modelComment, model.comment());
    Assertions.assertEquals(props, model.properties());

    ModelChange[] addProperty = new ModelChange[] {ModelChange.setProperty("k3", "v3")};
    Model alteredModel = modelOperationDispatcher.alterModel(modelIdent, addProperty);

    // validate updated model
    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(modelComment, alteredModel.comment());
    Assertions.assertEquals(
        ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"), alteredModel.properties());
  }

  @Test
  void testUpdateModelProperty() {
    String schemaName = "test_update_model_property_schema";
    String modelName = "test_update_model_property";
    String modelComment = "model which tests update property";
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(
        schemaIdent, "schema comment", ImmutableMap.of("k1", "v1", "k2", "v2"));

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Model model = modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    // validate registered model
    Assertions.assertEquals(modelName, model.name());
    Assertions.assertEquals(modelComment, model.comment());
    Assertions.assertEquals(props, model.properties());

    ModelChange[] updateProperty = new ModelChange[] {ModelChange.setProperty("k1", "v3")};
    Model alteredModel = modelOperationDispatcher.alterModel(modelIdent, updateProperty);

    // validate updated model
    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(modelComment, alteredModel.comment());
    Assertions.assertEquals(ImmutableMap.of("k1", "v3", "k2", "v2"), alteredModel.properties());
  }

  @Test
  void testRemoveModelProperty() {
    String schemaName = "test_remove_model_property_schema";
    String modelName = "test_remove_model_property";
    String modelComment = "model which tests update property";
    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(
        schemaIdent, "schema comment", ImmutableMap.of("k1", "v1", "k2", "v2"));

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Model model = modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    // validate registered model
    Assertions.assertEquals(modelName, model.name());
    Assertions.assertEquals(modelComment, model.comment());
    Assertions.assertEquals(props, model.properties());

    ModelChange[] removeProperty = new ModelChange[] {ModelChange.removeProperty("k1")};
    Model alteredModel = modelOperationDispatcher.alterModel(modelIdent, removeProperty);

    // validate updated model
    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(modelComment, alteredModel.comment());
    Assertions.assertEquals(ImmutableMap.of("k2", "v2"), alteredModel.properties());
  }

  @Test
  void testUpdateModelComment() {
    String schemaName = "test_update_model_comment_schema";
    String modelName = "test_update_model_comment";
    String modelComment = "model which tests update property";
    String newModelComment = "new model comment";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(
        schemaIdent, "schema comment", ImmutableMap.of("k1", "v1", "k2", "v2"));

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);

    Model model = modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    // validate registered model
    Assertions.assertEquals(modelName, model.name());
    Assertions.assertEquals(modelComment, model.comment());
    Assertions.assertEquals(props, model.properties());

    ModelChange change = ModelChange.updateComment(newModelComment);
    Model alteredModel = modelOperationDispatcher.alterModel(modelIdent, change);

    // validate updated model
    Assertions.assertEquals(modelName, alteredModel.name());
    Assertions.assertEquals(newModelComment, alteredModel.comment());
    Assertions.assertEquals(props, alteredModel.properties());
  }

  @Test
  void testUpdateModelVersionComment() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";
    String versionNewComment = "new version comment";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);
    ModelVersionChange changeComment = ModelVersionChange.updateComment(versionNewComment);
    ModelVersion modelVersion = modelOperationDispatcher.getModelVersion(modelIdent, "alias1");
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, "alias1", changeComment);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.aliases(), alteredModelVersion.aliases());
    Assertions.assertEquals(versionNewComment, alteredModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), alteredModelVersion.properties());
  }

  @Test
  void testUpdateAndAddModelVersionProperty() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Map<String, String> newProps = ImmutableMap.of("k1", "new value", "k2", "v2", "k3", "v3");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange[] changes =
        new ModelVersionChange[] {
          ModelVersionChange.setProperty("k1", "new value"),
          ModelVersionChange.setProperty("k3", "v3")
        };
    ModelVersion modelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, 0, changes);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertEquals(modelVersion.aliases(), alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(newProps, alteredModelVersion.properties());
  }

  @Test
  void testUpdateAndAddModelVersionPropertyByAlias() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Map<String, String> newProps = ImmutableMap.of("k1", "new value", "k2", "v2", "k3", "v3");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange[] changes =
        new ModelVersionChange[] {
          ModelVersionChange.setProperty("k1", "new value"),
          ModelVersionChange.setProperty("k3", "v3")
        };
    ModelVersion modelVersion =
        modelOperationDispatcher.getModelVersion(modelIdent, versionAliases[0]);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, versionAliases[0], changes);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertEquals(modelVersion.aliases(), alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(newProps, alteredModelVersion.properties());
  }

  @Test
  void testRemoveModelVersionProperty() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Map<String, String> newProps = ImmutableMap.of("k2", "v2");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change = ModelVersionChange.removeProperty("k1");

    ModelVersion modelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, 0, change);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertEquals(modelVersion.aliases(), alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(newProps, alteredModelVersion.properties());
  }

  @Test
  void testRemoveModelVersionPropertyByAlias() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    Map<String, String> newProps = ImmutableMap.of("k2", "v2");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change = ModelVersionChange.removeProperty("k1");

    ModelVersion modelVersion =
        modelOperationDispatcher.getModelVersion(modelIdent, versionAliases[0]);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, versionAliases[0], change);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertEquals(modelVersion.aliases(), alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(newProps, alteredModelVersion.properties());
  }

  @Test
  void testUpdateModelVersionUri() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    String newUri = "s3://new-bucket/new-path/new-model.json";

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change = ModelVersionChange.updateUri(newUri);
    ModelVersion modelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, 0, change);

    Assertions.assertEquals(newUri, alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertEquals(modelVersion.aliases(), alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), alteredModelVersion.properties());
  }

  @Test
  void testUpdateModelVersionUriByAlias() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");
    String newUri = "s3://new-bucket/new-path/new-model.json";

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change = ModelVersionChange.updateUri(newUri);
    ModelVersion modelVersion =
        modelOperationDispatcher.getModelVersion(modelIdent, versionAliases[0]);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, versionAliases[0], change);

    Assertions.assertEquals(newUri, alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertEquals(modelVersion.aliases(), alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), alteredModelVersion.properties());
  }

  @Test
  void testUpdateModelVersionAliases() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change =
        ModelVersionChange.updateAliases(new String[] {"new_alias1", "new_alias2"}, versionAliases);
    ModelVersion modelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, 0, change);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"new_alias1", "new_alias2"}, alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), alteredModelVersion.properties());

    // Reload model version
    ModelVersion reloadedModelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"new_alias1", "new_alias2"}, reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  @Test
  void testUpdateModelVersionAliasesByAlias() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change =
        ModelVersionChange.updateAliases(new String[] {"new_alias1", "new_alias2"}, versionAliases);
    ModelVersion modelVersion =
        modelOperationDispatcher.getModelVersion(modelIdent, versionAliases[0]);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, versionAliases[0], change);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"new_alias1", "new_alias2"}, alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), alteredModelVersion.properties());

    // Reload model version
    ModelVersion reloadedModelVersion =
        modelOperationDispatcher.getModelVersion(modelIdent, "new_alias1");
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"new_alias1", "new_alias2"}, reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  @Test
  void testUpdatePartialModelVersionAliases() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change =
        ModelVersionChange.updateAliases(
            new String[] {"new_alias1", "new_alias2"}, new String[] {"alias1"});
    ModelVersion modelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, 0, change);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"alias2", "new_alias1", "new_alias2"}, alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), alteredModelVersion.properties());

    // Reload model version
    ModelVersion reloadedModelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"alias2", "new_alias1", "new_alias2"}, reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  @Test
  void testUpdatePartialModelVersionAliasesByAlias() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias1", "alias2"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change =
        ModelVersionChange.updateAliases(
            new String[] {"new_alias1", "new_alias2"}, new String[] {"alias1"});
    ModelVersion modelVersion =
        modelOperationDispatcher.getModelVersion(modelIdent, versionAliases[0]);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, versionAliases[0], change);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"alias2", "new_alias1", "new_alias2"}, alteredModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), alteredModelVersion.properties());

    // Reload model version
    ModelVersion reloadedModelVersion =
        modelOperationDispatcher.getModelVersion(modelIdent, "new_alias1");
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertArrayEquals(
        new String[] {"alias2", "new_alias1", "new_alias2"}, reloadedModelVersion.aliases());
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  @Test
  void testUpdateModelVersionAliasesOverlapAddAndRemove() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias2", "alias3"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change =
        ModelVersionChange.updateAliases(
            new String[] {"alias1", "alias2"}, new String[] {"alias2", "alias3"});
    ModelVersion modelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, 0, change);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertEquals(
        ImmutableSet.of("alias1", "alias2"),
        Arrays.stream(alteredModelVersion.aliases()).collect(Collectors.toSet()));
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), alteredModelVersion.properties());

    // Reload model version
    ModelVersion reloadedModelVersion = modelOperationDispatcher.getModelVersion(modelIdent, 0);
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(
        ImmutableSet.of("alias1", "alias2"),
        Arrays.stream(reloadedModelVersion.aliases()).collect(Collectors.toSet()));
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  @Test
  void testUpdateModelVersionAliasesByAliasOverlapAddAndRemove() {
    String schemaName = randomSchemaName();
    String schemaComment = "schema which tests update";

    String modelName = randomModelName();
    String modelComment = "model which tests update";
    Map<String, String> props = ImmutableMap.of("k1", "v1", "k2", "v2");

    String versionUri = "s3://test-bucket/test-path/model.json";
    String[] versionAliases = {"alias2", "alias3"};
    String versionComment = "version which tests update";

    NameIdentifier schemaIdent = NameIdentifier.of(metalake, catalog, schemaName);
    schemaOperationDispatcher.createSchema(schemaIdent, schemaComment, props);

    NameIdentifier modelIdent =
        NameIdentifierUtil.ofModel(metalake, catalog, schemaName, modelName);
    modelOperationDispatcher.registerModel(modelIdent, modelComment, props);

    modelOperationDispatcher.linkModelVersion(
        modelIdent, versionUri, versionAliases, versionComment, props);

    ModelVersionChange change =
        ModelVersionChange.updateAliases(
            new String[] {"alias1", "alias2"}, new String[] {"alias2", "alias3"});
    ModelVersion modelVersion = modelOperationDispatcher.getModelVersion(modelIdent, "alias2");
    ModelVersion alteredModelVersion =
        modelOperationDispatcher.alterModelVersion(modelIdent, "alias2", change);

    Assertions.assertEquals(modelVersion.uri(), alteredModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), alteredModelVersion.version());
    Assertions.assertEquals(
        ImmutableSet.of("alias1", "alias2"),
        Arrays.stream(alteredModelVersion.aliases()).collect(Collectors.toSet()));
    Assertions.assertEquals(modelVersion.comment(), alteredModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), alteredModelVersion.properties());

    // Reload model version
    ModelVersion reloadedModelVersion =
        modelOperationDispatcher.getModelVersion(modelIdent, "alias1");
    Assertions.assertEquals(modelVersion.uri(), reloadedModelVersion.uri());
    Assertions.assertEquals(modelVersion.version(), reloadedModelVersion.version());
    Assertions.assertEquals(
        ImmutableSet.of("alias1", "alias2"),
        Arrays.stream(reloadedModelVersion.aliases()).collect(Collectors.toSet()));
    Assertions.assertEquals(modelVersion.comment(), reloadedModelVersion.comment());
    Assertions.assertEquals(modelVersion.properties(), reloadedModelVersion.properties());
  }

  private String randomSchemaName() {
    return "schema_" + UUID.randomUUID().toString().replace("-", "");
  }

  private String randomModelName() {
    return "model_" + UUID.randomUUID().toString().replace("-", "");
  }
}
