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
import org.apache.gravitino.model.ModelVersion;
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
        "Properties are reserved and cannot be set",
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
        "Properties are reserved and cannot be set",
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

  private String randomSchemaName() {
    return "schema_" + UUID.randomUUID().toString().replace("-", "");
  }

  private String randomModelName() {
    return "model_" + UUID.randomUUID().toString().replace("-", "");
  }
}
