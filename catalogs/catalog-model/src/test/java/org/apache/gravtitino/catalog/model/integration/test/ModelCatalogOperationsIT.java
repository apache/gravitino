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
import org.apache.gravitino.model.ModelVersion;
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
